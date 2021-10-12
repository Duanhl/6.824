package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const TickDuration = 10 * time.Millisecond

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType int8

const (
	GET OpType = iota + 1
	PUT
	APPEND
	READINDEX
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  OpType
	Id    int32
	Key   string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store   map[string]string `store:"kv store"`
	applied map[int32]int     `applied:"already applied request"`

	closeCh       chan interface{} `closeCh:"notify close channel"`
	activeTimeout int32
	activeElapsed int32

	lastApplied int

	rpcm map[int]Ctx
}

type Ctx struct {
	id int32
	ch chan Reply
}

func (kv *KVServer) process(op Op) chan Reply {
	c := make(chan Reply)
	if kv.killed() {
		close(c)
		return c
	}

	if index, _, isLeader := kv.rf.Start(op); isLeader {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		// already start at (index)
		if ctx, ok := kv.rpcm[index]; ok {
			close(ctx.ch)
		}

		//DPrintf("KvSrv %v start op[%v] in index(%v)", kv.me, op.Id, index)
		kv.rpcm[index] = Ctx{
			id: op.Id,
			ch: c,
		}
		return c
	} else {
		kv.clear()
		close(c)
		return c
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	ch := kv.process(Op{
		Type: GET,
		Id:   args.Id,
		Key:  args.Key,
	})
	if r, ok := <-ch; ok {
		reply.Err = r.Err
		reply.Value = r.Value
	} else {
		reply.Err = ErrServerDied
		reply.Value = ""
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var ot OpType
	if args.Op == "Put" {
		ot = PUT
	} else {
		ot = APPEND
	}
	ch := kv.process(Op{
		Type:  ot,
		Id:    args.Id,
		Key:   args.Key,
		Value: args.Value,
	})
	if r, ok := <-ch; ok {
		reply.Err = r.Err
	} else {
		reply.Err = ErrServerDied
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.closeCh)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.closeCh = make(chan interface{})

	kv.store = make(map[string]string)
	kv.applied = map[int32]int{}

	kv.activeTimeout = 20

	kv.rpcm = map[int]Ctx{}

	// You may need initialization code here.
	sn := persister.ReadSnapshot()
	if len(sn) > 0 {
		kv.mu.Lock()
		lastApplied, store, applied := kv.read(sn)
		kv.lastApplied = lastApplied
		kv.store = store
		kv.applied = applied
		kv.mu.Unlock()
	}

	go kv.run()
	return kv
}

func (kv *KVServer) doReply(seq int, id int32, reply Reply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if ctx, ok := kv.rpcm[seq]; ok {
		DPrintf("KvSrv %v replied op[%v]", kv.me, ctx.id)
		if ctx.id == id {
			ctx.ch <- reply
		} else {
			ctx.ch <- Reply{Err: ErrWrongLeader}
		}
		delete(kv.rpcm, seq)
	}
}

func (kv *KVServer) clear() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for _, ctx := range kv.rpcm {
		DPrintf("KvSrv %v clear op[%v] because leadership change", kv.me, ctx.id)
		ctx.ch <- Reply{Err: ErrWrongLeader}
	}
	kv.rpcm = map[int]Ctx{}
}

func (kv *KVServer) run() {
	DPrintf("KvSrv %v start", kv.me)
	ticker := time.Tick(TickDuration)
	for {
		select {
		case <-ticker:
			atomic.AddInt32(&kv.activeElapsed, 1)
			if atomic.LoadInt32(&kv.activeElapsed) > kv.activeTimeout {
				go func() {
					atomic.StoreInt32(&kv.activeElapsed, 0)
					if _, _, isLeader := kv.rf.Start(Op{Type: READINDEX}); !isLeader {
						kv.clear()
					}
				}()
			}
		case applyMsg := <-kv.applyCh:
			atomic.StoreInt32(&kv.activeElapsed, 0)
			kv.doApply(applyMsg)
		case <-kv.closeCh:
			DPrintf("KvSrv %v closed", kv.me)
			return
		}
	}
}

func (kv *KVServer) doApply(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if applyMsg.CommandValid {
		if applyMsg.CommandIndex != kv.lastApplied+1 {
			DPrintf("ERROR, KvSrv %v applied index %v -> %v", kv.me, kv.lastApplied, applyMsg.CommandIndex)
			return
		}
		op := applyMsg.Command.(Op)
		value, err := kv.changeStore(applyMsg.CommandIndex, op)
		go kv.doReply(applyMsg.CommandIndex, op.Id, Reply{
			Err:   err,
			Value: value,
		})
		kv.lastApplied = applyMsg.CommandIndex
		if kv.checkStateSize() {
			// do snapshot
			//go func() {
			//	kv.mu.Lock()
			data := kv.persists()
			//kv.mu.Unlock()

			DPrintf("KvSrv %v snapshot to index %v", kv.me, kv.lastApplied)
			kv.rf.Snapshot(kv.lastApplied, data)
			//}()
		}
		return
	}

	if applyMsg.SnapshotValid {
		if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
			DPrintf("KvSrv %v install snapshot, index %v -> %v", kv.me, kv.lastApplied, applyMsg.SnapshotIndex)
			lastApplied, store, applied := kv.read(applyMsg.Snapshot)
			kv.lastApplied = lastApplied
			kv.store = store
			kv.applied = applied

			for seq, ctx := range kv.rpcm {
				DPrintf("KvSrv %v install snapshot and clear op[%v]", kv.me, ctx.id)
				ctx.ch <- Reply{Err: ErrWrongLeader}
				delete(kv.rpcm, seq)
			}
		}
	}
}

func (kv *KVServer) changeStore(seq int, op Op) (string, Err) {
	switch op.Type {
	case GET:
		if v, ok := kv.store[op.Key]; ok {
			return v, OK
		} else {
			return "", ErrNoKey
		}
	case PUT:
		if _, alreadyApplied := kv.applied[op.Id]; alreadyApplied == false {
			kv.store[op.Key] = op.Value
			kv.applied[op.Id] = seq
		}
		return kv.store[op.Key], OK
	case APPEND:
		if _, alreadyApplied := kv.applied[op.Id]; alreadyApplied == false {
			oldV := kv.store[op.Key]
			kv.store[op.Key] = oldV + op.Value
			kv.applied[op.Id] = seq
		}
		return kv.store[op.Key], OK
	case READINDEX:
		return "", OK
	default:
		panic("Unsupported op type")
	}
}

func (kv *KVServer) checkStateSize() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	return kv.rf.RaftStateSize() > 8*kv.maxraftstate
}

func (kv *KVServer) persists() []byte {
	buf := new(bytes.Buffer)
	e := labgob.NewEncoder(buf)

	//applied := map[int64]int{}
	//for id, seq := range kv.applied {
	//	if seq >= kv.lastApplied - 10 {
	//		applied[id] = seq
	//	}
	//}
	e.Encode(&kv.lastApplied)
	e.Encode(&kv.store)
	e.Encode(&kv.applied)
	return buf.Bytes()
}

func (kv *KVServer) read(data []byte) (int, map[string]string, map[int32]int) {
	buf := bytes.NewBuffer(data)
	d := labgob.NewDecoder(buf)
	var lastApplied int
	var store map[string]string
	var applied map[int32]int

	if d.Decode(&lastApplied) != nil ||
		d.Decode(&store) != nil ||
		d.Decode(&applied) != nil {
		panic("Decode data error")
	}
	return lastApplied, store, applied
}
