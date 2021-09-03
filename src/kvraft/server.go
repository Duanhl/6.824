package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type OpType int8

const (
	GET    OpType = 0
	PUT    OpType = 1
	APPEND OpType = 3
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  OpType
	Id    int64
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
	store             map[string]string
	lastIncludedIndex int
	lastIncludedTerm  int

	ticker     chan struct{}
	opCh       chan Op
	resMap     map[int64]chan Reply
	indexIdMap map[int]int64
}

func (kv *KVServer) waitForRes(op Op) Reply {

	kv.mu.Lock()
	ch := make(chan Reply)
	kv.resMap[op.Id] = ch
	kv.opCh <- op
	kv.mu.Unlock()

	select {
	case reply := <-ch:
		return reply
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	res := kv.waitForRes(Op{
		Type: GET,
		Id:   args.Id,
		Key:  args.Key,
	})
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var ot OpType
	if args.Op == "Put" {
		ot = PUT
	} else {
		ot = APPEND
	}
	res := kv.waitForRes(Op{
		Type:  ot,
		Id:    args.Id,
		Key:   args.Key,
		Value: args.Value,
	})
	reply.Err = res.Err
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
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.rf.Kill()
	for _, c := range kv.resMap {
		c <- Reply{
			Err: ErrNoKey,
		}
	}

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
	kv.ticker = make(chan struct{})
	kv.opCh = make(chan Op)

	kv.store = make(map[string]string)

	kv.resMap = make(map[int64]chan Reply)
	kv.indexIdMap = make(map[int]int64)

	// You may need initialization code here.
	go kv.run()

	return kv
}

func (kv *KVServer) run() {
	if !kv.killed() {
		for {
			select {
			case applyMsg := <-kv.applyCh:
				kv.apply(applyMsg)
			case op := <-kv.opCh:
				kv.process(op)
			}
		}
	}
	DPrintf("server end up")
}

func (kv *KVServer) apply(applyMsg raft.ApplyMsg) {
	if applyMsg.CommandValid {
		index := applyMsg.CommandIndex
		op := applyMsg.Command.(Op)

		var ch chan Reply
		if id, ok := kv.indexIdMap[index]; ok {

			kv.mu.Lock()
			ch = kv.resMap[id]
			delete(kv.resMap, id)
			kv.mu.Unlock()

			if op.Id != id {
				ch <- Reply{Err: ErrWrongLeader}
				kv.doApply(op, nil)
			} else {
				kv.doApply(op, &ch)
			}
		} else {
			kv.doApply(op, nil)
		}

		if kv.checkStateSize() {
			b := new(bytes.Buffer)
			e := labgob.NewEncoder(b)
			if err := e.Encode(kv.store); err != nil {
				panic("encode store failed")
			}
			kv.lastIncludedIndex = applyMsg.CommandIndex
			kv.rf.Snapshot(applyMsg.CommandIndex, b.Bytes())
		}
	}

	if applyMsg.SnapshotValid {
		b := bytes.NewBuffer(applyMsg.Snapshot)
		d := labgob.NewDecoder(b)

		var store map[string]string

		if err := d.Decode(&store); err != nil {
			panic("decode store failed")
		}

		kv.store = store
		kv.lastIncludedIndex = applyMsg.SnapshotIndex
		kv.lastIncludedTerm = applyMsg.SnapshotTerm
	}
}

func (kv *KVServer) doApply(op Op, ch *chan Reply) {
	switch op.Type {
	case GET:
		if ch != nil {
			if v, ok := kv.store[op.Key]; ok {
				*ch <- Reply{
					Err:   OK,
					Value: v,
				}
			} else {
				*ch <- Reply{
					Err: ErrNoKey,
				}
			}
		}
		break
	case PUT:
		kv.store[op.Key] = op.Value
		if ch != nil {
			*ch <- Reply{
				Err: OK,
			}
		}
	case APPEND:
		if v, ok := kv.store[op.Key]; ok {
			kv.store[op.Key] = v + op.Value
		} else {
			kv.store[op.Key] = op.Value
		}
		if ch != nil {
			*ch <- Reply{
				Err: OK,
			}
		}
	}
}

func (kv *KVServer) process(op Op) {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {

		kv.mu.Lock()
		ch := kv.resMap[op.Id]
		kv.mu.Unlock()

		ch <- Reply{
			Err: ErrWrongLeader,
		}
	} else {
		kv.indexIdMap[index] = op.Id
	}

}

func (kv *KVServer) checkStateSize() bool {
	return false
	//return kv.rf.RaftStateSize() > kv.maxraftstate
}
