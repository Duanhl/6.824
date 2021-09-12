package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = true

const TickDuration = 50 * time.Millisecond

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
	store   map[string]string `store:"kv store"`
	opCh    chan Op           `opCh:"input request channel"`
	closeCh chan interface{}  `closeCh:"notify close channel"`
	ticker  chan interface{}  `ticker:"for periodicity check raft peer state or call readIndex"`

	applied   map[int64]bool
	requested map[int64]bool

	rpcm map[int64]chan Reply
}

func (kv *KVServer) register(op Op) chan Reply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	c := make(chan Reply)
	if kv.killed() {
		close(c)
		return c
	}

	// already applied, only for PUT/APPEND
	if _, ok := kv.applied[op.Id]; ok {
		go func() { c <- Reply{Err: OK} }()
	} else if _, ok := kv.requested[op.Id]; ok {
		oldC := kv.rpcm[op.Id]
		close(oldC)
		kv.rpcm[op.Id] = c
	} else {
		kv.rpcm[op.Id] = c
		kv.opCh <- op
	}
	return c
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	ch := kv.register(Op{
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
	ch := kv.register(Op{
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

	kv.opCh = make(chan Op)
	kv.closeCh = make(chan interface{})

	kv.ticker = make(chan interface{}, 32)
	go func() {
		ticker := time.Tick(TickDuration)
		for kv.killed() == false {
			select {
			case <-ticker:
				kv.ticker <- struct{}{}
			}
		}
		ticker = nil
	}()

	kv.store = make(map[string]string)
	kv.applied = map[int64]bool{}
	kv.requested = map[int64]bool{}

	kv.rpcm = map[int64]chan Reply{}

	// You may need initialization code here.
	go kv.run()

	return kv
}

func (kv *KVServer) run() {
	for {
		select {
		case applyMsg := <-kv.applyCh:
			kv.apply(applyMsg)
		case op := <-kv.opCh:
			kv.process(op)
		case <-kv.ticker:
			kv.tick()
		case <-kv.closeCh:
			return
		}
	}
}

func (kv *KVServer) apply(applyMsg raft.ApplyMsg) {
	op := applyMsg.Command.(Op)

	kv.mu.Lock()
	ch, needRespond := kv.rpcm[op.Id]
	delete(kv.rpcm, op.Id)
	kv.mu.Unlock()

	switch op.Type {
	case GET:
		if needRespond {
			if v, ok := kv.store[op.Key]; ok {
				ch <- Reply{
					Err:   OK,
					Value: v,
				}
			} else {
				ch <- Reply{
					Err: ErrNoKey,
				}
			}
		}
		break
	case PUT:
		if _, alreadyApplied := kv.applied[op.Id]; alreadyApplied == false {
			kv.store[op.Key] = op.Value
			kv.applied[op.Id] = true
		}
		if needRespond {
			ch <- Reply{
				Err: OK,
			}
		}
		break
	case APPEND:
		if _, alreadyApplied := kv.applied[op.Id]; alreadyApplied == false {
			oldV := kv.store[op.Key]
			kv.store[op.Key] = oldV + op.Value
			kv.applied[op.Id] = true
		}
		if needRespond {
			ch <- Reply{
				Err: OK,
			}
		}
		break
	}
}

func (kv *KVServer) tick() {
	_, isLeader := kv.rf.GetState()
	// raft state change, return all uncommited request
	if isLeader == false {
		kv.handleStateChange()
	}
}

func (kv *KVServer) process(op Op) {
	_, _, isLeader := kv.rf.Start(op)
	if isLeader == false {
		kv.handleStateChange()
	}
}

func (kv *KVServer) handleStateChange() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for id, _ := range kv.rpcm {
		ch := kv.rpcm[id]
		ch <- Reply{
			Err: ErrWrongLeader,
		}
		delete(kv.rpcm, id)
	}
}

func (kv *KVServer) checkStateSize() bool {
	return false
	//return kv.rf.RaftStateSize() > kv.maxraftstate
}
