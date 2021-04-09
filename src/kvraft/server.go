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

const Debug = false

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

	ticker chan struct{}
	msgCh  chan Op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
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
	kv.msgCh = make(chan Op)

	// You may need initialization code here.
	go kv.run()

	return kv
}

func (kv *KVServer) run() {
	for {
		select {
		case applyMsg := <-kv.applyCh:
			kv.apply(applyMsg)
		case op := <-kv.msgCh:
			kv.process(op)
		}
	}
}

func (kv *KVServer) apply(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if applyMsg.CommandValid {
		op := applyMsg.Command.(*Op)
		switch op.Type {
		case GET:
			break
		case PUT:
			kv.store[op.Key] = op.Value
		case APPEND:
			if v, ok := kv.store[op.Key]; ok {
				kv.store[op.Key] = v + op.Value
			} else {
				kv.store[op.Key] = op.Value
			}
		}
		if kv.checkStateSize(applyMsg.CommandIndex) {

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

func (kv *KVServer) process(op Op) {

}

func (kv *KVServer) checkStateSize(applyIndex int) bool {
	return applyIndex-kv.lastIncludedIndex >= kv.maxraftstate
}
