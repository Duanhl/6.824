package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key    string
	Value  string
	OpType string //"Get", "Put", "AppendEntries"
	Id     int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	store      map[string]string
	reqBuf     chan Op
	resChStore map[int64]chan Reply
	ticker     *time.Ticker
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Key:    args.Key,
		Value:  "",
		OpType: "Get",
		Id:     args.Id,
	}

	resCh := make(chan Reply)

	kv.command(op, resCh)

	for {
		select {
		case res := <-resCh:
			reply.Err = res.Err
			reply.Value = res.Value
			return
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		Key:    args.Key,
		Value:  args.Value,
		OpType: args.Op,
		Id:     args.Id,
	}

	resCh := make(chan Reply)

	kv.command(op, resCh)

	for {
		select {
		case res := <-resCh:
			reply.Err = res.Err
			return
		}
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

	// You may need initialization code here.
	kv.store = make(map[string]string)
	kv.reqBuf = make(chan Op)
	kv.resChStore = make(map[int64]chan Reply)

	go kv.run()

	return kv
}

func (kv *KVServer) run() {
	for {
		select {
		case applyMsg := <-kv.applyCh:
			kv.dealApply(applyMsg)
		case op := <-kv.reqBuf:
			_, _, isLeader := kv.rf.Start(op)
			if !isLeader {
				kv.replyToClient(Reply{
					Err: ErrWrongLeader,
				})
			}
		}
	}
}

func (kv *KVServer) dealApply(applyMsg raft.ApplyMsg) {

	switch applyMsg.Command.(type) {
	case Op:
		op := applyMsg.Command.(Op)
		switch op.OpType {
		case "Get":
			if v, ok := kv.store[op.Key]; ok {
				kv.replyToClient(Reply{
					Value: v,
				})
			} else {
				kv.replyToClient(Reply{
					Err: ErrNoKey,
				})
			}
			break
		case "Put":
			kv.store[op.Key] = op.Value
			kv.replyToClient(Reply{})
			break
		case "AppendEntries":
			if v, ok := kv.store[op.Key]; ok {
				kv.store[op.Key] += v
			} else {
				kv.store[op.Key] = op.Value
			}
			kv.replyToClient(Reply{})
			break
		}
		break
	default:
		DPrintf("wrong apply type,: %v", applyMsg)
	}
}

func (kv *KVServer) command(op Op, ch chan Reply) {
	kv.reqBuf <- op
	kv.resChStore[op.Id] = ch
}

func (kv *KVServer) replyToClient(reply Reply) {
	if v, ok := kv.resChStore[reply.Id]; ok {
		v <- reply
		delete(kv.resChStore, reply.Id)
	}
}
