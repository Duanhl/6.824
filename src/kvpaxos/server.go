package kvpaxos

import (
	"6.824/labrpc"
	"container/heap"
	"net"
)
import "log"
import "6.824/paxos"
import "sync"
import "sync/atomic"
import "encoding/gob"

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
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	store       map[string]string
	applyc      chan paxos.InsStatus
	prevApplied int
	reqsbuf     map[int64]bool
	applybuf    heap.Interface
}

type StatusHeap []paxos.InsStatus

func (sh StatusHeap) Len() int {
	return len(sh)
}

func (sh StatusHeap) Swap(i, j int) {
	sh[i], sh[j] = sh[j], sh[i]
}

func (sh StatusHeap) Less(i, j int) bool {
	return sh[i].Seq < sh[j].Seq
}

func (sh *StatusHeap) Push(x interface{}) {
	*sh = append(*sh, x.(paxos.InsStatus))
}

func (sh *StatusHeap) Pop() interface{} {
	x := (*sh)[0]
	*sh = (*sh)[1:]
	return x
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	return
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	return
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.

	kv.px = paxos.Make(servers, me)

	return kv
}
