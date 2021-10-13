package kvraft

import (
	"6.824/labrpc"
	"sync"
	"sync/atomic"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu     sync.Mutex
	id     int32
	seq    int32
	leader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func sleepRandom(base int64) {
	duration := time.Duration(base) + time.Duration(nrand()%base)
	time.Sleep(duration * time.Millisecond)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = int32(nrand())
	ck.seq = 0
	// You'll have to add code here.
	ck.leader = int(nrand()) % len(servers)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
loop:
	args := &GetArgs{
		Key:    key,
		Client: ck.id,
		Id:     atomic.AddInt32(&ck.seq, 1),
	}
	reply := &GetReply{}
	servers := ck.loop()
	for _, i := range servers {
		if ok := ck.servers[i].Call("KVServer.Get", args, reply); ok {
			if reply.Err == OK {
				ck.setLeader(i)
				return reply.Value
			} else if reply.Err == ErrNoKey {
				ck.setLeader(i)
				return ""
			}
		}
	}

	sleepRandom(50)

	goto loop
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Op:     op,
		Key:    key,
		Value:  value,
		Client: ck.id,
		Id:     atomic.AddInt32(&ck.seq, 1),
	}
loop:
	reply := &PutAppendReply{}
	servers := ck.loop()
	for _, i := range servers {
		if ok := ck.servers[i].Call("KVServer.PutAppend", args, reply); ok {
			if reply.Err == OK {
				ck.setLeader(i)
				return
			}
		}
	}

	sleepRandom(50)

	goto loop
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) loop() []int {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	var res []int
	for i := ck.leader; i < ck.leader+len(ck.servers); i++ {
		res = append(res, i%len(ck.servers))
	}
	return res
}

func (ck *Clerk) setLeader(server int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.leader = server
}
