package kvraft

import (
	"6.824/labrpc"
	"math"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu      sync.Mutex
	healthy []int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.healthy = make([]int, len(ck.servers))
	for i := 0; i < len(ck.healthy); i++ {
		ck.healthy[i] = 1
	}
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
		Key: key,
		Id:  nrand(),
	}
	reply := &GetReply{}
	i := ck.availableServer()
	if ok := ck.servers[i].Call("KVServer.Get", args, reply); ok {
		if reply.Err == OK {
			return reply.Value
		} else if reply.Err == ErrNoKey {
			return ""
		}
	}
	ck.mu.Lock()
	ck.healthy[i]++
	ck.mu.Unlock()

	goto loop
}

func (ck *Clerk) availableServer() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	server := None
	m := math.MaxInt32

	for i := 0; i < len(ck.servers); i++ {
		if ck.healthy[i] < m {
			m = ck.healthy[i]
			server = i
		}
	}
	return server
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
		Op:    op,
		Key:   key,
		Value: value,
		Id:    nrand(),
	}
loop:
	reply := &PutAppendReply{}
	i := ck.availableServer()
	if ok := ck.servers[i].Call("KVServer.PutAppend", args, reply); ok {
		if reply.Err == OK {
			return
		}
	}

	ck.mu.Lock()
	ck.healthy[i]++
	ck.mu.Unlock()

	goto loop
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
