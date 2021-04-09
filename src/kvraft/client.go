package kvraft

import (
	"6.824/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	leader int
	msgCh  chan ClientMsg
	resMap map[int64]chan ClientMsg
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
	args := &GetArgs{
		Key: key,
		Id:  nrand(),
	}
	reply := &GetReply{}
	for true {
		if ok := ck.servers[ck.leader].Call("Get", args, reply); ok {
			switch reply.Err {
			case OK:
				return reply.Value
			case ErrNoKey:
				return ""
			case ErrWrongLeader:
				ck.loopForNextLeader()
			}
		} else {
			time.Sleep(time.Millisecond * 20)
			ck.loopForNextLeader()
		}
	}

	return ""
}

func (ck *Clerk) loopForNextLeader() {
	if ck.leader == len(ck.servers)-1 {
		ck.leader = 0
	} else {
		ck.leader++
	}
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

	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Id:    nrand(),
	}
	reply := &PutAppendReply{}
	for true {
		if ok := ck.servers[ck.leader].Call("PutAppend", args, reply); ok {
			switch reply.Err {
			case OK:
				return
			case ErrNoKey:
				DPrintf("error return type")
				return
			case ErrWrongLeader:
				ck.loopForNextLeader()
			}
		} else {
			time.Sleep(time.Microsecond * 20)
			ck.loopForNextLeader()
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
