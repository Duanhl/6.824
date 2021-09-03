package kvpaxos

import (
	"6.824/labrpc"
	"log"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu      sync.Mutex
	healthy []int
	prevEnd int
}

func (ck *Clerk) availableServer() (int, *labrpc.ClientEnd) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	if ck.prevEnd != -1 && ck.healthy[ck.prevEnd] < 3 {
		return ck.prevEnd, ck.servers[ck.prevEnd]
	} else {
		h := []int{}
		for i := 0; i < len(ck.healthy); i++ {
			if ck.healthy[i] < 3 {
				h = append(h, i)
			}
		}
		if len(h) == 0 {
			log.Fatalf("no avaiable server")
		}
		j := int(nrand()) % len(h)
		return j, ck.servers[j]
	}
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
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	args := &GetArgs{
		Key: key,
		Id:  nrand(),
	}
	reply := &GetReply{}
loop:
	i, s := ck.availableServer()
	if ok := s.Call("KVPaxos.Get", args, reply); ok {
		if reply.Err == OK {
			ck.mu.Lock()
			ck.prevEnd = i
			ck.healthy[i] = 0
			ck.mu.Unlock()

			return reply.Value
		}
	}

	ck.mu.Lock()
	ck.prevEnd = -1
	ck.healthy[i] += 1
	ck.mu.Unlock()

	time.Sleep(20 * time.Millisecond)
	goto loop
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
		Id:    nrand(),
	}
	reply := &PutAppendReply{}
loop:
	i, s := ck.availableServer()
	if ok := s.Call("KVPaxos.Get", args, reply); ok {
		if reply.Err == OK {
			ck.mu.Lock()
			ck.prevEnd = i
			ck.healthy[i] = 0
			ck.mu.Unlock()

			return
		}
	}

	ck.mu.Lock()
	ck.prevEnd = -1
	ck.healthy[i] += 1
	ck.mu.Unlock()

	time.Sleep(20 * time.Millisecond)
	goto loop
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
