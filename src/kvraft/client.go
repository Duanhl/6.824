package raftkv

import (
	"labrpc"
	"time"
)
import "sync"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	leader  int
	lock    *sync.Mutex
	reqBuf  chan Args
	resBuf  chan Reply
	resChan map[int64]chan Reply
	ticker  *time.Ticker
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

	ck.leader = 0
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
	return ""
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
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "AppendEntries")
}

func (ck *Clerk) mainLoop() {
	for {
		select {
		case <-ck.ticker.C:
			for args := range ck.reqBuf {
				ck.sendRequest(args)
			}
			break
		case res := <-ck.resBuf:
			if resCh, ok := ck.resChan[res.Id]; ok {
				resCh <- res
				delete(ck.resChan, res.Id)
			}
			DPrintf("Error found no resChan, res: %v", res)
		}
	}
}

func (ck *Clerk) sendRequest(args Args) {
	switch args.Op {
	case Get:
		getArgs := &GetArgs{Key: args.Key}
		getReply := &GetReply{}
		if ok := ck.servers[ck.leader].Call("KVServer.Get", &getArgs, &getReply); ok {

		} else {
			time.AfterFunc(time.Duration(50), func() {
				ck.sendRequest(args)
			})
		}
	}
}
