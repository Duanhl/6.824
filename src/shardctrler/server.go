package shardctrler

import (
	"6.824/raft"
	"sort"
	"time"
)
import "6.824/labrpc"
import "sync"
import "6.824/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num
	closeCh chan interface{}

	rpcm map[int]chan Reply
}

type OpType int8

const (
	Join OpType = iota + 1
	Leave
	Move
	Query
)

type Reply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type Op struct {
	// Your data here.
	Type OpType
	// for join
	Servers map[int][]string

	// for leave
	GIDs []int

	// for move
	Shard int
	GID   int

	// for query
	Num int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	ch := sc.process(Op{
		Type:    Join,
		Servers: args.Servers,
	})
	if r, ok := <-ch; ok {
		reply.WrongLeader = r.WrongLeader
		reply.Err = r.Err
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	ch := sc.process(Op{
		Type: Leave,
		GIDs: args.GIDs,
	})
	if r, ok := <-ch; ok {
		reply.WrongLeader = r.WrongLeader
		reply.Err = r.Err
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	ch := sc.process(Op{
		Type:  Move,
		GID:   args.GID,
		Shard: args.Shard,
	})
	if r, ok := <-ch; ok {
		reply.WrongLeader = r.WrongLeader
		reply.Err = r.Err
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	ch := sc.process(Op{
		Type: Query,
		Num:  args.Num,
	})
	if r, ok := <-ch; ok {
		reply.WrongLeader = r.WrongLeader
		reply.Err = r.Err
		reply.Config = r.Config
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) process(op Op) chan Reply {
	ch := make(chan Reply)
	if index, _, isLeader := sc.rf.Start(op); isLeader {
		sc.mu.Lock()
		defer sc.mu.Unlock()

		if oldc, ok := sc.rpcm[index]; ok {
			oldc <- Reply{WrongLeader: true}
		}
		sc.rpcm[index] = ch
		return ch
	} else {
		go func() {
			ch <- Reply{WrongLeader: true}
		}()
		return ch
	}
}

func (sc *ShardCtrler) replied(index int, reply Reply) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if ch, ok := sc.rpcm[index]; ok {
		ch <- reply
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	close(sc.closeCh)
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) run() {
	for {
		select {
		case applyMsg := <-sc.applyCh:
			if applyMsg.CommandValid {
				op := applyMsg.Command.(Op)
				if op.Type == Query {
					reply := Reply{
						WrongLeader: false,
						Err:         OK,
					}
					if op.Num <= -1 {
						reply.Config = sc.configs[len(sc.configs)-1]
					} else if op.Num < len(sc.configs) {
						reply.Config = sc.configs[op.Num]
					} else {
						reply.Err = NumOutOfIndex
					}
					sc.replied(applyMsg.CommandIndex, reply)
				} else {
					prevConfig := sc.configs[len(sc.configs)-1]
					config := sc.next(prevConfig, op)
					sc.configs = append(sc.configs, config)
					sc.replied(applyMsg.CommandIndex, Reply{WrongLeader: false, Err: OK})
				}
			}
		case <-time.After(100 * time.Millisecond):

		case <-sc.closeCh:
			sc.mu.Lock()
			for _, ch := range sc.rpcm {
				ch <- Reply{WrongLeader: true}
			}
			sc.mu.Unlock()
			return
		}
	}
}

func (sc *ShardCtrler) next(prev Config, op Op) Config {
	var config Config
	var shards [NShards]int
	for i := 0; i < NShards; i++ {
		shards[i] = prev.Shards[i]
	}
	groups := map[int][]string{}
	for gid, servers := range prev.Groups {
		groups[gid] = servers
	}

	switch op.Type {
	case Join:
		for gid, servers := range op.Servers {
			groups[gid] = servers
		}
		gids := []int{}
		for gid, _ := range groups {
			gids = append(gids, gid)
		}

		shards = sc.doRebalanced(gids)
		config = Config{
			Num:    prev.Num + 1,
			Shards: shards,
			Groups: groups,
		}
		break

	case Leave:
		for gid, _ := range groups {
			if Find(op.GIDs, gid) {
				delete(groups, gid)
			}
		}
		gids := []int{}
		for gid, _ := range groups {
			gids = append(gids, gid)
		}

		shards = sc.doRebalanced(gids)
		config = Config{
			Num:    prev.Num + 1,
			Shards: shards,
			Groups: groups,
		}
		break

	case Move:
		shards[op.Shard] = op.GID
		config = Config{
			Num:    prev.Num + 1,
			Shards: shards,
			Groups: groups,
		}
		break

	default:
		config = Config{
			Num: prev.Num,
		}
	}

	return config
}

func (sc *ShardCtrler) doRebalanced(gids []int) [NShards]int {
	var shards [NShards]int
	if len(gids) == 0 {
		return shards
	}
	sort.Ints(gids)
	flag := 0
	for i := 0; i < NShards; i++ {
		shards[i] = gids[flag]
		flag += 1
		if flag >= len(gids) {
			flag = flag % len(gids)
		}
	}
	return shards
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.closeCh = make(chan interface{})
	sc.rpcm = map[int]chan Reply{}

	go sc.run()
	return sc
}
