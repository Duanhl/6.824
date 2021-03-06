package kvraft

import "sync"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id int64
}

type Reply struct {
	Err   Err
	Value string
}

type GetReply struct {
	Err   Err
	Value string
}

type ClientMsgType int8

const (
	Request  ClientMsgType = 0
	Response               = 1
)

type ClientMsg struct {
	Type ClientMsgType

	Key   string
	Value string
	Id    int64
	Op    string

	Err    Err
	Leader int
}

type LinkedSet struct {
	size   int
	store  map[int]int
	linked []int
	mu     *sync.Mutex
}

func NewLinkedSet(size int) *LinkedSet {
	return &LinkedSet{
		size:   size,
		store:  make(map[int]int),
		linked: []int{},
	}
}
