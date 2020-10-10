package raftkv

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

const (
	Get    = "Get"
	Put    = "Put"
	Append = "AppendEntries"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "AppendEntries"
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

type GetReply struct {
	Err   Err
	Value string
}

type Args struct {
	Key   string
	Value string
	Op    string // "Put" or "Get" or "AppendEntries"
	Id    int64
}

type Reply struct {
	Err   Err
	Value string
	Id    int64
}
