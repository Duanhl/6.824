package raft

type MessageType int32

const (
	MsgVoteRequest    MessageType = 1
	MsgVoteResponse   MessageType = 2
	MsgAppendRequest  MessageType = 3
	MsgAppendResponse MessageType = 4

	MsgAppendCommand MessageType = 5
)

type Message struct {
	MType        MessageType
	Id           int64
	From         int
	To           int
	Term         int
	PrevLogIdx   int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
	Command      interface{}

	Agreed bool
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}
