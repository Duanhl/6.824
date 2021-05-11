package raft

type MessageType int32

const (
	MsgVoteRequest MessageType = iota
	MsgVoteResponse
	MsgAppendRequest
	MsgAppendResponse

	MsgInstallSnapshotRequest
	MsgInstallSnapshotResponse
	MsgSnapshot
	MsgCondInstallSnapshot

	MsgAppendCommand
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
	Data         []byte

	Agreed bool

	Retry int
}

type MessageSorter []Message

func (ms MessageSorter) Len() int {
	return len(ms)
}

func (ms MessageSorter) Swap(i, j int) {
	ms[i], ms[j] = ms[j], ms[i]
}

func (ms MessageSorter) Less(i, j int) bool {
	if ms[i].PrevLogIdx < ms[j].PrevLogIdx {
		return false
	} else if ms[i].PrevLogIdx == ms[j].PrevLogIdx {
		return len(ms[i].Entries) < len(ms[j].Entries)
	} else {
		return true
	}
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int
}
