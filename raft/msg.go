package raft

type MsgType int32

const (
	App MsgType = iota
	AppResp
	Vote
	VoteResp
)

var MsgTypeNames = map[int32]string{
	0: "App",
	1: "AppResp",
	2: "Vote",
	3: "VoteResp",
}

type Entry struct {
	Term  int    `json:"term"`
	Index int    `json:"index"`
	Data  []byte `json:"data"`
}

type Snapshot struct {
	Data  []byte `json:"data"`
	Index int    `json:"index"`
	Term  int    `json:"term"`
}

type Msg struct {
	Type     MsgType  `json:"type"`
	To       int      `json:"to"`
	From     int      `json:"from"`
	Term     int      `json:"term"`
	LogTerm  int      `json:"logTerm"`
	Index    int      `json:"index"`
	Entries  []Entry  `json:"entries"`
	Commit   int      `json:"commit"`
	Snapshot Snapshot `json:"snapshot"`
	Reject   bool     `json:"reject"`
}
