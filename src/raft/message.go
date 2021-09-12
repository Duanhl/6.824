package raft

import (
	"encoding/json"
	"strconv"
)

type MessageType int32

const (
	VoteRequest MessageType = iota + 1
	VoteResponse

	AppendRequest
	AppendResponse

	InstallSnapshotRequest
	InstallSnapshotResponse

	DoSnapshot
	CondInstallSnapshot

	AppendCommand
	GetState
)

var MessageTypeMap = map[MessageType]string{
	VoteRequest:             "VoteRequest",
	VoteResponse:            "VoteResponse",
	AppendRequest:           "AppendRequest",
	AppendResponse:          "AppendResponse",
	InstallSnapshotRequest:  "InstallSnapshotRequest",
	InstallSnapshotResponse: "InstallSnapshotResponse",
	DoSnapshot:              "DoSnapshot",
	CondInstallSnapshot:     "CondInstallSnapshot",
	AppendCommand:           "AppendCommand",
	GetState:                "GetState",
}

func (m MessageType) String() string {
	return MessageTypeMap[m]
}

type Message struct {
	Type MessageType "message type"
	Id   uint64      "message id"
	From int         "what peer the message from"
	To   int         "what peer the message will sent"

	Term    int         "message term"
	Content interface{} "message body"
}

func (m Message) String() string {
	js := "\n{\n  type:" + m.Type.String() + ",\n"
	js += "  from:" + strconv.Itoa(m.From) + ",\n"
	js += "  to:" + strconv.Itoa(m.To) + ",\n"
	if b, err := json.Marshal(m.Content); err == nil {
		js += "  content:" + string(b) + "\n}"
	} else {
		js += "  content: nil \n}"
	}
	return js
}

// for getState
type StateInfo struct {
	Term     int
	IsLeader bool
}

// for start req
type StartInfo struct {
	Index    int
	Term     int
	IsLeader bool
}

// for do snapshot args
type SnapshotArgs struct {
	LastIncludedTerm  int
	LastIncludedIndex int
	Snapshot          []byte
}

// log entry
type Entry struct {
	Command interface{}
	Term    int
	Index   int
}
