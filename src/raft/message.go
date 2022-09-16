package raft

import (
	"encoding/json"
	"log"
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
	MType  MessageType
	Id     uint64
	Server int
	Term   int

	Success      bool
	LogIndex     int
	LogTerm      int
	LeaderCommit int
	Entries      []Entry
	Snapshot     []byte
	Val          interface{}

	replyC chan Message
}

func (m *Message) String() string {
	str := MessageTypeMap[m.MType] + "["
	b, err := json.Marshal(m)
	if err != nil {
		log.Fatalln(err)
	}
	str += string(b)
	str += "]"
	return str
}

// log entry
type Entry struct {
	Command interface{}
	Term    int
	Index   int
}
