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
	Type MessageType "message type"
	Id   uint64      "message id"
	From int         "what peer the message from"
	To   int         "what peer the message will sent"

	LastLogIndex int
	LastLogTerm  int
	LeaderCommit int
	Entries      []Entry
	Snapshot     []byte

	Val interface{}

	Term    int "message term"
	Success bool

	replyC chan Message
}

func (m *Message) String() string {
	str := MessageTypeMap[m.Type] + "["
	b, err := json.Marshal(m)
	if err != nil {
		log.Fatalln(err)
	}
	str += string(b)
	str += "]"
	return str
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
