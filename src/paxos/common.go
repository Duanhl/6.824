package paxos

import (
	"fmt"
	"log"
	"strconv"
	"sync/atomic"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type PrepareArgs struct {
	Seq int
	Pid Pid
}

type PrepareReply struct {
	Agree       bool
	AcceptedVal interface{}
	AcceptedPid Pid
}

type AcceptArgs struct {
	Seq int
	Pid Pid
	Val interface{}
}

type AcceptReply struct {
	Agree bool
}

type DoneBroadcastArgs struct {
	From int
	Seq  int
}

type DoneBroadcastReply struct {
}

type MsgType int

func (t MsgType) String() string {
	return MsgTypeName[t]
}

const (
	PrepareReq MsgType = iota + 1
	PrepareRes

	AcceptReq
	AcceptRes

	StartProposer
	Status
	DoneSnap
	MaxQuery
	MinQuery

	DoneBroadcast
)

var MsgTypeName = map[MsgType]string{
	PrepareReq:    "PrepareReq",
	PrepareRes:    "PrepareRes",
	AcceptReq:     "AcceptReq",
	AcceptRes:     "AcceptRes",
	StartProposer: "StartProposer",
	Status:        "Status",
	DoneSnap:      "DoneSnap",
	MaxQuery:      "MaxQuery",
	MinQuery:      "MinQuery",
	DoneBroadcast: "DoneBroadcast",
}

type Message struct {
	Type MsgType
	Id   uint64
	From int
	To   int

	Seq  int
	Pid  Pid
	Fate int

	Agree       bool
	AcceptedPid Pid
	Val         interface{}
}

func (m *Message) String() string {
	return "{Type: " + m.Type.String() + ", " +
		"Id: " + strconv.Itoa(int(m.Id)) + ", " +
		"From: " + strconv.Itoa(m.From) + ", " +
		"To: " + strconv.Itoa(m.To) + ", " +
		"Seq: " + strconv.Itoa(m.Seq) + ", " +
		"Pid: " + m.Pid.String() + ", " +
		"Fate: " + strconv.Itoa(m.Fate) + ", " +
		"Agree: " + strconv.FormatBool(m.Agree) + ", " +
		"AcceptedPid: " + m.AcceptedPid.String() + ", " +
		"Val: " + fmt.Sprintf("%v", m.Val) + "}"

}

type Context struct {
	resc chan Message
}

func (ctx *Context) Done() Message {
	return <-ctx.resc
}

// ProposerId
type Pid struct {
	Pre int
	Suf uint64
}

func (p *Pid) String() string {
	return strconv.Itoa(p.Pre) + "_" + strconv.Itoa(int(p.Suf))
}

func (s *Pid) Before(other Pid) bool {
	if s.Suf < other.Suf {
		return true
	}
	if s.Suf == other.Suf {
		return s.Pre < other.Pre
	}
	return false
}

func (s *Pid) After(other Pid) bool {
	if s.Suf > other.Suf {
		return true
	}
	if s.Suf == other.Suf {
		return s.Pre > other.Pre
	}
	return false
}

func (s *Pid) Equals(other Pid) bool {
	return s.Suf == other.Suf && s.Pre == other.Pre
}

// ProposerId Generator
type Sequence struct {
	Server int
	base   uint64
}

func (s *Sequence) Next() Pid {
	return Pid{
		Pre: s.Server,
		Suf: atomic.AddUint64(&s.base, 1),
	}
}

func (s *Sequence) Zero() Pid {
	return Pid{
		Pre: s.Server,
		Suf: 0,
	}
}

// after call rebase(), call next() will always generate a big value than base
func (s *Sequence) Rebase(base Pid) {
	origin := atomic.LoadUint64(&s.base)
	for origin < base.Suf && atomic.CompareAndSwapUint64(&s.base, origin, base.Suf) {
		origin = atomic.LoadUint64(&s.base)
	}
}

// Serialize ID generator
type Serializer struct {
	base uint64
}

func (s *Serializer) NextId() uint64 {
	return atomic.AddUint64(&s.base, 1)
}
