package paxos

import (
	"fmt"
	"log"
	"math"
	"strconv"
)

const Debug = false
const Warn = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func DWarn(format string, a ...interface{}) (n int, err error) {
	if Warn {
		log.Printf(format, a...)
	}
	return
}

type PrepareArgs struct {
	Seq int
	Pid Pid

	Done     int
	Proposer int
}

type PrepareReply struct {
	Agree       bool
	AcceptedVal interface{}
	AcceptedPid Pid

	Seq int
	Pid Pid
}

func (pr *PrepareReply) String() string {
	return "{" +
		"Agree:" + strconv.FormatBool(pr.Agree) + "," +
		"AcceptedVal:" + fmt.Sprintf("%v", pr.AcceptedVal) + "," +
		"AcceptedPid:" + pr.AcceptedPid.String() + "," +
		"Seq:" + strconv.Itoa(pr.Seq) + "," +
		"Pid:" + pr.Pid.String() + "," +
		"}"
}

type AcceptArgs struct {
	Seq int
	Pid Pid
	Val interface{}
}

type AcceptReply struct {
	Agree       bool
	AcceptedPid Pid

	Seq int
	Pid Pid
}

func (ar *AcceptReply) String() string {
	return "{" +
		"Agree:" + strconv.FormatBool(ar.Agree) + "," +
		"AcceptedPid:" + ar.AcceptedPid.String() + "," +
		"Seq:" + strconv.Itoa(ar.Seq) + "," +
		"Pid:" + ar.Pid.String() + "," +
		"}"
}

type LearnArgs struct {
	Seq int
	Val interface{}
}

type LearnReply struct {
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

	LearnReq
	LearnRes

	StartProposer
	Status
	DoneSnap
	MaxQuery
	MinQuery
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
}

type Message struct {
	Type MsgType
	Id   int64
	To   int

	IsForMySelf bool

	Content interface{}
}

func (m *Message) String() string {
	return "{Type: " + m.Type.String() + ", " +
		"Id: " + strconv.Itoa(int(m.Id)) + ", " +
		"To: " + strconv.Itoa(m.To) + ", " +
		"Content: " + fmt.Sprintf("%v", m.Content)
}

type Context struct {
	resc chan interface{}
}

func (ctx *Context) Done() interface{} {
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

func (s *Pid) EqualsZero() bool {
	return s.Suf == 0
}

func (s *Pid) Next() Pid {
	return Pid{
		Pre: s.Pre,
		Suf: s.Suf + 1,
	}
}

func ZeroPid() Pid {
	return Pid{
		Pre: None,
		Suf: 0,
	}
}

func Min(arr []int) int {
	min := math.MaxInt32
	for _, v := range arr {
		if v < min {
			min = v
		}
	}
	return min
}
