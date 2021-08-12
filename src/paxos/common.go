package paxos

import "sync/atomic"

type PrepareArgs struct {
	Seq int
	Pid Pid
}

type PrepareReply struct {
	Seq         int
	AcceptedVal interface{}
	AcceptedPid Pid
}

type AcceptArgs struct {
	Seq int
	Pid Pid
	Val interface{}
}

type AcceptReply struct {
	Pid Pid
}

type MsgType int

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
)

type Message struct {
	Type MsgType
	Id   uint64
	From int
	To   int

	Seq int
	Pid Pid

	Agree bool
	Val   interface{}
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
