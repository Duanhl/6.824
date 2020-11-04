package raft

import (
	"log"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func min(a, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

func max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

type Wait struct {
	ttype string
	msg   interface{}
	resc  chan interface{}
}

func NewWait(ttype string, msg interface{}) Wait {
	return Wait{
		ttype: ttype,
		msg:   msg,
		resc:  make(chan interface{}),
	}
}

func (wait *Wait) wait() interface{} {
	return <-wait.resc
}

func (wait *Wait) set(res interface{}) {
	wait.resc <- res
}
