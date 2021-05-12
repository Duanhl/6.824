package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"errors"
	"log"
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const enableDebugger = false

func Dprintf(fmt string, v ...interface{}) {
	if enableDebugger {
		log.Printf(fmt, v...)
	}
}

var MasterStopError = errors.New("master stopped")

type KVList struct {
	Key    string
	Values []string
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type TaskType uint8

func (tt TaskType) String() string {
	if tt == Mapper {
		return "map"
	} else if tt == Reducer {
		return "reduce"
	} else {
		return ""
	}
}

const (
	Mapper TaskType = iota
	Reducer
	None
)

type RequireTaskArgs struct {
}

type RequireTaskReply struct {
	Type    TaskType
	Index   int
	Files   []string
	NReduce int
}

type CommitTaskArgs struct {
	Type  TaskType
	Index int
	Files []string
}

type CommitTaskReply struct {
	Ok bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
