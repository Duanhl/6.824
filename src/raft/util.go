package raft

import (
	"log"
)

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func Min(left int, right int) int {
	if left > right {
		return right
	} else {
		return left
	}
}

func LastEntry(entries []LogEntry) *LogEntry {
	if entries == nil || len(entries) == 0 {
		return nil
	} else {
		return &entries[len(entries)-1]
	}
}
