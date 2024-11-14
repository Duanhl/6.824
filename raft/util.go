package raft

import "log"

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func limitSize(ents []Entry, maxSize int) []Entry {
	if len(ents) == 0 {
		return ents
	}
	if maxSize > len(ents) {
		return ents
	}
	return ents[0:maxSize]
}
