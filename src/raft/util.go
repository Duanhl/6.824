package raft

import (
	"log"
	"sort"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

func Majority(arr []int) int {
	quorum := (len(arr) - 1) / 2
	target := make([]int, len(arr))
	copy(target, arr)
	sort.Ints(target)
	return target[quorum]
}
