package raft

import (
	"log"
	"sort"
	"sync/atomic"
)

// Debugging
const Debug = false

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

func Count(arr []bool) int {
	count := 0
	for i := 0; i < len(arr); i++ {
		if arr[i] {
			count++
		}
	}
	return count
}

type IDGenerator interface {
	NextId() int64
}

type Serializer struct {
	base int64
}

func (s *Serializer) NextId() int64 {
	atomic.AddInt64(&s.base, 1)
	return atomic.LoadInt64(&s.base)
}
