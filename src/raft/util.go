package raft

import (
	"fmt"
	"log"
	"strings"
)

// Debugging
const Debug = 1

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

func entries2string(entries []LogEntry) string {
	arr := make([]string, len(entries)-1)
	for i, v := range entries {
		if i > 0 {
			arr[i-1] = fmt.Sprintf("%v", v.Val)
		}
	}
	return "[" + strings.Join(arr, ", ") + "]"
}
