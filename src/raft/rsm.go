package raft

import (
	"errors"
	"sync"
)

type Rsm struct {
	sn        []byte
	committed int
	applied   int
	logs      []Entry // always start with a dummy entry
	applyFunc func(entry Entry)
	mu        sync.Mutex
}

type HardRsm struct {
	committed int
	applied   int
	logs      []Entry // always start with a dummy entry
	sn        []byte
}

var IndexOutBoundError = errors.New("index out of bound")
var UnmatchArgumentError = errors.New("unmatch argument error")
var EntryValueForgottenError = errors.New("entry forgotten")

func (rsm *Rsm) EntryAt(index int) (Entry, error) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	if index <= rsm.logs[0].Index || index > rsm.logs[len(rsm.logs)-1].Index {
		return Entry{}, IndexOutBoundError
	}
	startIndex := rsm.logs[0].Index
	return rsm.logs[index-startIndex], nil
}

func (rsm *Rsm) EntryFrom(index int) ([]Entry, int, int, error) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	if index <= rsm.logs[0].Index || index > rsm.logs[len(rsm.logs)-1].Index {
		return nil, -1, -1, IndexOutBoundError
	}
	lastEntry, _ := rsm.LastEntry()
	if index == lastEntry.Index {
		return nil, lastEntry.Term, lastEntry.Index, nil
	} else {
		entries := rsm.logs[index-rsm.logs[0].Index:]
		prevEntry := rsm.logs[index-rsm.logs[0].Index-1]
		return entries, prevEntry.Term, prevEntry.Index, nil
	}
}

func (rsm *Rsm) hasUncommitted() bool {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	lastEntry, _ := rsm.LastEntry()
	return lastEntry.Index > rsm.committed
}

func (rsm *Rsm) CommitIndex() int {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	return rsm.committed
}

func (rsm *Rsm) AppliedIndex() int {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	return rsm.applied
}

// drop log from given start index, only uncommited
func (rsm *Rsm) Drop(index int) error {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	if index <= rsm.committed {
		return IndexOutBoundError
	}

	e, _ := rsm.LastEntry()
	if index > e.Index {
		return IndexOutBoundError
	}
	startIndex := index - rsm.logs[0].Index
	rsm.logs = rsm.logs[:startIndex]
	return nil
}

// forgotten log entries until given index, only applied entry
// can forgotten
func (rsm *Rsm) Forgotten(index int, sn []byte) error {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	if index > rsm.applied {
		return IndexOutBoundError
	}

	startIndex := index - rsm.logs[0].Index
	rsm.logs = rsm.logs[startIndex:]
	rsm.sn = sn
	return nil
}

func (rsm *Rsm) Commit(committed int) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	if committed <= rsm.committed {
		return
	}
	rsm.committed = committed
}

func (rsm *Rsm) LastEntry() (Entry, error) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	if len(rsm.logs) == 1 {
		return rsm.logs[0], EntryValueForgottenError
	}
	return rsm.logs[len(rsm.logs)-1], nil
}

// append log entry at tail
func (rsm *Rsm) Append(term int, command interface{}) int {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	e, _ := rsm.LastEntry()
	rsm.logs = append(rsm.logs, Entry{
		Command: command,
		Term:    term,
		Index:   e.Index + 1,
	})
	return e.Index + 1
}

func (rsm *Rsm) AppendEntries(entries []Entry) error {
	if len(entries) == 0 {
		return nil
	}

	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	first := entries[0]
	last, _ := rsm.LastEntry()
	if first.Index != last.Index+1 {
		return UnmatchArgumentError
	}
	rsm.logs = append(rsm.logs, entries...)
	return nil
}

func (rsm *Rsm) Apply(applied int) error {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	if applied <= rsm.applied {
		return nil
	}
	if applied > rsm.committed {
		return IndexOutBoundError
	}

	for index := rsm.applied + 1; index < applied; index++ {
		logIndex := index - rsm.logs[0].Index
		rsm.applyFunc(rsm.logs[logIndex])
	}
	rsm.applied = applied
	return nil
}

func (rsm *Rsm) getState() HardRsm {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()

	return HardRsm{
		committed: rsm.committed,
		applied:   rsm.applied,
		logs:      rsm.logs,
		sn:        rsm.sn,
	}
}
