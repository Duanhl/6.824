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
	applyFunc func(Entry)
	mu        sync.Mutex
}

type HardRsm struct {
	Committed int
	Applied   int
	Logs      []Entry // always start with a dummy entry
}

func (rsm *Rsm) Initialize(hardRsm HardRsm, sn []byte) {
	rsm.sn = sn
	rsm.committed = hardRsm.Committed
	rsm.applied = hardRsm.Applied
	rsm.logs = hardRsm.Logs
}

var IndexOutBoundError = errors.New("index out of bound")
var UnmatchArgumentError = errors.New("unmatch argument error")

func (rsm *Rsm) EntryAt(index int) (Entry, error) {
	if index < rsm.logs[0].Index || index > rsm.logs[len(rsm.logs)-1].Index {
		return Entry{}, IndexOutBoundError
	}
	startIndex := rsm.logs[0].Index
	return rsm.logs[index-startIndex], nil
}

func (rsm *Rsm) EntryFrom(start, limit int) ([]Entry, int, int, error) {
	lastEntry := rsm.LastEntry()
	firstIndex := rsm.logs[0].Index
	if start <= firstIndex {
		return nil, -1, -1, IndexOutBoundError
	}
	if start > lastEntry.Index {
		return nil, lastEntry.Term, lastEntry.Index, nil
	} else {
		ents := make([]Entry, Min(lastEntry.Index-start+1, limit))
		copy(ents, rsm.logs[start-firstIndex:])
		return ents, rsm.logs[start-firstIndex-1].Term, rsm.logs[start-firstIndex-1].Index, nil
	}
}

func (rsm *Rsm) hasUncommitted() bool {
	lastEntry := rsm.LastEntry()
	return lastEntry.Index > rsm.committed
}

func (rsm *Rsm) UpToDate(otherTerm int, otherIndex int) bool {
	lastEntry := rsm.LastEntry()
	return lastEntry.Term > otherTerm ||
		(lastEntry.Term == otherTerm && lastEntry.Index > otherIndex)
}

func (rsm *Rsm) Compact(prevTerm, prevIndex int, entries []Entry) (bool, int) {
	entry, err := rsm.EntryAt(prevIndex)
	if err != nil || entry.Term != prevTerm {
		return false, -1
	}
	appendIndex := 0
	keepIndex := prevIndex + 1 - rsm.logs[0].Index
	for appendIndex < len(entries) && keepIndex < len(rsm.logs) {
		if entries[appendIndex].Term != rsm.logs[keepIndex].Term {
			break
		}
		appendIndex++
		keepIndex++
	}
	if appendIndex < len(entries) {
		rsm.logs = append(rsm.logs[:keepIndex], entries[appendIndex:]...)
	}
	if len(entries) == 0 {
		return true, prevIndex
	} else {
		return true, entries[len(entries)-1].Index
	}
}

// forgotten log entries until given index, only Applied entry
// can forgotten
func (rsm *Rsm) Forgotten(index int, sn []byte) error {
	if index > rsm.applied {
		return IndexOutBoundError
	}

	startIndex := index - rsm.logs[0].Index
	rsm.logs = rsm.logs[startIndex:]
	rsm.sn = sn
	return nil
}

func (rsm *Rsm) Commit(committed int) bool {
	if committed <= rsm.committed {
		return false
	}
	rsm.committed = committed
	zeroIndex := rsm.logs[0].Index
	for i := rsm.applied + 1; i <= rsm.committed; i++ {
		rsm.applyFunc(rsm.logs[i-zeroIndex])
		rsm.applied += 1
	}
	return true
}

func (rsm *Rsm) LastEntry() Entry {
	return rsm.logs[len(rsm.logs)-1]
}

// append log entry at tail
func (rsm *Rsm) Append(term int, command interface{}) int {
	e := rsm.LastEntry()
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

	first := entries[0]
	last := rsm.LastEntry()
	if first.Index != last.Index+1 {
		return UnmatchArgumentError
	}
	rsm.logs = append(rsm.logs, entries...)
	return nil
}

func (rsm *Rsm) getState() (HardRsm, []byte) {
	return HardRsm{
		Committed: rsm.committed,
		Applied:   rsm.applied,
		Logs:      rsm.logs,
	}, rsm.sn
}
