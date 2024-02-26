package raft

import (
	"errors"
	"sync"
)

type LogStore struct {
	snapshot     []byte
	commitIndex  int
	appliedIndex int
	logs         []Entry

	mu         sync.RWMutex
	startIndex int
	endIndex   int
	startTerm  int
	endTerm    int
}

func NewStore() *LogStore {
	return &LogStore{
		snapshot:     nil,
		commitIndex:  0,
		appliedIndex: 0,
		logs:         []Entry{{Command: nil, Term: 0, Index: 0}},
	}
}

func (store *LogStore) Init(snapshot []byte, commitIndex, appliedIndex int, logs []Entry) {
	store.mu.Lock()
	defer store.mu.Unlock()

	store.snapshot = snapshot
	store.commitIndex = commitIndex
	store.appliedIndex = appliedIndex
	store.logs = logs

	store.startIndex = logs[0].Index
	store.endTerm = logs[len(logs)-1].Index
	store.startTerm = logs[0].Term
	store.endTerm = logs[len(logs)-1].Term
}

func (store *LogStore) ReadState() (snapshot []byte, commitIndex, appliedIndex int, logs []Entry) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	snapshot = store.snapshot
	commitIndex = store.commitIndex
	appliedIndex = store.appliedIndex
	logs = store.logs
	return
}

var errOutOfBound = errors.New("index out of bound")

func (store *LogStore) From(from int, limit int) (entries []Entry, term int, index int, err error) {
	store.mu.RLock()
	defer store.mu.RUnlock()

	if from <= store.startIndex {
		err = errOutOfBound
		return
	}

	if from > store.endIndex {
		term = store.endTerm
		index = store.endIndex
		return
	}

	entries = make([]Entry, Min(store.endIndex-store.startIndex+1, limit))
	copy(entries, store.logs[from-store.startIndex:])
	term = store.logs[from-store.startIndex-1].Term
	index = store.logs[from-store.startIndex-1].Index
	return
}

func (store *LogStore) UpToDate(otherTerm int, otherIndex int) bool {
	store.mu.RLock()
	defer store.mu.RUnlock()

	return store.endTerm > otherTerm ||
		(store.endTerm == otherTerm && store.endIndex > otherIndex)
}

func (store *LogStore) Compact(prevTerm, prevIndex int, entries []Entry) (bool, int) {
	store.mu.Lock()
	defer store.mu.Unlock()

	if prevIndex < store.startIndex || prevIndex > store.endIndex {
		return false, -1
	}
	entry := store.logs[prevIndex-store.startIndex]
	if entry.Term != prevTerm {
		return false, -1
	}

	appendIndex := 0
	keepIndex := prevIndex + 1 - store.startIndex
	for appendIndex < len(entries) && keepIndex < len(store.logs) {
		if entries[appendIndex].Term != store.logs[keepIndex].Term {
			break
		}
		appendIndex++
		keepIndex++
	}

	if appendIndex < len(entries) {
		store.logs = append(store.logs[:keepIndex], entries[appendIndex:]...)
		store.endIndex = entries[len(entries)-1].Index
		store.endTerm = entries[len(entries)-1].Term
	}
	if len(entries) == 0 {
		return true, prevIndex
	} else {
		return true, entries[len(entries)-1].Index
	}
}

func (store *LogStore) Forgotten(index int, snapshot []byte) error {
	store.mu.Lock()
	defer store.mu.Unlock()

	if index > store.appliedIndex {
		return errOutOfBound
	}
	startIndex := index - store.startIndex
	store.logs = store.logs[startIndex:]
	store.startIndex = store.logs[0].Index
	store.startTerm = store.logs[0].Term
	store.snapshot = snapshot
	return nil
}

func (store *LogStore) Append(term int, command interface{}) int {
	store.mu.Lock()
	defer store.mu.Unlock()

	entry := Entry{
		Command: command,
		Term:    term,
		Index:   store.endIndex + 1,
	}
	store.logs = append(store.logs, entry)
	store.endIndex = entry.Index
	store.endTerm = entry.Term
	return entry.Index
}

func (store *LogStore) CommitAndApply(commitIndex int, applyFunc func(Entry)) bool {
	store.mu.Lock()
	defer store.mu.Unlock()

	if commitIndex <= store.commitIndex {
		return false
	}

	store.commitIndex = commitIndex
	for i := store.appliedIndex + 1; i <= commitIndex; i++ {
		applyFunc(store.logs[i-store.startIndex])
		store.appliedIndex++
	}
	return true
}
