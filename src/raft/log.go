package raft

type RaftLog struct {
	CommitIndex  int
	AppliedIndex int
	Entries      []Entry
}

type Entry struct {
	Type  string
	Term  int
	Index int
	Val   interface{}
}

func NewRaftLog() *RaftLog {
	entries := make([]Entry, 1)
	entries[0] = Entry{
		Type:  "",
		Term:  0,
		Index: 0,
		Val:   nil,
	}
	return &RaftLog{
		CommitIndex:  0,
		AppliedIndex: 0,
		Entries:      entries,
	}
}

func (rl *RaftLog) IsUpToDate(index, term int) bool {
	last := rl.Entries[len(rl.Entries)-1]
	return last.Term > term ||
		(last.Term == term && last.Index > index)
}

func (rl *RaftLog) Matched(index, term int) bool {
	return index < len(rl.Entries) && rl.Entries[index].Term == term
}

func (rl *RaftLog) AppendEntries(begin int, entries []Entry) {
	if len(entries) == 0 {
		rl.Entries = rl.Entries[:begin]
	} else {
		rl.Entries = append(rl.Entries[:begin], entries...)
	}
}

func (rl *RaftLog) Append(entry Entry) int {
	entry.Index = len(rl.Entries)
	rl.Entries = append(rl.Entries, entry)
	return entry.Index
}

func (rl *RaftLog) UnApplied() []Entry {
	if rl.AppliedIndex == rl.CommitIndex {
		return make([]Entry, 0)
	}
	return rl.Entries[rl.AppliedIndex+1 : rl.CommitIndex+1]
}

func (rl *RaftLog) UnCommitted() []Entry {
	return rl.Entries[rl.CommitIndex:]
}

func (rl *RaftLog) FindFirstIndexInTerm(term int) int {
	index := -1
	for i := 0; i < len(rl.Entries); i++ {
		if rl.Entries[i].Term == term {
			return rl.Entries[i].Index
		}
	}
	return index
}

func (rl *RaftLog) LastEntry() Entry {
	return rl.Entries[len(rl.Entries)-1]
}
