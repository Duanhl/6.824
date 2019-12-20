package sstable

type SSTable struct {
	state State
}

type State uint8

const (
	Started     State = 0
	Initialized       = 1
	Closed            = 2
)

type Entry struct {
	Key   string
	Value string
}

func (st *SSTable) get(key string) string {
	return ""
}

func (st *SSTable) rangeKey(start string, end string) []Entry {
	return make([]Entry, 1)
}
