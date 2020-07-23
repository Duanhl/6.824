package sstable

import "sync"

type SSTable struct {
	state     State
	lock      *sync.Mutex
	writeable *SkipList
	immutable *SkipList

	metadata *MetaDataBlock
}

type State uint8

const (
	Started     State = 0
	Initialized       = 1
	Closed            = 2
)

func (st *SSTable) Start() {
}

func (st *SSTable) Close() {

}

func (st *SSTable) Get(key string) string {
	return ""
}

func (st *SSTable) Put(key string, value string) string {
	st.lock.Lock()
	defer st.lock.Unlock()

	return ""
}

func (st *SSTable) Range(start string, end string) []KV {
	return make([]KV, 1)
}
