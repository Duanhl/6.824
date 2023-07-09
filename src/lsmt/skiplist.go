package lsmt

type SkipListNode struct {
	Key   string
	Value []byte
	Nexts []*SkipListNode
}

type SkipList struct {
	head *SkipListNode
	tail *SkipListNode

	maxHeight int
}

func (sl *SkipList) Get(key string) ([]byte, error) {
	return nil, nil
}

func (sl *SkipList) Put(key string, value []byte) error {
	return nil
}

func (sl *SkipList) Range(start string, leftInclusive bool,
	end string, rightInclusive bool) ([][]byte, error) {
	return nil, nil
}
