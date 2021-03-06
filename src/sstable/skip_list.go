package sstable

import "math/rand"

const (
	SlP        = 0.25 //random probability, Normalized Search Times L(n)/p, avg points per node 1/(1-p)
	SlMaxLevel = 8    // (1 / SLP) ** 8 = 65536, at most 65536 element dump to log files
)

type KV struct {
	key string
	val string
}

type SkipListNode struct {
	key      string
	val      string
	forwards []*SkipListNode //for n'th level forward
}

type SkipList struct {
	header *SkipListNode
	length int
	level  int
}

type SkipListIterator struct {
	sl   *SkipList
	curr *SkipListNode
}

func (sli *SkipListIterator) HasNext() bool {
	return sli.curr.forwards[0].key != ""
}

func (sli *SkipListIterator) Next() (*KV, error) {
	if !sli.HasNext() {
		return nil, NoNextElementError
	} else {
		kv := &KV{
			key: sli.curr.forwards[0].key,
			val: sli.curr.forwards[0].val,
		}
		sli.curr = sli.curr.forwards[0]
		return kv, nil
	}
}

func createSl() SkipList {
	header := makeNode(SlMaxLevel, "", "")
	skipList := SkipList{
		length: 0,
		level:  0,
		header: header,
	}
	return skipList
}

func (sl *SkipList) Get(key string) (string, error) {
	if key == "" {
		return "", IllegalArgumentErrors("key", "")
	}
	sln := sl.lastLevelNode(key)
	if sln.key != key {
		return "", NotFoundError(key)
	} else {
		return sln.val, nil
	}
}

func (sl *SkipList) Range(start, end string) []KV {
	if end < start {
		return nil
	}
	var sls []KV
	lln := sl.lastLevelNode(start)
	if lln.key == "" {
		return nil
	}
	if lln.key == start {
		sls = append(sls, KV{
			key: lln.key,
			val: lln.val,
		})
	}
	lln = lln.forwards[0]
	for lln.key != "" && lln.key <= end {
		sls = append(sls, KV{
			key: lln.key,
			val: lln.val,
		})
		lln = lln.forwards[0]
	}
	return sls
}

func (sl *SkipList) lastLevelNode(key string) *SkipListNode {
	x := sl.header
	for i := sl.level - 1; i > -1; i-- {
		for forwardKey := x.forwards[i].key; forwardKey != "" && forwardKey < key; forwardKey = x.forwards[i].key {
			x = x.forwards[i]
		}
	}
	return x.forwards[0]
}

func (sl *SkipList) Put(key string, val string) string {
	if key == "" {
		return ""
	}
	update := make([]*SkipListNode, SlMaxLevel)
	x := sl.header
	for i := sl.level - 1; i > -1; i-- {
		for forwardKey := x.forwards[i].key; forwardKey != "" && forwardKey < key; forwardKey = x.forwards[i].key {
			x = x.forwards[i]
		}
		update[i] = x
	}
	x = x.forwards[0]
	if x.key == key {
		res := x.val
		x.val = val
		return res
	} else {
		lvl := randomLevel()
		if lvl >= sl.level {
			for i := sl.level; i <= lvl; i++ {
				update[i] = sl.header
			}
			sl.level = lvl + 1
		}
		sln := makeNode(lvl+1, key, val)
		for i := 0; i <= lvl; i++ {
			sln.forwards[i] = update[i].forwards[i]
			update[i].forwards[i] = sln
		}
		sl.length++
		return ""
	}
}

func (sl *SkipList) Del(key string) (string, error) {
	if key == "" {
		return "", IllegalArgumentErrors("key", key)
	}
	update := make([]*SkipListNode, SlMaxLevel)
	x := sl.header
	for i := sl.level - 1; i > -1; i-- {
		for forwardKey := x.forwards[i].key; forwardKey != "" && forwardKey < key; forwardKey = x.forwards[i].key {
			x = x.forwards[i]
		}
		update[i] = x
	}
	x = x.forwards[0]
	if x.key == key {
		for i := 0; i < sl.level; i++ {
			if update[i].forwards[i] != x {
				break
			}
			update[i].forwards[i] = x.forwards[i]
		}
		for sl.level > -1 && sl.header.forwards[sl.level-1] == nil {
			sl.level -= 1
		}
		sl.length--
		return x.val, nil
	} else {
		return "", NotFoundError(key)
	}
}

func (sl *SkipList) Size() int {
	return sl.length
}

func (sl *SkipList) Iter() *SkipListIterator {
	return &SkipListIterator{
		sl:   sl,
		curr: sl.header,
	}
}

func (sl *SkipList) IterKey(key string) *SkipListIterator {
	llv := sl.lastLevelNode(key)
	return &SkipListIterator{
		sl:   sl,
		curr: llv,
	}
}

func makeNode(level int, key string, val string) *SkipListNode {
	levels := make([]*SkipListNode, level)
	for i := 0; i < level; i++ {
		levels[i] = &SkipListNode{}
	}
	return &SkipListNode{
		key:      key,
		val:      val,
		forwards: levels,
	}
}

func randomLevel() int {
	lvl := 0
	for rand.Float32() < SlP && lvl < SlMaxLevel-1 {
		lvl++
	}
	return lvl
}
