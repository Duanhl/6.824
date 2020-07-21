package sstable

import "math/rand"

const (
	SlMaxLevel = 12  // 2 ** 12 = 4096, at most 4096 element dump to log files
	SlP        = 0.5 //random probability
)

type KV struct {
	key string
	val string
}

type SkipListNode struct {
	key      string
	val      string
	backward *SkipListNode
	forwards []*SkipListNode //for n'th level forward
}

type SkipList struct {
	header *SkipListNode
	tail   *SkipListNode
	length int64
	level  int
}

type NotFoundKeyError struct {
	errMsg string
}

func (nfe *NotFoundKeyError) Error() string {
	return nfe.errMsg
}

func NotFoundError(key string) *NotFoundKeyError {
	return &NotFoundKeyError{errMsg: key + " not found"}
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

func (sl *SkipList) Get(key string) (string, error, int) {
	sln, count := sl.lastLevelNode(key)
	if sln.key != key {
		return "", NotFoundError(key), count
	} else {
		return sln.val, nil, count
	}
}

func (sl *SkipList) Range(start, end string) []KV {
	if end < start {
		return nil
	}
	var sls []KV
	lln, _ := sl.lastLevelNode(start)
	for lln.key <= end {
		sls = append(sls, KV{
			key: lln.key,
			val: lln.val,
		})
		lln = lln.forwards[0]
	}
	return sls
}

func (sl *SkipList) lastLevelNode(key string) (*SkipListNode, int) {
	count := 0
	x := sl.header
	for i := sl.level - 1; i > -1; i-- {
		for forwardKey := x.forwards[i].key; forwardKey != "" && forwardKey < key; forwardKey = x.forwards[i].key {
			x = x.forwards[i]
			count += 1
		}
	}
	return x.forwards[0], count
}

func (sl *SkipList) Put(key string, val string) string {
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
		return ""
	}
}

func (sl *SkipList) Del(key string) (string, error) {
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
		return x.val, nil
	} else {
		return "", NotFoundError(key)
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
		backward: nil,
		forwards: levels,
	}
}

func randomLevel() int {
	lvl := 0
	for rand.Float32() < SlP && lvl < SlMaxLevel {
		lvl++
	}
	return lvl
}
