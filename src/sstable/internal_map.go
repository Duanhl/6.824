package sstable

import "os"

type LevelDB struct {
	fd       *os.File
	cache    map[string]string
	segments *[]Segment
}

type Segment struct {
}
