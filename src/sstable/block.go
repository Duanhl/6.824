package sstable

import (
	"io"
	"os"
)

type RecordType int

const (
	zeroType = 0
	fullType = 1

	firstType  = 2
	middleType = 3
	lastType   = 4
)

const (
	KB         int = 1024
	BlockSize      = 64 * KB
	HeaderSize     = 4 + 1 + 2 //checkSum(4 bytes), type(1 byte), length(2 bytes)
)

type LogStruct struct {
}

type LogWriter struct {
	fd          *io.Writer
	blockOffset int64
	crcs        []int32
}

func (lw *LogWriter) appendRecord(string) error {
	return nil
}

type LogReader struct {
	fd       *os.File
	checkSum bool
}

// operation log format
type OperationLog struct {
}

// metadata block
type MetaDataBlock struct {
	data  []DataBlock
	dirty []DataBlock
}

type MetaDataBlockStruct struct {
}

// data block
type DataBlock struct {
	id    string
	start string
	end   string
	bloom BloomFilter
	data  *mmap
}

type DataBlockStruct struct {
	htype    byte
	checksum uint32
	length   [2]byte
	data     []byte
}

type BloomFilter struct {
}
