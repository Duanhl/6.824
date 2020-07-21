package sstable

import (
	"bytes"
	"os"
)

type RecordType int

const (
	DEL RecordType = 0
	ADD            = 1
)

const (
	KB         int64 = 1024
	BlockSize        = 64 * KB
	HeaderSize       = 4 + 1 + 2 //checkSum(4 bytes), type(1 byte), length(2 bytes)
)

type LogStruct struct {
}

type LogWriter struct {
	fd *os.File
}

func (lw *LogWriter) appendRecord(buffer bytes.Buffer, rtype RecordType) error {
	return nil
}

type LogReader struct {
	fd       *os.File
	checkSum bool
}
