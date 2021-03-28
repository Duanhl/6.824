package sstable

//
//import (
//	"errors"
//	"os"
//	"syscall"
//)
//
//type mmap struct {
//	f    *os.File
//	data []byte
//
//	rp int
//	wp int
//}
//
//func MapNewRegion(f *os.File, offset int64, length int) (*mmap, error) {
//	if offset%int64(os.Getpagesize()) != 0 {
//		return nil, errors.New("offset must be a multiple of the system's page size")
//	}
//	if err := f.Truncate(offset + int64(length)); err != nil {
//		return nil, errors.New("truncate file error")
//	}
//	if data, err := syscall.Mmap(int(f.Fd()), offset, length, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED); err != nil {
//		return nil, errors.New("Map file failed")
//	} else {
//		return &mmap{
//			f:    f,
//			data: data,
//			rp:   0,
//			wp:   0,
//		}, nil
//	}
//}
//
//func (mmap *mmap) Write(p []byte) (int, error) {
//	count := 0
//	for i := 0; i < len(p) && mmap.wp < len(mmap.data); i++ {
//		mmap.data[i] = p[i]
//		count += 1
//		mmap.wp++
//	}
//	if count < len(p) {
//		return count, errors.New("p data out of mmap buf size")
//	} else {
//		return count, nil
//	}
//}
//
//func (mmap *mmap) Read(p []byte) (int, error) {
//	count := 0
//	for i := 0; i < len(p) && mmap.rp < len(mmap.data); i++ {
//		p[i] = mmap.data[i]
//		count++
//		mmap.rp++
//	}
//	if count < len(p) {
//		return count, errors.New("p data out of mmap buf size")
//	} else {
//		return count, nil
//	}
//}
//
//func (mmap *mmap) readSeek(offset int) error {
//	if offset < 0 || offset > len(mmap.data) {
//		return OutOfIndexError
//	}
//	mmap.rp = offset
//	return nil
//}
//
//func (mmap *mmap) close() error {
//	if err := syscall.Munmap(mmap.data); err != nil {
//		return err
//	}
//	if err := mmap.f.Close(); err != nil {
//		return err
//	}
//	return nil
//}
//
