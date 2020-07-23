package sstable

import (
	"log"
	"os"
	"strconv"
	"testing"
)

func TestSkipList_Get(t *testing.T) {
	sl := createSl()
	sl.Put("Hello", "World")
	exceptedGet(sl, "Hello", "World")
	sl.Put("test", "t1")
	exceptedGet(sl, "test", "t1")
	sl.Put("Just", "Fire")
	exceptedGet(sl, "Just", "Fire")
}

func TestSkipList_DupPut(t *testing.T) {
	sl := createSl()
	sl.Put("test", "1")
	exceptedGet(sl, "test", "1")
	sl.Put("test", "2")
	exceptedGet(sl, "test", "2")
}

func TestSkipList_NotFoundPut(t *testing.T) {
	sl := createSl()
	sl.Put("test", "1")
	sl.Put("didi", "2")
	if _, err := sl.Get("Text"); err == nil {
		log.Fatal("Get Wrong Value")
	}
}

func TestSkipList_Del(t *testing.T) {
	sl := createSl()
	sl.Put("test", "1")
	sl.Put("didi", "2")
	exceptedGet(sl, "test", "1")
	sl.Del("test")
	if _, err := sl.Get("test"); err == nil {
		log.Fatal("Get Wrong Value")
	}
	exceptedGet(sl, "didi", "2")
}

func TestSkipList_Range(t *testing.T) {
	sl := createSl()
	for i := 1000; i < 2000; i++ {
		sl.Put(strconv.Itoa(i), strconv.Itoa(i))
	}
	rangeRes := sl.Range("1100", "1150")
	for i := 1100; i < 1150; i++ {
		if rangeRes[i-1100].key != strconv.Itoa(i) {
			log.Fatal("error range value get")
		}
	}
}

func exceptedGet(sl SkipList, key string, excepted string) {
	if res, err := sl.Get(key); err != nil {
		log.Fatal(err)
	} else {
		if res != excepted {
			log.Fatal("ger error value: " + res)
		}
	}
}

func BenchmarkSkipList_Get(b *testing.B) {
	sl := createSl()
	for i := 1000; i < 2000; i++ {
		sl.Put(strconv.Itoa(i), strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i%1000 + 1000
		exceptedGet(sl, strconv.Itoa(idx), strconv.Itoa(idx))
	}
}

func BenchmarkSkipList_Put(b *testing.B) {
	sl := createSl()
	for i := 0; i < b.N; i++ {
		idx := i%1000 + 1000
		sl.Put(strconv.Itoa(idx), strconv.Itoa(idx))
	}
}

func TestSkipList_Iter(t *testing.T) {
	sl := createSl()
	for i := 0; i < 1024; i++ {
		sl.Put(strconv.Itoa(i), strconv.Itoa(i))
	}
	iter := sl.IterKey("770")
	count := 8
	for i := 0; i < count; i++ {
		kv, _ := iter.Next()
		if kv.key != strconv.Itoa(771+i) {
			log.Fatal("error iter value get")
		}
	}
}

func TestMmap(t *testing.T) {
	f, err := os.Create("/tmp/sstable/log000")
	if err != nil {
		t.Fatal(err)
	}

	mmap, err := MapNewRegion(f, 0, BlockSize)
	if err != nil {
		t.Fatal(err)
	}
	defer mmap.close()

	target := "Just in File"
	b1 := []byte(target)
	if _, err := mmap.Write(b1); err != nil {
		t.Fatal(err)
	}

	b2 := make([]byte, len(b1))
	if _, err := mmap.Read(b2); err != nil {
		t.Fatal(err)
	}

	if target != string(b2) {
		t.Fatal("Read error value: " + string(b2))
	}
}
