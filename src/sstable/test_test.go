package sstable

import (
	"log"
	"strconv"
	"testing"
)

func TestSkipList_Get(t *testing.T) {
	sl := createSl()
	sl.Put("Hello", "World")
	exceptedGet(sl, "Hello", "World", t)
	sl.Put("test", "t1")
	exceptedGet(sl, "test", "t1", t)
	sl.Put("Just", "Fire")
	exceptedGet(sl, "Just", "Fire", t)
}

func TestSkipList_DupPut(t *testing.T) {
	sl := createSl()
	sl.Put("test", "1")
	exceptedGet(sl, "test", "1", t)
	sl.Put("test", "2")
	exceptedGet(sl, "test", "2", t)
}

func TestSkipList_NotFoundPut(t *testing.T) {
	sl := createSl()
	sl.Put("test", "1")
	sl.Put("didi", "2")
	if _, err, _ := sl.Get("Text"); err == nil {
		log.Fatal("Get Wrong Value")
	}
}

func TestSkipList_Del(t *testing.T) {
	sl := createSl()
	sl.Put("test", "1")
	sl.Put("didi", "2")
	exceptedGet(sl, "test", "1", t)
	sl.Del("test")
	if _, err, _ := sl.Get("test"); err == nil {
		log.Fatal("Get Wrong Value")
	}
	exceptedGet(sl, "didi", "2", t)
}

func TestSkipList_GetCount(t *testing.T) {
	sl := createSl()
	for i := 1001; i < 2000; i++ {
		sl.Put(strconv.Itoa(i), strconv.Itoa(i))
	}
	target := "1789"
	if val, err, count := sl.Get(target); err != nil {
		log.Fatal("Not Found Key")
	} else {
		if val != target {
			log.Fatal("Get Error Value")
		}
		if count > 40 {
			log.Fatal("Query Too Many")
		}
	}
}

func TestSkipList_Range(t *testing.T) {
	sl := createSl()
	for i := 1001; i < 2000; i++ {
		sl.Put(strconv.Itoa(i), strconv.Itoa(i))
	}
	rangeRes := sl.Range("1100", "1150")
	for i := 1100; i < 1150; i++ {
		if rangeRes[i-1100].key != strconv.Itoa(i) {
			log.Fatal("error range value get")
		}
	}
}

func exceptedGet(sl SkipList, key string, excepted string, t *testing.T) {
	if res, err, _ := sl.Get(key); err != nil {
		t.Fatal(err)
	} else {
		if res != excepted {
			t.Fatal("ger error value: " + res)
		}
	}
}

func BenchmarkSkipList_Get(b *testing.B) {

}

func BenchmarkSkipList_Put(b *testing.B) {

}
