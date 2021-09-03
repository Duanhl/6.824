package kvpaxos

import "testing"
import "runtime"
import "strconv"
import "time"
import "fmt"
import "math/rand"
import "strings"
import "sync/atomic"

func check(t *testing.T, ck *Clerk, key string, value string) {
	v := ck.Get(key)
	if v != value {
		t.Fatalf("Get(%v) -> %v, expected %v", key, v, value)
	}
}

// predict effect of Append(k, val) if old value is prev.
func NextValue(prev string, val string) string {
	return prev + val
}

func TestBasic(t *testing.T) {
	const nservers = 3
	cfg := make_config(t, nservers, false)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())
	var cka [nservers]*Clerk
	for i := 0; i < nservers; i++ {
		cka[i] = cfg.makeClient([]int{i})
	}

	fmt.Printf("Test: Basic put/append/get ...\n")

	ck.Append("app", "x")
	ck.Append("app", "y")
	check(t, ck, "app", "xy")

	ck.Put("a", "aa")
	check(t, ck, "a", "aa")

	cka[1].Put("a", "aaa")

	check(t, cka[2], "a", "aaa")
	check(t, cka[1], "a", "aaa")
	check(t, ck, "a", "aaa")

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Concurrent clients ...\n")

	for iters := 0; iters < 20; iters++ {
		const npara = 15
		var ca [npara]chan bool
		for nth := 0; nth < npara; nth++ {
			ca[nth] = make(chan bool)
			go func(me int) {
				defer func() { ca[me] <- true }()
				ci := (rand.Int() % nservers)
				myck := cfg.makeClient([]int{ci})
				if (rand.Int() % 1000) < 500 {
					myck.Put("b", strconv.Itoa(rand.Int()))
				} else {
					myck.Get("b")
				}
			}(nth)
		}
		for nth := 0; nth < npara; nth++ {
			<-ca[nth]
		}
		var va [nservers]string
		for i := 0; i < nservers; i++ {
			va[i] = cka[i].Get("b")
			if va[i] != va[0] {
				t.Fatalf("mismatch")
			}
		}
	}

	fmt.Printf("  ... Passed\n")

	time.Sleep(1 * time.Second)
}

func TestDone(t *testing.T) {
	const nservers = 3
	cfg := make_config(t, nservers, false)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())
	var cka [nservers]*Clerk
	for pi := 0; pi < nservers; pi++ {
		cka[pi] = cfg.makeClient([]int{pi})
	}

	fmt.Printf("Test: server frees Paxos log memory...\n")

	ck.Put("a", "aa")
	check(t, ck, "a", "aa")

	runtime.GC()
	var m0 runtime.MemStats
	runtime.ReadMemStats(&m0)
	// rtm's m0.Alloc is 2 MB

	sz := 1000000
	items := 10

	for iters := 0; iters < 2; iters++ {
		for i := 0; i < items; i++ {
			key := strconv.Itoa(i)
			value := make([]byte, sz)
			for j := 0; j < len(value); j++ {
				value[j] = byte((rand.Int() % 100) + 1)
			}
			ck.Put(key, string(value))
			check(t, cka[i%nservers], key, string(value))
		}
	}

	// Put and Get to each of the replicas, in case
	// the Done information is piggybacked on
	// the Paxos proposer messages.
	for iters := 0; iters < 2; iters++ {
		for pi := 0; pi < nservers; pi++ {
			cka[pi].Put("a", "aa")
			check(t, cka[pi], "a", "aa")
		}
	}

	time.Sleep(1 * time.Second)

	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	// rtm's m1.Alloc is 45 MB

	// fmt.Printf("  Memory: before %v, after %v\n", m0.Alloc, m1.Alloc)

	allowed := m0.Alloc + uint64(nservers*items*sz*2)
	if m1.Alloc > allowed {
		t.Fatalf("Memory use did not shrink enough (Used: %v, allowed: %v).\n", m1.Alloc, allowed)
	}

	fmt.Printf("  ... Passed\n")
}

func TestPartition(t *testing.T) {
	const nservers = 5
	cfg := make_config(t, nservers, false)
	defer cfg.cleanup()

	var cka [nservers]*Clerk
	for i := 0; i < nservers; i++ {
		cka[i] = cfg.makeClient([]int{i})
	}

	fmt.Printf("Test: No partition ...\n")

	cfg.partition([]int{0, 1, 2, 3, 4}, []int{})

	cka[0].Put("1", "12")
	cka[2].Put("1", "13")
	check(t, cka[3], "1", "13")

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Progress in majority ...\n")

	cfg.partition([]int{2, 3, 4}, []int{0, 1})
	cka[2].Put("1", "14")
	check(t, cka[4], "1", "14")

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: No progress in minority ...\n")

	done0 := make(chan bool)
	done1 := make(chan bool)
	go func() {
		cka[0].Put("1", "15")
		done0 <- true
	}()
	go func() {
		cka[1].Get("1")
		done1 <- true
	}()

	select {
	case <-done0:
		t.Fatalf("Put in minority completed")
	case <-done1:
		t.Fatalf("Get in minority completed")
	case <-time.After(time.Second):
	}

	check(t, cka[4], "1", "14")
	cka[3].Put("1", "16")
	check(t, cka[4], "1", "16")

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Completion after heal ...\n")

	cfg.partition([]int{0, 2, 3, 4}, []int{1})

	select {
	case <-done0:
	case <-time.After(30 * 100 * time.Millisecond):
		t.Fatalf("Put did not complete")
	}

	select {
	case <-done1:
		t.Fatalf("Get in minority completed")
	default:
	}

	check(t, cka[4], "1", "15")
	check(t, cka[0], "1", "15")

	cfg.partition([]int{0, 1, 2}, []int{3, 4})

	select {
	case <-done1:
	case <-time.After(100 * 100 * time.Millisecond):
		t.Fatalf("Get did not complete")
	}

	check(t, cka[1], "1", "15")

	fmt.Printf("  ... Passed\n")
}

// check that all known appends are present in a value,
// and are in order for each concurrent client.
func checkAppends(t *testing.T, v string, counts []int) {
	nclients := len(counts)
	for i := 0; i < nclients; i++ {
		lastoff := -1
		for j := 0; j < counts[i]; j++ {
			wanted := "x " + strconv.Itoa(i) + " " + strconv.Itoa(j) + " y"
			off := strings.Index(v, wanted)
			if off < 0 {
				t.Fatalf("missing element in Append result")
			}
			off1 := strings.LastIndex(v, wanted)
			if off1 != off {
				t.Fatalf("duplicate element in Append result")
			}
			if off <= lastoff {
				t.Fatalf("wrong order for element in Append result")
			}
			lastoff = off
		}
	}
}

func TestUnreliable(t *testing.T) {
	const nservers = 3
	cfg := make_config(t, nservers, true)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())
	var cka [nservers]*Clerk
	for i := 0; i < nservers; i++ {
		cka[i] = cfg.makeClient([]int{i})
	}

	fmt.Printf("Test: Basic put/get, unreliable ...\n")

	ck.Put("a", "aa")
	check(t, ck, "a", "aa")

	cka[1].Put("a", "aaa")

	check(t, cka[2], "a", "aaa")
	check(t, cka[1], "a", "aaa")
	check(t, ck, "a", "aaa")

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Sequence of puts, unreliable ...\n")

	for iters := 0; iters < 6; iters++ {
		const ncli = 5
		var ca [ncli]chan bool
		for cli := 0; cli < ncli; cli++ {
			ca[cli] = make(chan bool)
			go func(me int) {
				ok := false
				defer func() { ca[me] <- ok }()
				myck := cfg.makeClient(cfg.All())
				key := strconv.Itoa(me)
				vv := myck.Get(key)
				myck.Append(key, "0")
				vv = NextValue(vv, "0")
				myck.Append(key, "1")
				vv = NextValue(vv, "1")
				myck.Append(key, "2")
				vv = NextValue(vv, "2")
				time.Sleep(100 * time.Millisecond)
				if myck.Get(key) != vv {
					t.Fatalf("wrong value")
				}
				if myck.Get(key) != vv {
					t.Fatalf("wrong value")
				}
				ok = true
			}(cli)
		}
		for cli := 0; cli < ncli; cli++ {
			x := <-ca[cli]
			if x == false {
				t.Fatalf("failure")
			}
		}
	}

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Concurrent clients, unreliable ...\n")

	for iters := 0; iters < 20; iters++ {
		const ncli = 15
		var ca [ncli]chan bool
		for cli := 0; cli < ncli; cli++ {
			ca[cli] = make(chan bool)
			go func(me int) {
				defer func() { ca[me] <- true }()
				myck := cfg.makeClient(cfg.All())
				if (rand.Int() % 1000) < 500 {
					myck.Put("b", strconv.Itoa(rand.Int()))
				} else {
					myck.Get("b")
				}
			}(cli)
		}
		for cli := 0; cli < ncli; cli++ {
			<-ca[cli]
		}

		var va [nservers]string
		for i := 0; i < nservers; i++ {
			va[i] = cka[i].Get("b")
			if va[i] != va[0] {
				t.Fatalf("mismatch; 0 got %v, %v got %v", va[0], i, va[i])
			}
		}
	}

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Concurrent Append to same key, unreliable ...\n")

	ck.Put("k", "")

	ff := func(me int, ch chan int) {
		ret := -1
		defer func() { ch <- ret }()
		myck := cfg.makeClient(cfg.All())
		n := 0
		for n < 5 {
			myck.Append("k", "x "+strconv.Itoa(me)+" "+strconv.Itoa(n)+" y")
			n++
		}
		ret = n
	}

	ncli := 5
	cha := []chan int{}
	for i := 0; i < ncli; i++ {
		cha = append(cha, make(chan int))
		go ff(i, cha[i])
	}

	counts := []int{}
	for i := 0; i < ncli; i++ {
		n := <-cha[i]
		if n < 0 {
			t.Fatal("client failed")
		}
		counts = append(counts, n)
	}

	vx := ck.Get("k")
	checkAppends(t, vx, counts)

	{
		for i := 0; i < nservers; i++ {
			vi := cka[i].Get("k")
			if vi != vx {
				t.Fatalf("mismatch; 0 got %v, %v got %v", vx, i, vi)
			}
		}
	}

	fmt.Printf("  ... Passed\n")

	time.Sleep(1 * time.Second)
}

func TestHole(t *testing.T) {
	const nservers = 5
	cfg := make_config(t, nservers, false)
	defer cfg.cleanup()

	fmt.Printf("Test: Tolerates holes in paxos sequence ...\n")

	for iters := 0; iters < 5; iters++ {
		cfg.partition([]int{0, 1, 2, 3, 4}, []int{})

		ck2 := cfg.makeClient([]int{2})
		ck2.Put("q", "q")

		done := int32(0)
		const nclients = 10
		var ca [nclients]chan bool
		for xcli := 0; xcli < nclients; xcli++ {
			ca[xcli] = make(chan bool)
			go func(cli int) {
				ok := false
				defer func() { ca[cli] <- ok }()
				var cka [nservers]*Clerk
				for i := 0; i < nservers; i++ {
					cka[i] = cfg.makeClient([]int{i})
				}
				key := strconv.Itoa(cli)
				last := ""
				cka[0].Put(key, last)
				for atomic.LoadInt32(&done) == 0 {
					ci := (rand.Int() % 2)
					if (rand.Int() % 1000) < 500 {
						nv := strconv.Itoa(rand.Int())
						cka[ci].Put(key, nv)
						last = nv
					} else {
						v := cka[ci].Get(key)
						if v != last {
							t.Fatalf("%v: wrong value, key %v, wanted %v, got %v",
								cli, key, last, v)
						}
					}
				}
				ok = true
			}(xcli)
		}

		time.Sleep(3 * time.Second)

		cfg.partition([]int{2, 3, 4}, []int{0, 1})

		// can majority partition make progress even though
		// minority servers were interrupted in the middle of
		// paxos agreements?
		check(t, ck2, "q", "q")
		ck2.Put("q", "qq")
		check(t, ck2, "q", "qq")

		// restore network, wait for all threads to exit.
		cfg.partition([]int{0, 1, 2, 3, 4}, []int{})
		atomic.StoreInt32(&done, 1)
		ok := true
		for i := 0; i < nclients; i++ {
			z := <-ca[i]
			ok = ok && z
		}
		if ok == false {
			t.Fatal("something is wrong")
		}
		check(t, ck2, "q", "qq")
	}

	fmt.Printf("  ... Passed\n")
}

func TestManyPartition(t *testing.T) {
	const nservers = 5
	cfg := make_config(t, nservers, true)
	defer cfg.cleanup()

	fmt.Printf("Test: Many clients, changing partitions ...\n")

	done := int32(0)

	// re-partition periodically
	ch1 := make(chan bool)
	go func() {
		defer func() { ch1 <- true }()
		for atomic.LoadInt32(&done) == 0 {
			var a [nservers]int
			for i := 0; i < nservers; i++ {
				a[i] = (rand.Int() % 2)
			}
			pa := make([][]int, 2)
			for i := 0; i < 2; i++ {
				pa[i] = make([]int, 0)
				for j := 0; j < nservers; j++ {
					if a[j] == i {
						pa[i] = append(pa[i], j)
					}
				}
			}
			cfg.partition(pa[0], pa[1])
			time.Sleep(time.Duration(rand.Int63()%200) * time.Millisecond)
		}
	}()

	const nclients = 10
	var ca [nclients]chan bool
	for xcli := 0; xcli < nclients; xcli++ {
		ca[xcli] = make(chan bool)
		go func(cli int) {
			ok := false
			defer func() { ca[cli] <- ok }()

			myck := cfg.makeClient(cfg.All())
			key := strconv.Itoa(cli)
			last := ""
			myck.Put(key, last)
			for atomic.LoadInt32(&done) == 0 {
				if (rand.Int() % 1000) < 500 {
					nv := strconv.Itoa(rand.Int())
					myck.Append(key, nv)
					last = NextValue(last, nv)
				} else {
					v := myck.Get(key)
					if v != last {
						t.Fatalf("%v: get wrong value, key %v, wanted %v, got %v",
							cli, key, last, v)
					}
				}
			}
			ok = true
		}(xcli)
	}

	time.Sleep(20 * time.Second)
	atomic.StoreInt32(&done, 1)
	<-ch1
	cfg.partition([]int{0, 1, 2, 3, 4}, []int{})

	ok := true
	for i := 0; i < nclients; i++ {
		z := <-ca[i]
		ok = ok && z
	}

	if ok {
		fmt.Printf("  ... Passed\n")
	}
}
