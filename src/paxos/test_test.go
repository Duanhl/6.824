package paxos

import "testing"
import "runtime"
import "time"
import "fmt"
import "math/rand"
import "sync/atomic"

func TestSpeed(t *testing.T) {
	const npaxos = 3
	cfg := make_config(t, npaxos, false)
	defer cfg.cleanup()

	t0 := time.Now()

	for i := 0; i < 20; i++ {
		cfg.paxos[0].Start(i, "x")
		cfg.waitn(i, npaxos)
	}

	d := time.Since(t0)
	fmt.Printf("20 agreements %v seconds\n", d.Seconds())
}

func TestBasic(t *testing.T) {
	const npaxos = 3
	cfg := make_config(t, npaxos, false)
	defer cfg.cleanup()

	fmt.Printf("Test: Single proposer ...\n")

	cfg.paxos[0].Start(0, "hello")
	cfg.waitn(0, npaxos)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Many proposers, same value ...\n")

	for i := 0; i < npaxos; i++ {
		cfg.paxos[i].Start(1, 77)
	}
	cfg.waitn(1, npaxos)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Many proposers, different values ...\n")

	cfg.paxos[0].Start(2, 100)
	cfg.paxos[1].Start(2, 101)
	cfg.paxos[2].Start(2, 102)
	cfg.waitn(2, npaxos)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Out-of-order instances ...\n")

	cfg.paxos[0].Start(7, 700)
	cfg.paxos[0].Start(6, 600)
	cfg.paxos[1].Start(5, 500)
	cfg.waitn(7, npaxos)
	cfg.paxos[0].Start(4, 400)
	cfg.paxos[1].Start(3, 300)
	cfg.waitn(6, npaxos)
	cfg.waitn(5, npaxos)
	cfg.waitn(4, npaxos)
	cfg.waitn(3, npaxos)

	if cfg.paxos[0].Max() != 7 {
		t.Fatalf("wrong Max()")
	}

	fmt.Printf("  ... Passed\n")
}

func TestDeaf(t *testing.T) {
	const npaxos = 5
	cfg := make_config(t, npaxos, false)
	defer cfg.cleanup()

	fmt.Printf("Test: Deaf proposer ...\n")

	cfg.paxos[0].Start(0, "hello")
	cfg.waitn(0, npaxos)

	cfg.disconnect(0)
	cfg.disconnect(npaxos - 1)

	cfg.paxos[1].Start(1, "goodbye")
	cfg.waitmajority(1)
	time.Sleep(1 * time.Second)
	if cfg.ndecided(1) != npaxos-2 {
		t.Fatalf("a deaf peer heard about a decision")
	}

	cfg.connect(0)
	cfg.paxos[0].Start(1, "xxx")
	cfg.waitn(1, npaxos-1)
	time.Sleep(1 * time.Second)
	if cfg.ndecided(1) != npaxos-1 {
		t.Fatalf("a deaf peer heard about a decision")
	}

	cfg.connect(npaxos - 1)
	cfg.paxos[npaxos-1].Start(1, "yyy")
	cfg.waitn(1, npaxos)

	fmt.Printf("  ... Passed\n")
}

func TestForget(t *testing.T) {
	const npaxos = 6
	cfg := make_config(t, npaxos, false)
	defer cfg.cleanup()

	fmt.Printf("Test: Forgetting ...\n")

	// initial Min() correct?
	for i := 0; i < npaxos; i++ {
		m := cfg.paxos[i].Min()
		if m > 0 {
			t.Fatalf("wrong initial Min() %v", m)
		}
	}

	cfg.paxos[0].Start(0, "00")
	cfg.paxos[1].Start(1, "11")
	cfg.paxos[2].Start(2, "22")
	cfg.paxos[0].Start(6, "66")
	cfg.paxos[1].Start(7, "77")

	cfg.waitn(0, npaxos)

	// Min() correct?
	for i := 0; i < npaxos; i++ {
		m := cfg.paxos[i].Min()
		if m != 0 {
			t.Fatalf("wrong Min() %v; expected 0", m)
		}
	}

	cfg.waitn(1, npaxos)

	// Min() correct?
	for i := 0; i < npaxos; i++ {
		m := cfg.paxos[i].Min()
		if m != 0 {
			t.Fatalf("wrong Min() %v; expected 0", m)
		}
	}

	// everyone Done() -> Min() changes?
	for i := 0; i < npaxos; i++ {
		cfg.paxos[i].Done(0)
	}
	for i := 1; i < npaxos; i++ {
		cfg.paxos[i].Done(1)
	}
	for i := 0; i < npaxos; i++ {
		cfg.paxos[i].Start(8+i, "xx")
	}
	allok := false
	for iters := 0; iters < 12; iters++ {
		allok = true
		for i := 0; i < npaxos; i++ {
			s := cfg.paxos[i].Min()
			if s != 1 {
				allok = false
			}
		}
		if allok {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if allok != true {
		t.Fatalf("Min() did not advance after Done()")
	}

	fmt.Printf("  ... Passed\n")
}

func TestManyForget(t *testing.T) {
	const npaxos = 3
	cfg := make_config(t, npaxos, false)
	defer cfg.cleanup()

	fmt.Printf("Test: Lots of forgetting ...\n")

	const maxseq = 20

	go func() {
		na := rand.Perm(maxseq)
		for i := 0; i < len(na); i++ {
			seq := na[i]
			j := (rand.Int() % npaxos)
			v := rand.Int()
			cfg.paxos[j].Start(seq, v)
			runtime.Gosched()
		}
	}()

	done := make(chan bool)
	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			seq := (rand.Int() % maxseq)
			i := (rand.Int() % npaxos)
			if seq >= cfg.paxos[i].Min() {
				decided, _ := cfg.paxos[i].Status(seq)
				if decided == Decided {
					cfg.paxos[i].Done(seq)
				}
			}
			runtime.Gosched()
		}
	}()

	time.Sleep(5 * time.Second)
	done <- true

	cfg.setunreliable(true)

	time.Sleep(2 * time.Second)

	for seq := 0; seq < maxseq; seq++ {
		for i := 0; i < npaxos; i++ {
			if seq >= cfg.paxos[i].Min() {
				cfg.paxos[i].Status(seq)
			}
		}
	}

	fmt.Printf("  ... Passed\n")
}

//
// does paxos forgetting actually free the memory?
//
func TestForgetMem(t *testing.T) {
	const npaxos = 3
	cfg := make_config(t, npaxos, false)
	defer cfg.cleanup()

	cfg.paxos[0].Start(0, "x")
	cfg.waitn(0, npaxos)

	runtime.GC()
	var m0 runtime.MemStats
	runtime.ReadMemStats(&m0)
	// m0.Alloc about a megabyte

	for i := 1; i <= 10; i++ {
		big := make([]byte, 1000000)
		for j := 0; j < len(big); j++ {
			big[j] = byte('a' + rand.Int()%26)
		}
		cfg.paxos[0].Start(i, string(big))
		cfg.waitn(i, npaxos)
	}

	runtime.GC()
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	// m1.Alloc about 90 megabytes

	for i := 0; i < npaxos; i++ {
		cfg.paxos[i].Done(10)
	}
	for i := 0; i < npaxos; i++ {
		cfg.paxos[i].Start(11+i, "z")
	}
	time.Sleep(3 * time.Second)
	for i := 0; i < npaxos; i++ {
		if cfg.paxos[i].Min() != 11 {
			t.Fatalf("expected Min() %v, got %v\n", 11, cfg.paxos[i].Min())
		}
	}

	runtime.GC()
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)
	// m2.Alloc about 10 megabytes

	if m2.Alloc > (m1.Alloc / 2) {
		t.Fatalf("memory use did not shrink enough")
	}

	again := make([]string, 10)
	for seq := 0; seq < npaxos && seq < 10; seq++ {
		again[seq] = randstring(20)
		for i := 0; i < npaxos; i++ {
			fate, _ := cfg.paxos[i].Status(seq)
			if fate != Forgotten {
				t.Fatalf("seq %d < Min() %d but not Forgotten", seq, cfg.paxos[i].Min())
			}
			cfg.paxos[i].Start(seq, again[seq])
		}
	}
	time.Sleep(1 * time.Second)
	for seq := 0; seq < npaxos && seq < 10; seq++ {
		for i := 0; i < npaxos; i++ {
			fate, v := cfg.paxos[i].Status(seq)
			if fate != Forgotten || v == again[seq] {
				t.Fatalf("seq %d < Min() %d but not Forgotten", seq, cfg.paxos[i].Min())
			}
		}
	}

	fmt.Printf("  ... Passed\n")
}

//
// does Max() work after Done()s?
//
func TestDoneMax(t *testing.T) {
	const npaxos = 3
	cfg := make_config(t, npaxos, false)
	defer cfg.cleanup()

	cfg.paxos[0].Start(0, "x")
	cfg.waitn(0, npaxos)

	for i := 1; i <= 10; i++ {
		cfg.paxos[0].Start(i, "y")
		cfg.waitn(i, npaxos)
	}

	for i := 0; i < npaxos; i++ {
		cfg.paxos[i].Done(10)
	}

	// Propagate messages so everyone knows about Done(10)
	for i := 0; i < npaxos; i++ {
		cfg.paxos[i].Start(10, "z")
	}
	time.Sleep(2 * time.Second)
	for i := 0; i < npaxos; i++ {
		mx := cfg.paxos[i].Max()
		if mx != 10 {
			t.Fatalf("Max() did not return correct result %d after calling Done(); returned %d", 10, mx)
		}
	}

	fmt.Printf("  ... Passed\n")
}

func TestRPCCount(t *testing.T) {
	const npaxos = 3
	cfg := make_config(t, npaxos, false)
	defer cfg.cleanup()

	ninst1 := 5
	seq := 0
	for i := 0; i < ninst1; i++ {
		cfg.paxos[0].Start(seq, "x")
		cfg.waitn(seq, npaxos)
		seq++
	}

	time.Sleep(2 * time.Second)

	total1 := int32(cfg.rpcTotal())

	// per agreement:
	// 3 prepares
	// 3 accepts
	// 3 decides
	expected1 := int32(ninst1 * npaxos * npaxos)
	if total1 > expected1 {
		t.Fatalf("too many RPCs for serial Start()s; %v instances, got %v, expected %v",
			ninst1, total1, expected1)
	}

	ninst2 := 5
	for i := 0; i < ninst2; i++ {
		for j := 0; j < npaxos; j++ {
			go cfg.paxos[j].Start(seq, j+(i*10))
		}
		cfg.waitn(seq, npaxos)
		seq++
	}

	time.Sleep(2 * time.Second)

	total2 := int32(0)
	for j := 0; j < npaxos; j++ {
		total2 += atomic.LoadInt32(&cfg.paxos[j].rpcCount)
	}
	total2 -= total1

	// worst case per agreement:
	// Proposer 1: 3 prep, 3 acc, 3 decides.
	// Proposer 2: 3 prep, 3 acc, 3 prep, 3 acc, 3 decides.
	// Proposer 3: 3 prep, 3 acc, 3 prep, 3 acc, 3 prep, 3 acc, 3 decides.
	expected2 := int32(ninst2 * npaxos * 15)
	if total2 > expected2 {
		t.Fatalf("too many RPCs for concurrent Start()s; %v instances, got %v, expected %v",
			ninst2, total2, expected2)
	}

	fmt.Printf("  ... Passed\n")
}

//
// many agreements (without failures)
//
func TestMany(t *testing.T) {
	const npaxos = 3
	cfg := make_config(t, npaxos, false)
	defer cfg.cleanup()

	for i := 0; i < npaxos; i++ {
		cfg.paxos[i].Start(0, 0)
	}

	const ninst = 50
	for seq := 1; seq < ninst; seq++ {
		// only 5 active instances, to limit the
		// number of file descriptors.
		for seq >= 5 && cfg.ndecided(seq-5) < npaxos {
			time.Sleep(20 * time.Millisecond)
		}
		for i := 0; i < npaxos; i++ {
			cfg.paxos[i].Start(seq, (seq*10)+i)
		}
	}

	for {
		done := true
		for seq := 1; seq < ninst; seq++ {
			if cfg.ndecided(seq) < npaxos {
				done = false
			}
		}
		if done {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Printf("  ... Passed\n")
}

//
// a peer starts up, with proposal, after others decide.
// then another peer starts, without a proposal.
//
func TestOld(t *testing.T) {
	const npaxos = 5
	cfg := make_config(t, npaxos, false)
	defer cfg.cleanup()

	cfg.disconnect(0)
	cfg.disconnect(4)
	cfg.paxos[1].Start(1, 111)

	cfg.waitmajority(1)

	cfg.connect(0)
	cfg.paxos[0].Start(1, 222)

	cfg.waitn(1, 4)

	if false {
		cfg.connect(4)
		cfg.waitn(1, npaxos)
	}

	fmt.Printf("  ... Passed\n")
}

//
// many agreements, with unreliable RPC
//
func TestManyUnreliable(t *testing.T) {
	const npaxos = 5
	cfg := make_config(t, npaxos, false)
	defer cfg.cleanup()

	cfg.setunreliable(true)

	for i := 0; i < npaxos; i++ {
		cfg.paxos[i].Start(0, 0)
	}

	const ninst = 50
	for seq := 1; seq < ninst; seq++ {
		// only 3 active instances, to limit the
		// number of file descriptors.
		for seq >= 3 && cfg.ndecided(seq-3) < npaxos {
			time.Sleep(20 * time.Millisecond)
		}
		for i := 0; i < npaxos; i++ {
			cfg.paxos[i].Start(seq, (seq*10)+i)
		}
	}

	for {
		done := true
		for seq := 1; seq < ninst; seq++ {
			if cfg.ndecided(seq) < npaxos {
				done = false
			}
		}
		if done {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Printf("  ... Passed\n")
}

func TestPartition(t *testing.T) {
	const npaxos = 5
	cfg := make_config(t, npaxos, false)
	defer cfg.cleanup()

	seq := 0

	fmt.Printf("Test: No decision if partitioned ...\n")

	cfg.disconnect(1)
	cfg.disconnect(3)
	cfg.disconnect(4)

	cfg.paxos[1].Start(seq, 111)
	cfg.checkmax(seq, 0)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Decision in majority partition ...\n")

	cfg.disconnect(0)
	cfg.disconnect(4)
	cfg.connect(1)
	cfg.connect(3)

	time.Sleep(2 * time.Second)
	cfg.waitmajority(seq)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: All agree after full heal ...\n")

	cfg.paxos[0].Start(seq, 1000) // poke them
	cfg.paxos[4].Start(seq, 1004)

	cfg.connect(0)
	cfg.connect(4)

	cfg.waitn(seq, npaxos)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: One peer switches partitions ...\n")

	for iters := 0; iters < 20; iters++ {
		seq++

		cfg.connect(0)
		cfg.connect(1)
		cfg.disconnect(3)
		cfg.disconnect(4)
		cfg.paxos[0].Start(seq, seq*10)
		cfg.paxos[3].Start(seq, (seq*10)+1)
		cfg.waitmajority(seq)
		if cfg.ndecided(seq) > 3 {
			t.Fatalf("too many decided")
		}

		cfg.disconnect(0)
		cfg.disconnect(1)
		cfg.connect(3)
		cfg.connect(4)
		cfg.waitn(seq, npaxos)
	}

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: One peer switches partitions, unreliable ...\n")

	for iters := 0; iters < 20; iters++ {
		seq++

		cfg.setunreliable(true)

		cfg.connect(0)
		cfg.connect(1)
		cfg.disconnect(3)
		cfg.disconnect(4)
		for i := 0; i < npaxos; i++ {
			cfg.paxos[i].Start(seq, (seq*10)+i)
		}
		cfg.waitn(seq, 3)
		if cfg.ndecided(seq) > 3 {
			t.Fatalf("too many decided")
		}

		cfg.disconnect(0)
		cfg.disconnect(1)
		cfg.connect(3)
		cfg.connect(4)

		cfg.setunreliable(false)

		cfg.waitn(seq, 5)
	}

	fmt.Printf("  ... Passed\n")
}

func TestLots(t *testing.T) {
	const npaxos = 5
	cfg := make_config(t, npaxos, false)
	defer cfg.cleanup()

	fmt.Printf("Test: Many requests, changing partitions ...\n")

	cfg.setunreliable(true)

	done := int32(0)

	// re-partition periodically
	ch1 := make(chan bool)
	go func() {
		defer func() { ch1 <- true }()
		for atomic.LoadInt32(&done) == 0 {
			var a [npaxos]int
			for i := 0; i < npaxos; i++ {
				a[i] = (rand.Int() % 3)
			}
			pa := make([][]int, 3)
			for i := 0; i < 3; i++ {
				pa[i] = make([]int, 0)
				for j := 0; j < npaxos; j++ {
					if a[j] == i {
						pa[i] = append(pa[i], j)
					}
				}
			}

			for i := 0; i < npaxos; i++ {
				cfg.disconnect(i)
			}
			for _, p := range pa {
				if len(p) >= 3 {
					for _, v := range p {
						cfg.connect(v)
					}
				}
			}
			time.Sleep(time.Duration(rand.Int63()%200) * time.Millisecond)
		}
	}()

	seq := int32(0)

	// periodically start a new instance
	ch2 := make(chan bool)
	go func() {
		defer func() { ch2 <- true }()
		for atomic.LoadInt32(&done) == 0 {
			// how many instances are in progress?
			nd := 0
			sq := int(atomic.LoadInt32(&seq))
			for i := 0; i < sq; i++ {
				if cfg.ndecided(i) == npaxos {
					nd++
				}
			}
			if sq-nd < 10 {
				for i := 0; i < npaxos; i++ {
					cfg.paxos[i].Start(sq, rand.Int()%10)
				}
				atomic.AddInt32(&seq, 1)
			}
			time.Sleep(time.Duration(rand.Int63()%300) * time.Millisecond)
		}
	}()

	// periodically check that decisions are consistent
	ch3 := make(chan bool)
	go func() {
		defer func() { ch3 <- true }()
		for atomic.LoadInt32(&done) == 0 {
			for i := 0; i < int(atomic.LoadInt32(&seq)); i++ {
				cfg.ndecided(i)
			}
			time.Sleep(time.Duration(rand.Int63()%300) * time.Millisecond)
		}
	}()

	time.Sleep(20 * time.Second)
	atomic.StoreInt32(&done, 1)
	<-ch1
	<-ch2
	<-ch3

	// repair, then check that all instances decided.
	cfg.setunreliable(false)
	for i := 0; i < npaxos; i++ {
		cfg.connect(i)
	}
	time.Sleep(5 * time.Second)

	for i := 0; i < int(atomic.LoadInt32(&seq)); i++ {
		cfg.waitmajority(i)
	}

	fmt.Printf("  ... Passed\n")
}
