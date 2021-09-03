package kvpaxos

import (
	"6.824/labrpc"
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"math/big"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"
)

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func makeSeed() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

type config struct {
	mu           sync.Mutex
	t            *testing.T
	net          *labrpc.Network
	n            int
	kvservers    []*KVPaxos
	endnames     [][]string // names of each server's sending ClientEnds
	clerks       map[*Clerk][]string
	nextClientId int
	maxraftstate int
	start        time.Time // time at which make_config() was called
	// begin()/end() statistics
	t0    time.Time // time at which test_test.go called cfg.begin()
	rpcs0 int       // rpcTotal() at start of test
	ops   int32     // number of clerk get/put/append method calls
}

func (cfg *config) ConnectAll() {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	for i := 0; i < cfg.n; i++ {
		cfg.connectUnlocked(i, cfg.All())
	}
}

func (cfg *config) All() []int {
	all := make([]int, cfg.n)
	for i := 0; i < cfg.n; i++ {
		all[i] = i
	}
	return all
}

// Sets up 2 partitions with connectivity between servers in each  partition.
func (cfg *config) partition(p1 []int, p2 []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	// log.Printf("partition servers into: %v %v\n", p1, p2)
	for i := 0; i < len(p1); i++ {
		cfg.disconnectUnlocked(p1[i], p2)
		cfg.connectUnlocked(p1[i], p1)
	}
	for i := 0; i < len(p2); i++ {
		cfg.disconnectUnlocked(p2[i], p1)
		cfg.connectUnlocked(p2[i], p2)
	}
}

// attach server i to servers listed in to
// caller must hold cfg.mu
func (cfg *config) connectUnlocked(i int, to []int) {
	// log.Printf("connect peer %d to %v\n", i, to)

	// outgoing socket files
	for j := 0; j < len(to); j++ {
		endname := cfg.endnames[i][to[j]]
		cfg.net.Enable(endname, true)
	}

	// incoming socket files
	for j := 0; j < len(to); j++ {
		endname := cfg.endnames[to[j]][i]
		cfg.net.Enable(endname, true)
	}
}

func (cfg *config) connect(i int, to []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.connectUnlocked(i, to)
}

// detach server i from the servers listed in from
// caller must hold cfg.mu
func (cfg *config) disconnectUnlocked(i int, from []int) {
	// log.Printf("disconnect peer %d from %v\n", i, from)

	// outgoing socket files
	for j := 0; j < len(from); j++ {
		if cfg.endnames[i] != nil {
			endname := cfg.endnames[i][from[j]]
			cfg.net.Enable(endname, false)
		}
	}

	// incoming socket files
	for j := 0; j < len(from); j++ {
		if cfg.endnames[j] != nil {
			endname := cfg.endnames[from[j]][i]
			cfg.net.Enable(endname, false)
		}
	}
}

func (cfg *config) disconnect(i int, from []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.disconnectUnlocked(i, from)
}

func (cfg *config) checkTimeout() {
	// enforce a two minute real-time limit on each test
	if !cfg.t.Failed() && time.Since(cfg.start) > 120*time.Second {
		cfg.t.Fatal("test took longer than 120 seconds")
	}
}

func (cfg *config) cleanup() {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	for i := 0; i < len(cfg.kvservers); i++ {
		if cfg.kvservers[i] != nil {
			cfg.kvservers[i].kill()
		}
	}
	cfg.net.Cleanup()
	cfg.checkTimeout()
}

// caller should hold cfg.mu
func (cfg *config) DisconnectClientUnlocked(ck *Clerk, from []int) {
	// log.Printf("DisconnectClient %v from %v\n", ck, from)
	endnames := cfg.clerks[ck]
	for j := 0; j < len(from); j++ {
		s := endnames[from[j]]
		cfg.net.Enable(s, false)
	}
}

// If restart servers, first call ShutdownServer
func (cfg *config) StartServer(i int) {
	cfg.mu.Lock()

	// a fresh set of outgoing ClientEnd names.
	cfg.endnames[i] = make([]string, cfg.n)
	for j := 0; j < cfg.n; j++ {
		cfg.endnames[i][j] = randstring(20)
	}

	// a fresh set of ClientEnds.
	ends := make([]*labrpc.ClientEnd, cfg.n)
	for j := 0; j < cfg.n; j++ {
		ends[j] = cfg.net.MakeEnd(cfg.endnames[i][j])
		cfg.net.Connect(cfg.endnames[i][j], j)
	}
	cfg.mu.Unlock()

	cfg.kvservers[i] = StartServer(ends, i)

	kvsvc := labrpc.MakeService(cfg.kvservers[i])
	rfsvc := labrpc.MakeService(cfg.kvservers[i].px)
	srv := labrpc.MakeServer()
	srv.AddService(kvsvc)
	srv.AddService(rfsvc)
	cfg.net.AddServer(i, srv)
}

// Create a clerk with clerk specific server names.
// Give it connections to all of the servers, but for
// now enable only connections to servers in to[].
func (cfg *config) makeClient(to []int) *Clerk {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	// a fresh set of ClientEnds.
	ends := make([]*labrpc.ClientEnd, cfg.n)
	endnames := make([]string, cfg.n)
	for j := 0; j < cfg.n; j++ {
		endnames[j] = randstring(20)
		ends[j] = cfg.net.MakeEnd(endnames[j])
		cfg.net.Connect(endnames[j], j)
	}

	ck := MakeClerk(random_handles(ends))
	cfg.clerks[ck] = endnames
	cfg.nextClientId++
	cfg.ConnectClientUnlocked(ck, to)
	return ck
}

// caller should hold cfg.mu
func (cfg *config) ConnectClientUnlocked(ck *Clerk, to []int) {
	// log.Printf("ConnectClient %v to %v\n", ck, to)
	endnames := cfg.clerks[ck]
	for j := 0; j < len(to); j++ {
		s := endnames[to[j]]
		cfg.net.Enable(s, true)
	}
}

func (cfg *config) ConnectClient(ck *Clerk, to []int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	cfg.ConnectClientUnlocked(ck, to)
}

// Randomize server handles
func random_handles(kvh []*labrpc.ClientEnd) []*labrpc.ClientEnd {
	sa := make([]*labrpc.ClientEnd, len(kvh))
	copy(sa, kvh)
	for i := range sa {
		j := rand.Intn(i + 1)
		sa[i], sa[j] = sa[j], sa[i]
	}
	return sa
}

var ncpu_once sync.Once

func make_config(t *testing.T, n int, unreliable bool) *config {
	ncpu_once.Do(func() {
		if runtime.NumCPU() < 2 {
			fmt.Printf("warning: only one CPU, which may conceal locking bugs\n")
		}
		rand.Seed(makeSeed())
	})
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.t = t
	cfg.net = labrpc.MakeNetwork()
	cfg.n = n
	cfg.kvservers = make([]*KVPaxos, cfg.n)
	cfg.endnames = make([][]string, cfg.n)
	cfg.clerks = make(map[*Clerk][]string)
	cfg.nextClientId = cfg.n + 1000 // client ids start 1000 above the highest serverid
	cfg.start = time.Now()

	// create a full set of KV servers.
	for i := 0; i < cfg.n; i++ {
		cfg.StartServer(i)
	}

	cfg.ConnectAll()

	cfg.net.Reliable(!unreliable)

	return cfg
}

func (cfg *config) rpcTotal() int {
	return cfg.net.GetTotalCount()
}
