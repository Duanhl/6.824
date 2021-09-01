package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"net"
	"time"
)
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"

// Fate px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
	NotReady       // not ready for propose
)

type Phase int

const (
	Prepare Phase = iota + 1
	Accept
	Done

	Ready //for trigger a round instance
)

const None = -1

const (
	ProcessBase   int64 = 200
	NextRoundBase int64 = 500
	RetryBase     int64 = 100
)

func randomTimeout(base int64) time.Duration {
	return time.Duration(base+rand.Int63n(base)) * time.Millisecond
}

type Instances struct {
	ents    []Instance
	prevSeq int
}

func (i *Instances) LastSeq() int {
	l := len(i.ents)
	if l == 0 {
		return i.prevSeq
	} else {
		return i.ents[l-1].seq
	}
}

type Instance struct {
	seq  int
	fate Fate
	val  interface{}

	ps ProposerState
	as AcceptorState
}

type ProposerState struct {
	choose    Pid
	candidate interface{}
	phase     Phase
	maxAccPid Pid
	maxAccVal interface{}
	vote      int
	reject    int
}

type AcceptorState struct {
	promise Pid
	accPid  Pid
	accVal  interface{}
}

type Timeout struct {
	seq   int
	pid   Pid
	phase Phase
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	instances Instances

	quorum int
	keep   []int // for Done(), Min()

	// for ever seen maxPid, if px choose a new pid, update maxSeenPid = pid
	// if px receive a reject pid bigger than maxSeenPid, update maxSeenPid.Suf = rejectPid.Suf
	maxSeenPid Pid

	recv   chan Message
	outc   chan Message
	retryc chan Message
	rpcm   map[int64]Context
	timec  chan Timeout
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			//fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

type SeqValue struct {
	Seq int
	Val interface{}
}

// Start
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	ctx := px.register(Message{
		Type: StartProposer,
		Content: SeqValue{
			Seq: seq,
			Val: v,
		},
	})
	ctx.Done()
}

// Done
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	ctx := px.register(Message{
		Type:    DoneSnap,
		Content: seq,
	})
	ctx.Done()
}

// Max
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	ctx := px.register(Message{
		Type: MaxQuery,
	})
	msg := ctx.Done()
	return msg.(int)
}

// Min
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	ctx := px.register(Message{
		Type: MinQuery,
	})
	msg := ctx.Done()
	return msg.(int)
}

type InsStatus struct {
	fate Fate
	val  interface{}
}

// Status
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	ctx := px.register(Message{
		Type:    Status,
		Content: seq,
	})
	msg := ctx.Done().(InsStatus)
	return msg.fate, msg.val
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	ctx := px.register(Message{
		Type: PrepareReq,
		Content: PrepareArgs{
			Seq:      args.Seq,
			Pid:      args.Pid,
			Done:     args.Done,
			Proposer: args.Proposer,
		},
	})
	msg := ctx.Done().(PrepareReply)

	reply.Agree = msg.Agree
	reply.AcceptedVal = msg.AcceptedVal
	reply.AcceptedPid = msg.AcceptedPid

	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	ctx := px.register(Message{
		Type: AcceptReq,
		Content: AcceptArgs{
			Seq: args.Seq,
			Pid: args.Pid,
			Val: args.Val,
		},
	})
	msg := ctx.Done().(AcceptReply)

	reply.Agree = msg.Agree
	reply.AcceptedPid = msg.AcceptedPid

	return nil
}

func (px *Paxos) Learn(args *LearnArgs, reply *LearnReply) error {
	ctx := px.register(Message{
		Type: LearnReq,
		Content: LearnArgs{
			Seq: args.Seq,
			Val: args.Val,
		},
	})
	ctx.Done()
	return nil
}

// Kill
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

// Make
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.instances = Instances{
		ents:    []Instance{},
		prevSeq: -1,
	}

	px.quorum = len(peers)/2 + 1
	px.keep = make([]int, len(peers))

	for i := range px.keep {
		px.keep[i] = -1
	}

	px.maxSeenPid = Pid{
		Pre: me,
		Suf: 0,
	}

	px.recv = make(chan Message, 128)
	px.outc = make(chan Message, 128)
	px.retryc = make(chan Message, 128)
	px.rpcm = map[int64]Context{}
	px.timec = make(chan Timeout)

	go px.run()
	go px.out()

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}

func (px *Paxos) run() {
	for px.isdead() == false {
		select {
		case msg := <-px.retryc: // for retry
			switch msg.Type {
			case PrepareReq:
				m := msg.Content.(PrepareArgs)
				ins := px.instanceAt(m.Seq)
				if m.Pid.Equals(ins.ps.choose) && ins.ps.phase == Prepare {
					px.sendMessage(msg)
				}
				break
			case AcceptReq:
				m := msg.Content.(AcceptArgs)
				ins := px.instanceAt(m.Seq)
				if m.Pid.Equals(ins.ps.choose) && ins.ps.phase == Accept {
					px.sendMessage(msg)
				}
				break
			default:
			}
		case msg := <-px.recv: // for process
			switch msg.Type {
			// process msg
			case StartProposer:
				m := msg.Content.(SeqValue)
				px.handleStart(msg.Id, m.Seq, m.Val)
				break
			case Status:
				m := msg.Content.(int)
				px.handleStatus(msg.Id, m)
				break
			case DoneSnap:
				m := msg.Content.(int)
				px.handleDoneSnap(msg.Id, m)
				break
			case MinQuery:
				px.handleMinQuery(msg.Id)
				break
			case MaxQuery:
				px.handleMaxQuery(msg.Id)
				break

			case LearnReq:
				m := msg.Content.(LearnArgs)
				px.handleLearn(msg.Id, m)
				break

			case AcceptReq:
				m := msg.Content.(AcceptArgs)
				px.handleAccept(msg.Id, m, msg.IsForMySelf)
				break
			case AcceptRes:
				m := msg.Content.(AcceptReply)
				px.handleAcceptRes(m)
				break
			case PrepareReq:
				m := msg.Content.(PrepareArgs)
				px.handlePrepare(msg.Id, m, msg.IsForMySelf)
				break
			case PrepareRes:
				m := msg.Content.(PrepareReply)
				px.handlePrepareRes(m)
				break

			default:
				break
			}
		case t := <-px.timec:
			px.handleTimeout(t)
		}
	}
}

func (px *Paxos) out() {
	for px.isdead() == false {
		select {
		case msg := <-px.outc:
			if msg.IsForMySelf {
				px.recv <- msg
			} else {
				if msg.Type == PrepareReq || msg.Type == AcceptReq || msg.Type == LearnReq {
					go px.remote(msg)
				} else {
					px.mu.Lock()
					ctx := px.rpcm[msg.Id]
					ctx.resc <- msg.Content
					delete(px.rpcm, msg.Id)
					px.mu.Unlock()
				}
			}
		}
	}
}

func (px *Paxos) sendMessage(msg Message) {
	if px.isdead() == false {
		px.outc <- msg
	}
}

func (px *Paxos) retry(msg Message) {
	time.AfterFunc(randomTimeout(RetryBase), func() {
		px.retryc <- msg
	})
}

func (px *Paxos) remote(msg Message) {
	switch msg.Type {
	case PrepareReq:
		c := msg.Content.(PrepareArgs)
		args := &PrepareArgs{
			Seq:      c.Seq,
			Pid:      c.Pid,
			Done:     c.Done,
			Proposer: px.me,
		}
		reply := &PrepareReply{}
		if call(px.peers[msg.To], "Paxos.Prepare", args, reply) {
			px.recv <- Message{
				Type: PrepareRes,
				Content: PrepareReply{
					Agree:       reply.Agree,
					AcceptedVal: reply.AcceptedVal,
					AcceptedPid: reply.AcceptedPid,
					Seq:         c.Seq,
					Pid:         c.Pid,
				},
			}
		} else {
			px.retry(msg)
		}
		break
	case AcceptReq:
		c := msg.Content.(AcceptArgs)
		args := &AcceptArgs{
			Seq: c.Seq,
			Pid: c.Pid,
			Val: c.Val,
		}

		reply := &AcceptReply{}
		if call(px.peers[msg.To], "Paxos.Accept", args, reply) {
			px.recv <- Message{
				Type: AcceptRes,
				Content: AcceptReply{
					Agree: reply.Agree,
					Seq:   c.Seq,
					Pid:   c.Pid,
				},
			}

		} else {
			px.retry(msg)
		}
		break

	case LearnReq:
		c := msg.Content.(LearnArgs)
		args := &LearnArgs{
			Seq: c.Seq,
			Val: c.Val,
		}
		reply := &LearnReply{}
		if !call(px.peers[msg.To], "Paxos.Learn", args, reply) {
			px.retry(msg)
		}
	default:
	}
}

func (px *Paxos) register(msg Message) Context {
	px.mu.Lock()
	defer px.mu.Unlock()

	ctx := Context{
		resc: make(chan interface{}),
	}
	id := rand.Int63()
	msg.Id = id
	px.rpcm[id] = ctx
	px.recv <- msg

	return ctx
}

func (px *Paxos) handleStart(id int64, seq int, val interface{}) {
	ins := px.instanceAt(seq)
	if ins.fate == NotReady {
		ins.ps.candidate = val
		ins.fate = Pending
		px.updateInstance(ins)

		px.startPrepare(seq)
	}
	px.sendMessage(Message{
		Type: StartProposer,
		Id:   id,
	})
}

func (px *Paxos) handleStatus(id int64, seq int) {
	ins := px.instanceAt(seq)

	px.sendMessage(Message{
		Type: Status,
		Id:   id,
		Content: InsStatus{
			fate: ins.fate,
			val:  ins.val,
		},
	})
}

func (px *Paxos) handleDoneSnap(id int64, seq int) {
	if px.keep[px.me] < seq {
		px.keep[px.me] = seq
	}
	px.sendMessage(Message{
		Type: DoneSnap,
		Id:   id,
	})
}

func (px *Paxos) handleMinQuery(id int64) {
	px.sendMessage(Message{
		Type:    MinQuery,
		Id:      id,
		Content: Min(px.keep) + 1,
	})
}

func (px *Paxos) handleMaxQuery(id int64) {
	lastSeq := px.instances.LastSeq()
	px.sendMessage(Message{
		Type:    MaxQuery,
		Id:      id,
		Content: lastSeq,
	})
}

// for proposer process timeout
func (px *Paxos) handleTimeout(t Timeout) {

	ins := px.instanceAt(t.seq)
	if t.phase == Ready {
		if ins.fate == NotReady {
			px.startPrepare(t.seq)
		}
		return
	}

	if t.pid.Equals(ins.ps.choose) {
		if (ins.ps.phase == Prepare && t.phase == Prepare) ||
			(ins.ps.phase == Accept && t.phase == Accept) {
			px.startPrepare(t.seq)
		}
	}
}

func (px *Paxos) startPrepare(seq int) {
	ins := px.instanceAt(seq)
	choose := px.maxSeenPid.Next() //select new epoch
	px.maxSeenPid = choose

	ins.ps.choose = choose
	ins.ps.vote = 0
	ins.ps.reject = 0
	ins.ps.maxAccPid = ZeroPid()
	ins.ps.maxAccVal = nil
	ins.ps.phase = Prepare

	px.updateInstance(ins)

	DPrintf("server %v start prepare: [%v, %s]", px.me, seq, choose.String())
	for i := 0; i < len(px.peers); i++ {
		px.outc <- Message{
			Type:        PrepareReq,
			To:          i,
			IsForMySelf: px.me == i,
			Content: PrepareArgs{
				Seq:      seq,
				Pid:      choose,
				Done:     px.keep[px.me],
				Proposer: px.me,
			},
		}
	}

	// set timeout
	time.AfterFunc(randomTimeout(ProcessBase), func() {
		px.timec <- Timeout{
			seq:   seq,
			pid:   choose,
			phase: Prepare,
		}
	})
}

func (px *Paxos) startAccept(seq int) {
	ins := px.instanceAt(seq)

	DPrintf("server %v start accept: [%v, %s]", px.me, seq, ins.ps.choose.String())
	for i := 0; i < len(px.peers); i++ {
		px.outc <- Message{
			Type:        AcceptReq,
			To:          i,
			IsForMySelf: px.me == i,
			Content: AcceptArgs{
				Seq: ins.seq,
				Pid: ins.ps.choose,
				Val: ins.ps.candidate,
			},
		}
	}

	// set timeout
	time.AfterFunc(randomTimeout(ProcessBase), func() {
		px.timec <- Timeout{
			seq:   seq,
			pid:   ins.ps.choose,
			phase: Accept,
		}
	})
}

func (px *Paxos) startLearn(seq int, val interface{}) {
	for i := 0; i < len(px.peers); i++ {
		if i != px.me {
			px.outc <- Message{
				Type: LearnReq,
				To:   i,
				Content: LearnArgs{
					Seq: seq,
					Val: val,
				},
			}
		}
	}
}

func (px *Paxos) updateMaxSeenPid(pid Pid) {
	if pid.After(px.maxSeenPid) {
		px.maxSeenPid = Pid{
			Pre: px.me,
			Suf: pid.Suf,
		}
	}
}

func (px *Paxos) handleLearn(id int64, args LearnArgs) {
	if args.Seq <= px.instances.prevSeq {
		px.sendMessage(Message{
			Type: LearnRes,
			Id:   id,
		})
		return
	}
	ins := px.instanceAt(args.Seq)
	ins.fate = Decided
	ins.val = args.Val
	ins.ps.phase = Done
	px.updateInstance(ins)

	DPrintf("server %v learn: [%v, %v]", px.me, args.Seq, args.Val)
	px.sendMessage(Message{
		Type: LearnRes,
		Id:   id,
	})
}

func (px *Paxos) handlePrepare(id int64, args PrepareArgs, isForMySelf bool) {
	// update peers keep
	px.checkAndForgotten(args.Proposer, args.Done)

	// instance always decided and forgotten
	if args.Seq <= px.instances.prevSeq {
		px.sendMessage(Message{
			Type:        PrepareRes,
			Id:          id,
			IsForMySelf: isForMySelf,
			Content: PrepareReply{
				Agree:       false,
				AcceptedVal: nil,
				AcceptedPid: ZeroPid(),
				Seq:         args.Seq,
				Pid:         args.Pid,
			},
		})
		return
	}

	ins := px.instanceAt(args.Seq)
	reply := PrepareReply{}

	px.updateMaxSeenPid(args.Pid)

	if args.Pid.Equals(ins.as.promise) || args.Pid.After(ins.as.promise) {
		ins.as.promise = args.Pid
		px.updateInstance(ins)

		reply.Agree = true
		reply.AcceptedVal = ins.as.accVal
		reply.AcceptedPid = ins.as.accPid

		reply.Seq = args.Seq
		reply.Pid = args.Pid

		DWarn("server %v promised for [%v, %s]", px.me, args.Seq, args.Pid.String())
	} else {
		// if reject, give promised
		reply.Agree = false
		reply.AcceptedVal = nil
		reply.AcceptedPid = ins.as.accPid

		reply.Seq = args.Seq
		reply.Pid = args.Pid
	}
	res := Message{
		Type:        PrepareRes,
		Id:          id,
		IsForMySelf: isForMySelf,
		Content:     reply,
	}
	px.sendMessage(res)
}

func (px *Paxos) checkAndForgotten(proposer int, done int) {
	if px.keep[proposer] < done {
		px.keep[proposer] = done
		minKeep := Min(px.keep)
		px.drop(minKeep)
	}
}

func (px *Paxos) handlePrepareRes(reply PrepareReply) {
	ins := px.instanceAt(reply.Seq)
	DWarn("server %v received prepareReply: %s", px.me, reply.String())

	if reply.Pid.Equals(ins.ps.choose) && ins.ps.phase == Prepare {
		if reply.Agree {
			ins.ps.vote += 1
			if !reply.AcceptedPid.EqualsZero() && reply.AcceptedPid.After(ins.ps.maxAccPid) {
				ins.ps.maxAccPid = reply.AcceptedPid
				ins.ps.maxAccVal = reply.AcceptedVal
			}
			px.updateInstance(ins)

			if ins.ps.vote == px.quorum {
				if ins.ps.maxAccVal != nil {
					ins.ps.candidate = ins.ps.maxAccVal
				}
				ins.ps.phase = Accept
				ins.ps.vote = 0
				ins.ps.reject = 0

				px.updateInstance(ins)
				px.startAccept(reply.Seq)
			}
		} else {
			ins.ps.reject += 1
			px.updateInstance(ins)

			// update maxSeenPid
			px.updateMaxSeenPid(reply.AcceptedPid)

			if ins.ps.reject == px.quorum {
				px.startPrepare(reply.Seq)
			}
		}
	}
}

func (px *Paxos) handleAccept(id int64, args AcceptArgs, isForMySelf bool) {
	if args.Seq <= px.instances.prevSeq {
		px.sendMessage(Message{
			Type:        AcceptRes,
			Id:          id,
			IsForMySelf: isForMySelf,
			Content: AcceptReply{
				Agree:       false,
				AcceptedPid: ZeroPid(),
				Seq:         args.Seq,
				Pid:         args.Pid,
			},
		})
		return
	}

	ins := px.instanceAt(args.Seq)

	px.updateMaxSeenPid(args.Pid)

	reply := AcceptReply{}
	if args.Pid.After(ins.as.promise) || args.Pid.Equals(ins.as.promise) {
		ins.as.promise = args.Pid
		ins.as.accPid = args.Pid
		ins.as.accVal = args.Val
		px.updateInstance(ins)

		reply.Agree = true
		reply.AcceptedPid = ins.as.promise

		reply.Seq = args.Seq
		reply.Pid = args.Pid
		DWarn("server %v accepted: [%v, %s]", px.me, args.Seq, args.Pid.String())
	} else {
		reply.Agree = false
		reply.AcceptedPid = ins.as.promise

		reply.Seq = args.Seq
		reply.Pid = args.Pid
	}
	res := Message{
		Type:        AcceptRes,
		Id:          id,
		IsForMySelf: isForMySelf,
		Content:     reply,
	}
	px.sendMessage(res)
	time.AfterFunc(randomTimeout(NextRoundBase), func() {
		px.timec <- Timeout{
			seq:   args.Seq,
			phase: Ready,
		}
	})
}

func (px *Paxos) handleAcceptRes(reply AcceptReply) {
	DWarn("server %v received acceptReply: %s", px.me, reply.String())

	ins := px.instanceAt(reply.Seq)

	if reply.Pid.Equals(ins.ps.choose) && ins.ps.phase == Accept {
		if reply.Agree {
			ins.ps.vote += 1

			if ins.ps.vote == px.quorum {
				ins.fate = Decided
				ins.val = ins.ps.candidate
				ins.ps.phase = Done

				DPrintf("server %v decided, propose:[%v, %v]\n", px.me, ins.seq, ins.val)

				px.startLearn(ins.seq, ins.val)
			}

			px.updateInstance(ins)
		} else {
			ins.ps.reject += 1
			px.updateInstance(ins)

			// update maxSeenPid
			px.updateMaxSeenPid(reply.AcceptedPid)

			if ins.ps.reject == px.quorum {
				px.startPrepare(ins.seq)
			}
		}
	}
}

// found instance at seq, if seq smaller than first seq, return forgotten instance
// if seq bigger than last seq, create instance and full it util seq
//  |dummy instance| ---> |normal instances|
//
func (px *Paxos) instanceAt(seq int) Instance {
	if seq <= px.instances.prevSeq {
		return Instance{
			seq:  seq,
			fate: Forgotten,
		}
	}
	var maxSeq int
	l := len(px.instances.ents)
	if l == 0 {
		maxSeq = px.instances.prevSeq
	} else {
		maxSeq = px.instances.ents[l-1].seq
	}
	if seq > maxSeq {
		for i := maxSeq + 1; i <= seq; i++ {
			px.instances.ents = append(px.instances.ents, Instance{
				seq:  i,
				fate: NotReady,
				as: AcceptorState{
					promise: ZeroPid(),
					accPid:  ZeroPid(),
					accVal:  nil,
				},
			})
		}
	}
	return px.instances.ents[seq-px.instances.prevSeq-1]
}

// call after instanceAt, always in instances
func (px *Paxos) updateInstance(ins Instance) {
	px.instances.ents[ins.seq-px.instances.prevSeq-1] = ins
}

// drop instance where ents.seq <= seq
func (px *Paxos) drop(seq int) {
	if seq <= px.instances.prevSeq {
		return
	}
	// use sliced slice can't free memory, copy a new slice instead
	ents := px.instances.ents[seq-px.instances.prevSeq:]
	px.instances.ents = []Instance{}
	for _, ins := range ents {
		px.instances.ents = append(px.instances.ents, ins)
	}
	ents = nil
	px.instances.prevSeq = seq
}
