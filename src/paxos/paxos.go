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
	"errors"
	"net"
	"strconv"
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
)

type Phase int

const (
	Prepare Phase = iota + 1
	Accept
)

const None = -1
const DefaultTimeout = 200 * time.Millisecond

type Instance struct {
	Seq  int
	Fate Fate
	Val  interface{}

	// for acceptor
	MinPid      Pid
	AcceptedPid Pid
	AcceptedVal interface{}

	// for proposer
	Choose Pid
	Qp     map[int]Propose
	Qa     map[int]bool
}

type Propose struct {
	AcceptedPid Pid
	AcceptedVal interface{}
}

type Timeout struct {
	Seq   int
	Pid   Pid
	Phase Phase
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
	ins []Instance

	quorum int
	keep   []int // for Done(), Min()

	seq *Sequence
	ser *Serializer

	recv  chan Message
	outc  chan Message
	rpcm  map[uint64]Context
	timec chan Timeout
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
			fmt.Printf("paxos Dial() failed: %v\n", err1)
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
		From: None,
		Seq:  seq,
		Val:  v,
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
		Type: DoneSnap,
		From: None,
		Seq:  seq,
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
		From: None,
	})
	msg := ctx.Done()
	return msg.Seq
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
		From: None,
	})
	msg := ctx.Done()
	return msg.Seq
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
		Type: Status,
		From: None,
		Seq:  seq,
	})
	msg := ctx.Done()
	return Fate(msg.Fate), msg.Val
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	ctx := px.register(Message{
		Type: PrepareReq,
		Seq:  args.Seq,
		Pid:  args.Pid,
	})
	msg := ctx.Done()

	reply.Agree = msg.Agree
	reply.AcceptedVal = msg.Val
	reply.AcceptedPid = msg.Pid

	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	ctx := px.register(Message{
		Type: AcceptReq,
		Seq:  args.Seq,
		Pid:  args.Pid,
		Val:  args.Val,
	})
	msg := ctx.Done()

	reply.Agree = msg.Agree

	return nil
}

func (px *Paxos) DoneBroadcast(args *DoneBroadcastArgs, reply *DoneBroadcastReply) error {
	ctx := px.register(Message{
		Type: DoneBroadcast,
		From: args.From,
		Seq:  args.Seq,
	})
	ctx.Done()
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
	px.ins = []Instance{}

	px.quorum = len(peers)/2 + 1
	px.keep = make([]int, len(peers))

	px.seq = &Sequence{
		Server: me,
	}
	px.ser = &Serializer{}

	px.recv = make(chan Message, 128)
	px.outc = make(chan Message, 128)
	px.rpcm = map[uint64]Context{}
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
		case msg := <-px.recv:
			if msg.From == px.me && msg.To != px.me {
				if ins, err := px.instanceAt(msg.Seq); err == nil {
					// for prepare retry
					if msg.Type == PrepareReq && msg.Pid.Equals(ins.Choose) && len(ins.Qp) < px.quorum {
						px.sendMessage(msg)
					}

					// for accept retry
					if msg.Type == AcceptReq && msg.Pid.Equals(ins.Choose) && len(ins.Qa) < px.quorum {
						px.sendMessage(msg)
					}
				}
			} else {
				switch msg.Type {
				// process msg
				case StartProposer:
					px.handleStart(msg)
					break
				case Status:
					px.handleStatus(msg)
					break
				case DoneSnap:
					px.handleDoneSnap(msg)
					break
				case MinQuery:
					px.handleMinQuery(msg)
					break
				case MaxQuery:
					px.handleMaxQuery(msg)
					break

				case AcceptReq:
					px.handleAccept(msg)
					break
				case AcceptRes:
					px.handleAcceptRes(msg)
					break
				case PrepareReq:
					px.handlePrepare(msg)
					break
				case PrepareRes:
					px.handlePrepareRes(msg)
					break

				case DoneBroadcast:
					px.handleDoneBroadcast(msg)

				default:
					DPrintf("unsupported msg type: %s", msg.Type)
				}

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
			if msg.From == px.me && msg.To == px.me { // me send to myself
				px.recv <- msg
			} else if msg.To == None || // response client function call
				msg.Type == PrepareRes || msg.Type == AcceptRes { // for rpc call response
				px.mu.Lock()

				if ctx, ok := px.rpcm[msg.Id]; ok {
					ctx.resc <- msg
					delete(px.rpcm, msg.Id)
				} else {
					DPrintf("Error, ctx already be response, msg: %s", msg)
				}

				px.mu.Unlock()

			} else { // for rpc call to other peer
				go px.remote(msg)
			}
		}
	}
}

func (px *Paxos) sendMessage(msg Message) {
	if px.isdead() == false {
		px.outc <- msg
	}
}

func (px *Paxos) remote(msg Message) {
	switch msg.Type {
	case PrepareReq:
		args := &PrepareArgs{
			Seq: msg.Seq,
			Pid: msg.Pid,
		}
		reply := &PrepareReply{}
		if call(px.peers[msg.To], "Paxos.Prepare", args, reply) {
			px.recv <- Message{
				Type:        PrepareRes,
				From:        msg.To,
				To:          px.me,
				Seq:         args.Seq,
				Pid:         args.Pid,
				Agree:       reply.Agree,
				AcceptedPid: reply.AcceptedPid,
				Val:         reply.AcceptedVal,
			}

		} else {
			px.recv <- msg // for retry
		}

	case AcceptReq:
		args := &AcceptArgs{
			Seq: msg.Seq,
			Pid: msg.Pid,
			Val: msg.Val,
		}

		reply := &AcceptReply{}
		if call(px.peers[msg.To], "Paxos.Accept", args, reply) {
			px.recv <- Message{
				Type:  PrepareRes,
				From:  msg.To,
				To:    px.me,
				Seq:   msg.Seq,
				Pid:   msg.Pid,
				Agree: reply.Agree,
			}

		} else {
			px.recv <- msg // for retry
		}

	case DoneBroadcast:
		args := &DoneBroadcastArgs{
			From: msg.From,
			Seq:  msg.Seq,
		}
		reply := &DoneBroadcastReply{}
		call(px.peers[msg.To], "Paxos.DoneBroadcast", args, reply)
	}
}

func (px *Paxos) register(msg Message) Context {
	px.mu.Lock()
	defer px.mu.Unlock()

	ctx := Context{
		resc: make(chan Message),
	}
	id := px.ser.NextId()
	msg.Id = id
	px.rpcm[id] = ctx
	px.recv <- msg

	return ctx
}

func (px *Paxos) handleStart(msg Message) {
	if ins, err := px.instanceAt(msg.Seq); err == nil {
		pid := px.seq.Next()

		ins.Choose = pid
		ins.Qp = map[int]Propose{}
		ins.Qa = map[int]bool{}

		px.updateInstance(ins)

		px.startPrepare(msg.Seq, pid)
	} else {
		px.sendMessage(Message{
			Id: msg.Id,
			To: None,
		})
	}
}

func (px *Paxos) handleStatus(msg Message) {
	if ins, err := px.instanceAt(msg.Seq); err == nil {
		var val interface{}
		if ins.Fate == Decided {
			val = ins.Val
		}
		px.sendMessage(Message{
			Type: Status,
			Id:   msg.Id,
			To:   None,
			Fate: int(ins.Fate),
			Val:  val,
		})
	} else {
		px.sendMessage(Message{
			Type: Status,
			Id:   msg.Id,
			To:   None,
			Fate: int(Forgotten),
			Val:  nil,
		})
	}
}

func (px *Paxos) handleDoneSnap(msg Message) {
	firstSeq := px.ins[0].Seq
	if msg.Seq >= firstSeq {
		px.ins = px.ins[firstSeq-msg.Seq+1:]
	}
	px.keep[px.me] = msg.Seq
	for i := 0; i < len(px.peers); i++ {
		if i != px.me {
			px.outc <- Message{
				Type: DoneBroadcast,
				From: px.me,
				To:   i,
				Seq:  msg.Seq,
			}
		}
	}
	px.sendMessage(Message{
		Type: DoneSnap,
		Id:   msg.Id,
		To:   None,
	})
}

func (px *Paxos) handleMinQuery(msg Message) {

}

func (px *Paxos) handleMaxQuery(msg Message) {
	lastSeq := px.ins[len(px.ins)-1].Seq
	px.sendMessage(Message{
		Type: MaxQuery,
		Id:   msg.Id,
		To:   None,
		Seq:  lastSeq,
	})
}

// for proposer process timeout
func (px *Paxos) handleTimeout(t Timeout) {
	if ins, err := px.instanceAt(t.Seq); err == nil {
		if t.Pid.Equals(ins.Choose) {
			// when prepare timeout but haven't received enough agree prepare
			if t.Phase == Prepare && len(ins.Qp) < px.quorum {
				pid := px.seq.Next()
				ins.Choose = pid
				ins.Qp = map[int]Propose{}
				px.updateInstance(ins)

				px.startPrepare(t.Seq, pid)
			}

			if t.Phase == Accept && len(ins.Qa) < px.quorum {
				pid := px.seq.Next()
				ins.Choose = pid
				ins.Qp = map[int]Propose{}
				ins.Qa = map[int]bool{}

				px.updateInstance(ins)
				px.startAccept(t.Seq, pid, ins.Val)
			}
		}
	}
}

func (px *Paxos) startPrepare(seq int, pid Pid) {
	for i := 0; i < len(px.peers); i++ {
		px.outc <- Message{
			Type: PrepareReq,
			From: px.me,
			To:   i,
			Seq:  seq,
			Pid:  pid,
		}
	}

	// set timeout
	t := Timeout{
		Seq:   seq,
		Pid:   pid,
		Phase: Prepare,
	}
	time.AfterFunc(DefaultTimeout, func() {
		px.timec <- t
	})
}

func (px *Paxos) startAccept(seq int, pid Pid, val interface{}) {
	for i := 0; i < len(px.peers); i++ {
		px.outc <- Message{
			Type: AcceptReq,
			From: px.me,
			To:   i,
			Seq:  seq,
			Pid:  pid,
			Val:  val,
		}
	}

	// set timeout
	t := Timeout{
		Seq:   seq,
		Pid:   pid,
		Phase: Accept,
	}
	time.AfterFunc(DefaultTimeout, func() {
		px.timec <- t
	})
}

func (px *Paxos) handlePrepare(msg Message) {
	if ins, err := px.instanceAt(msg.Seq); err == nil {
		if msg.Pid.After(ins.MinPid) {
			ins.MinPid = msg.Pid
			px.updateInstance(ins)

			px.sendMessage(Message{
				Type:  PrepareRes,
				Id:    msg.Id,
				From:  px.me,
				To:    msg.From,
				Agree: true,
				Pid:   ins.AcceptedPid,
				Val:   ins.AcceptedVal,
			})
		} else {
			px.sendMessage(Message{
				Type:  PrepareRes,
				Id:    msg.Id,
				From:  px.me,
				To:    msg.From,
				Agree: false,
				Pid:   ins.AcceptedPid,
				Val:   nil,
			})
		}
	}
}

func (px *Paxos) handlePrepareRes(msg Message) {
	if ins, err := px.instanceAt(msg.Seq); err == nil {
		if ins.Choose.Equals(msg.Pid) { //with same round
			if msg.Agree {
				ins.Qp[msg.From] = Propose{
					AcceptedPid: msg.Pid,
					AcceptedVal: msg.Val,
				}

				if len(ins.Qp) == px.quorum {
					maxPid := Pid{
						Pre: -1,
						Suf: 0,
					}
					var v interface{}
					for _, p := range ins.Qp {
						if maxPid.Before(p.AcceptedPid) {
							maxPid = p.AcceptedPid
							v = p.AcceptedVal
						}
					}

					if v != nil {
						ins.Val = v
					}

					px.startAccept(ins.Seq, ins.Choose, ins.Val)
				}
				px.updateInstance(ins)
			}
		}
	}
}

func (px *Paxos) handleAccept(msg Message) {
	if ins, err := px.instanceAt(msg.Seq); err == nil {
		if msg.Pid.After(ins.MinPid) || msg.Pid.Equals(ins.MinPid) {
			ins.MinPid = msg.Pid
			ins.AcceptedPid = msg.Pid
			ins.AcceptedVal = msg.Val
			px.updateInstance(ins)

			px.sendMessage(Message{
				Type:  AcceptRes,
				Id:    msg.Id,
				From:  px.me,
				To:    msg.From,
				Seq:   msg.Seq,
				Pid:   ins.MinPid,
				Agree: true,
				Val:   nil,
			})
		} else {
			px.sendMessage(Message{
				Type:  AcceptRes,
				Id:    msg.Id,
				From:  px.me,
				To:    msg.From,
				Seq:   msg.Seq,
				Pid:   ins.MinPid,
				Agree: false,
				Val:   nil,
			})
		}
	}
}

func (px *Paxos) handleAcceptRes(msg Message) {
	if ins, err := px.instanceAt(msg.Seq); err == nil {
		if ins.Choose.Equals(msg.Pid) { // with same round
			if msg.Agree {
				ins.Qa[msg.From] = true

				// choose a value
				if len(ins.Qa) == px.quorum {
					ins.Fate = Decided
				}

				px.updateInstance(ins)
			}
		}
	}
}

func (px *Paxos) handleDoneBroadcast(msg Message) {
	if px.keep[msg.From] < msg.Seq {
		px.keep[msg.From] = msg.Seq
	}
	px.sendMessage(Message{})
}

// found instance at seq, if seq smaller than first seq, return error
// if seq bigger than last seq, create instance and full it util seq
func (px *Paxos) instanceAt(seq int) (Instance, error) {
	var instance Instance
	if seq < px.ins[0].Seq {
		return instance, errors.New("out of index:" + strconv.Itoa(seq))
	}
	lastSeq := px.ins[len(px.ins)-1].Seq
	if seq > lastSeq {
		for i := lastSeq + 1; i <= seq; i++ {
			px.ins = append(px.ins, Instance{
				Seq:  i,
				Fate: Pending,
			})
		}
	}
	return px.ins[seq-px.ins[0].Seq], nil
}

// call after instanceAt, always in instances
func (px *Paxos) updateInstance(ins Instance) {
	px.ins[ins.Seq-px.ins[0].Seq] = ins
}
