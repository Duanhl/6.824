package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"fmt"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type ServerState int32

const (
	Follower ServerState = iota + 1
	Candidate
	Leader
)

const None int = -1

const (
	TickUnit            = 5 * time.Millisecond
	ElectionBase  int64 = 40
	HeartbeatBase int64 = 15
)

type Context struct {
	ch   chan Message
	Type MessageType
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	voteFor     int

	state ServerState

	votes      []bool
	matchIndex []int
	nextIndex  []int

	logs        []Entry //logs, first entry is dummy entry
	lastApplied int
	commitIndex int
	sn          []byte //snapshot
	applyCh     chan ApplyMsg
	applyBuf    []ApplyMsg

	retryc chan Message
	recv   chan Message
	outc   chan Message
	rpcm   map[uint64]Context

	closeCh chan interface{}

	tickCh  chan interface{}
	elapsed int64
	timeout int64

	stepFunc func(rf *Raft, msg Message)

	mu   sync.Mutex
	cond sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	ch := rf.register(Message{
		Type: GetState,
	})
	if s, ok := (<-ch).Content.(StateInfo); ok {
		return s.Term, s.IsLeader
	} else {
		return None, false
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	rf.persister.SaveStateAndSnapshot(w.Bytes(), rf.sn)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte, snapshot []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var ents []Entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&ents) != nil {
		panic("error when decode state")
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.logs = ents
		rf.sn = snapshot
		rf.lastApplied = rf.logs[0].Index
		rf.commitIndex = rf.logs[0].Index
	}

}

func (rf *Raft) RaftStateSize() int {
	return rf.persister.RaftStateSize()
}

//
// A service wants to switch to sn.  Only do so if Raft hasn't
// have more recent info since it communicate the sn on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	ch := rf.register(Message{
		Type: CondInstallSnapshot,
		Content: SnapshotArgs{
			LastIncludedTerm:  lastIncludedTerm,
			LastIncludedIndex: lastIncludedIndex,
			Snapshot:          snapshot,
		},
	})
	if r, ok := (<-ch).Content.(bool); ok {
		return r
	} else {
		return false
	}
}

// the service says it has created a sn that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	<-rf.register(Message{
		Type: DoSnapshot,
		Content: SnapshotArgs{
			LastIncludedTerm:  0,
			LastIncludedIndex: index,
			Snapshot:          snapshot,
		},
	})
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	ch := rf.register(Message{
		Type:    VoteRequest,
		Term:    args.Term,
		Content: *args,
	})
	if c, ok := (<-ch).Content.(RequestVoteReply); ok {
		reply.Term = c.Term
		reply.VoteGranted = c.VoteGranted
	} else {
		reply.Term = None
		reply.VoteGranted = false
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	LastLogIndex int
	LastLogTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ch := rf.register(Message{
		Type:    AppendRequest,
		Term:    args.Term,
		Content: *args,
	})
	if r, ok := (<-ch).Content.(AppendEntriesReply); ok {
		reply.Term = r.Term
		reply.Success = r.Success
		reply.LastLogTerm = r.LastLogTerm
		reply.LastLogIndex = r.LastLogIndex
	} else {
		reply.Term = None
		reply.Success = false
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term  int
	Index int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ch := rf.register(Message{
		Type:    InstallSnapshotRequest,
		Term:    args.Term,
		Content: *args,
	})
	if r, ok := (<-ch).Content.(InstallSnapshotReply); ok {
		reply.Term = r.Term
	} else {
		reply.Term = None
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).
	ch := rf.register(Message{
		Type:    AppendCommand,
		Content: command,
	})
	if s, ok := (<-ch).Content.(StartInfo); ok {
		return s.Index, s.Term, s.IsLeader
	} else {
		return None, None, false
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	ticker := time.Tick(TickUnit)
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-ticker:
			rf.tickCh <- struct{}{}
		}
	}

}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.cond = *sync.NewCond(&sync.Mutex{})

	rf.currentTerm = 0
	rf.voteFor = None
	rf.state = Follower

	rf.votes = make([]bool, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))

	rf.logs = []Entry{{Term: 0, Index: 0}}
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.sn = nil
	rf.applyCh = applyCh
	rf.applyBuf = []ApplyMsg{}

	rf.recv = make(chan Message, 128)
	rf.outc = make(chan Message, 128)
	rf.retryc = make(chan Message, 32)
	rf.rpcm = make(map[uint64]Context)

	rf.closeCh = make(chan interface{})

	rf.tickCh = make(chan interface{}, 32)
	rf.resetTimeout(ElectionBase)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())
	rf.becomeFollower(rf.currentTerm)

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.run()
	go rf.out()
	go rf.apply()

	return rf
}

func (rf *Raft) run() {
	for rf.killed() == false {
		select {
		case <-rf.tickCh:
			rf.tick()
		case msg := <-rf.recv:
			rf.step(msg)
		case msg := <-rf.retryc:
			if rf.needRetry(msg) {
				rf.sendMessage(msg)
			}
		}
	}
	close(rf.closeCh)
	rf.cond.Signal()
	rf.clear()
}

func (rf *Raft) out() {
	for {
		select {
		case msg := <-rf.outc:
			switch msg.Type {
			case VoteRequest, AppendRequest, InstallSnapshotRequest:
				go rf.remote(msg)
				break
			default:
				rf.mu.Lock()
				ctx := rf.rpcm[msg.Id]
				ctx.ch <- msg
				delete(rf.rpcm, msg.Id)
				rf.mu.Unlock()
			}

		case <-rf.closeCh:
			return
		}
	}
}

func (rf *Raft) apply() {
	for {
		select {
		case <-rf.closeCh:
			close(rf.applyCh)
			return
		default:
			rf.cond.L.Lock()
			for len(rf.applyBuf) == 0 {
				rf.cond.Wait()
			}
			buf := rf.applyBuf
			rf.applyBuf = []ApplyMsg{}
			rf.cond.L.Unlock()

			for _, msg := range buf {
				rf.applyCh <- msg
			}
		}
	}
}

func (rf *Raft) tick() {
	rf.elapsed++
	if rf.elapsed > rf.timeout {
		if rf.state == Leader {
			rf.heartbeat(None)
			rf.elapsed = 0
		} else {
			rf.becomeCandidate()
		}
	}
}

func (rf *Raft) clear() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for id, ctx := range rf.rpcm {
		ctx.ch <- rf.reject(id, ctx.Type)
		delete(rf.rpcm, id)
	}
}

func (rf *Raft) becomeFollower(term int) {
	rf.stepFunc = stepFollower

	rf.state = Follower
	if term > rf.currentTerm {
		rf.voteFor = None
		rf.currentTerm = term
		rf.persist()
		DPrintf("server %v become follower in %v", rf.me, rf.currentTerm)
	}
}

func (rf *Raft) becomeCandidate() {
	rf.stepFunc = stepCandidate

	rf.currentTerm++
	rf.voteFor = rf.me
	rf.state = Candidate
	rf.resetTimeout(ElectionBase)

	rf.persist()
	DPrintf("server %v become candidate in %v", rf.me, rf.currentTerm)
	rf.startElection()
}

func (rf *Raft) startElection() {
	lastEntry := rf.logs[len(rf.logs)-1]
	for server := 0; server < len(rf.peers); server++ {
		if server != rf.me {
			rf.votes[server] = false
			rf.sendMessage(Message{
				Type: VoteRequest,
				To:   server,
				Term: rf.currentTerm,
				Content: RequestVoteArgs{
					Term:         rf.currentTerm,
					CandidateId:  rf.me,
					LastLogIndex: lastEntry.Index,
					LastLogTerm:  lastEntry.Term,
				},
			})
		} else {
			rf.votes[server] = true
		}
	}
}

func (rf *Raft) becomeLeader() {
	rf.stepFunc = stepLeader

	rf.state = Leader
	for server := 0; server < len(rf.peers); server++ {
		if server == rf.me {
			rf.matchIndex[server] = rf.logs[len(rf.logs)-1].Index
		} else {
			rf.matchIndex[server] = 0
		}
		rf.nextIndex[server] = rf.logs[len(rf.logs)-1].Index + 1
	}
	rf.resetTimeout(HeartbeatBase)
	rf.trigger()
	DPrintf("server %v become leader in %v", rf.me, rf.currentTerm)
}

func (rf *Raft) heartbeat(limit int) {
	for server := 0; server < len(rf.peers); server++ {
		if server != rf.me {
			rf.doHb(server, limit)
		}
	}
}

func (rf *Raft) doHb(server int, limit int) {
	// send snapshot
	if rf.nextIndex[server] <= rf.logs[0].Index {
		forgotten := rf.logs[0]
		rf.sendMessage(Message{
			Type: InstallSnapshotRequest,
			To:   server,
			Term: rf.currentTerm,
			Content: InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: forgotten.Index,
				LastIncludedTerm:  forgotten.Term,
				Data:              rf.sn,
			},
		})
	} else {
		prevEntry, entries := rf.entriesAfter(rf.nextIndex[server], limit)
		rf.sendMessage(Message{
			Type: AppendRequest,
			To:   server,
			Term: rf.currentTerm,
			Content: AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevEntry.Index,
				PrevLogTerm:  prevEntry.Term,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			},
		})
	}
}

func (rf *Raft) step(msg Message) {
	switch msg.Type {
	// for other peer's msg
	case VoteRequest, VoteResponse, AppendRequest, AppendResponse, InstallSnapshotRequest, InstallSnapshotResponse:
		if msg.Term < rf.currentTerm {
			if msg.Type == VoteRequest || msg.Type == AppendRequest || msg.Type == InstallSnapshotRequest {
				res := rf.reject(msg.Id, msg.Type)
				rf.sendMessage(res)
			}
			// just discard response
			return
		}

		if msg.Term > rf.currentTerm {
			rf.becomeFollower(msg.Term)
		}

		if (msg.Type == AppendRequest && rf.state == Candidate) ||
			(msg.Type == InstallSnapshotRequest && rf.state == Candidate) {
			rf.becomeFollower(msg.Term)
			rf.resetElapsed()
		}

		rf.stepFunc(rf, msg)
		return

	// for client call
	default:
		switch msg.Type {
		case GetState:
			rf.sendMessage(Message{
				Type: GetState,
				Id:   msg.Id,
				Content: StateInfo{
					Term:     rf.currentTerm,
					IsLeader: rf.state == Leader,
				},
			})
			return

		case AppendCommand:
			if rf.state == Leader {
				lastEntry := rf.appendCommand(msg.Content)
				rf.matchIndex[rf.me] = lastEntry.Index
				rf.sendMessage(Message{
					Type: AppendCommand,
					Id:   msg.Id,
					Content: StartInfo{
						Index:    lastEntry.Index,
						Term:     rf.currentTerm,
						IsLeader: true,
					},
				})
				// trigger broadcast append command
				rf.trigger()
				//rf.heartbeat()
			} else {
				rf.sendMessage(Message{
					Type: AppendCommand,
					Id:   msg.Id,
					Content: StartInfo{
						Index:    0,
						Term:     0,
						IsLeader: false,
					},
				})
			}
			return

		case DoSnapshot:
			args := msg.Content.(SnapshotArgs)
			// already do snapshot in that index
			if args.LastIncludedIndex > rf.logs[0].Index {
				if args.LastIncludedIndex > rf.logs[len(rf.logs)-1].Index {
					panic(fmt.Sprintf("server %v not has log in %v", rf.me, args.LastIncludedIndex))
				}
				entry := rf.entryAt(args.LastIncludedIndex)
				rf.doSnapshot(entry.Index, entry.Term, args.Snapshot)
			}
			rf.sendMessage(Message{
				Type: DoSnapshot,
				Id:   msg.Id,
			})
			return

		case CondInstallSnapshot:
			args := msg.Content.(SnapshotArgs)
			if args.LastIncludedIndex > rf.lastApplied {
				rf.doSnapshot(args.LastIncludedIndex, args.LastIncludedTerm, args.Snapshot)
				rf.sendMessage(Message{
					Type:    DoSnapshot,
					Id:      msg.Id,
					Content: true,
				})
			} else {
				rf.sendMessage(Message{
					Type:    DoSnapshot,
					Id:      msg.Id,
					Content: false,
				})
			}
			return
		}

	}
}

func (rf *Raft) sendMessage(msg Message) {
	rf.outc <- msg
}

func (rf *Raft) resetElapsed() {
	rf.elapsed = 0
}

func (rf *Raft) resetTimeout(base int64) {
	rf.timeout = base + rand.Int63n(base)
	rf.elapsed = 0
}

func (rf *Raft) trigger() {
	rf.elapsed = rf.timeout
}

func stepFollower(rf *Raft, msg Message) {
	switch msg.Type {
	case VoteRequest:
		args := msg.Content.(RequestVoteArgs)
		if rf.voteFor == None && !rf.isUpToDate(args.LastLogIndex, args.LastLogTerm) {
			rf.voteFor = args.CandidateId
			rf.persist()
			rf.resetElapsed()
			DPrintf("server %v vote for candidate %v in term: %v", rf.me, args.CandidateId, args.Term)
			rf.sendMessage(Message{
				Type: VoteResponse,
				Id:   msg.Id,
				Content: RequestVoteReply{
					Term:        rf.currentTerm,
					VoteGranted: true,
				},
			})
		} else {
			DPrintf("server %v reject vote from %v, myself: (t:%v, i:%v), other: (t:%v, i: %v)", rf.me, args.CandidateId,
				rf.logs[len(rf.logs)-1].Term, rf.logs[len(rf.logs)-1].Index, args.LastLogTerm, args.LastLogIndex)
			rf.sendMessage(Message{
				Type: VoteResponse,
				Id:   msg.Id,
				Content: RequestVoteReply{
					Term:        rf.currentTerm,
					VoteGranted: false,
				},
			})
		}
		return

	case AppendRequest:
		rf.resetElapsed()
		args := msg.Content.(AppendEntriesArgs)
		if rf.match(args.PrevLogIndex, args.PrevLogTerm) {
			matchIndex := rf.appendEntries(args.PrevLogIndex, args.Entries)
			mayCommitIndex := Min(args.LeaderCommit, matchIndex)
			rf.commit(mayCommitIndex)
			rf.sendMessage(Message{
				Type: AppendResponse,
				Id:   msg.Id,
				Content: AppendEntriesReply{
					Term:         rf.currentTerm,
					Success:      true,
					LastLogIndex: matchIndex,
				},
			})
		} else {
			conflict := rf.conflict(args.PrevLogIndex)
			rf.sendMessage(Message{
				Type: AppendResponse,
				Id:   msg.Id,
				Content: AppendEntriesReply{
					Term:         rf.currentTerm,
					Success:      false,
					LastLogIndex: conflict.Index,
					LastLogTerm:  conflict.Term,
				},
			})
		}
		return

	case InstallSnapshotRequest:
		rf.resetElapsed()
		args := msg.Content.(InstallSnapshotArgs)
		rf.cond.L.Lock()
		rf.applyBuf = append(rf.applyBuf, ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		})
		rf.cond.Signal()
		rf.cond.L.Unlock()
		rf.sendMessage(Message{
			Type: InstallSnapshotResponse,
			Id:   msg.Id,
			Content: InstallSnapshotReply{
				Term: rf.currentTerm,
			},
		})
		return

	default:
		// do nothing
	}
}

func stepCandidate(rf *Raft, msg Message) {
	switch msg.Type {
	case VoteResponse:
		if msg.Content.(RequestVoteReply).VoteGranted {
			rf.votes[msg.From] = true
			if Count(rf.votes) > len(rf.peers)/2 {
				rf.becomeLeader()
			}
		}
		break

	case VoteRequest:
		res := rf.reject(msg.Id, VoteRequest)
		rf.sendMessage(res)
		break
	default:
		// do nothing
		DPrintf("[candidate %v] unsupported msg: %s", rf.me, msg.String())
	}
}

func stepLeader(rf *Raft, msg Message) {
	switch msg.Type {
	case AppendResponse:
		reply := msg.Content.(AppendEntriesReply)
		if reply.Success {
			if reply.LastLogIndex > rf.matchIndex[msg.From] {
				rf.matchIndex[msg.From] = reply.LastLogIndex
				rf.nextIndex[msg.From] = reply.LastLogIndex + 1

				mayCommitIndex := Majority(rf.matchIndex)
				if mayCommitIndex > rf.commitIndex {
					if entry := rf.entryAt(mayCommitIndex); entry.Index != None && entry.Term == rf.currentTerm {
						rf.commit(mayCommitIndex)
						//need send a empty heartbeat to all server notify leader's commit index has changed
						rf.heartbeat(0)
					}
				}
			}
		} else {
			var nextIndex int
			index := rf.lastIndexOfTerm(reply.LastLogTerm)
			if reply.LastLogTerm == None || index == None {
				nextIndex = reply.LastLogIndex
			} else {
				nextIndex = index + 1
			}
			if nextIndex > rf.matchIndex[msg.From] {
				rf.nextIndex[msg.From] = nextIndex
				rf.doHb(msg.From, None)
			}
		}
		break
	case InstallSnapshotResponse:
		reply := msg.Content.(InstallSnapshotReply)
		if reply.Index > rf.matchIndex[msg.From] {
			rf.matchIndex[msg.From] = reply.Index
			rf.nextIndex[msg.From] = reply.Index + 1
		}
		break
	case VoteRequest, VoteResponse:
		// pass
		break
	default:
		DPrintf("[leader %v] unsupported msg: %s", rf.me, msg.String())
	}
}

func (rf *Raft) register(msg Message) <-chan Message {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	c := make(chan Message)
	ctx := Context{
		ch:   c,
		Type: msg.Type,
	}
	if rf.killed() == false {
		id := rand.Uint64()
		msg.Id = id
		rf.rpcm[id] = ctx
		rf.recv <- msg
		return c
	} else {
		close(c)
		return c
	}
}

// for clear
func (rf *Raft) reject(id uint64, mtype MessageType) Message {
	res := Message{
		Id:   id,
		Term: rf.currentTerm,
	}
	switch mtype {
	case VoteRequest:
		res.Content = RequestVoteReply{Term: rf.currentTerm}
		break
	case AppendRequest:
		res.Content = AppendEntriesReply{Term: rf.currentTerm}
		break
	case InstallSnapshotRequest:
		res.Content = InstallSnapshotReply{Term: rf.currentTerm}

	case GetState:
		res.Content = StateInfo{}
		break
	case AppendCommand:
		res.Content = StartInfo{}
		break
	case DoSnapshot:
		res.Content = false
		break
	case CondInstallSnapshot:
		res.Content = false
		break

	default:
		panic(fmt.Sprintf("unsupported msg type: %s", mtype.String()))
	}
	return res
}

func (rf *Raft) needRetry(msg Message) bool {
	// term small than current, shouldn't retry
	if msg.Term < rf.currentTerm {
		return false
	}
	if msg.Type == VoteResponse && rf.state != Candidate {
		return false
	}
	if (msg.Type == InstallSnapshotRequest || msg.Type == AppendRequest) && rf.state != Leader {
		return false
	}

	if msg.Type == AppendRequest {
		args := msg.Content.(AppendEntriesArgs)
		var lastIndex int
		if len(args.Entries) == 0 {
			lastIndex = args.PrevLogIndex
		} else {
			lastIndex = args.Entries[len(args.Entries)-1].Index
		}
		return lastIndex == rf.logs[len(rf.logs)-1].Index
	}
	return true
}

func (rf *Raft) retry(msg Message) {
	if !rf.killed() {
		rf.retryc <- msg
	}
}

func (rf *Raft) remote(msg Message) {
	switch msg.Type {
	case VoteRequest:
		args := msg.Content.(RequestVoteArgs)
		reply := &RequestVoteReply{}
		if ok := rf.peers[msg.To].Call("Raft.RequestVote", &args, reply); ok {
			rf.recv <- Message{
				Type:    VoteResponse,
				From:    msg.To,
				Term:    reply.Term,
				Content: *reply,
			}
		} else {
			rf.retry(msg)
		}
		break

	case AppendRequest:
		args := msg.Content.(AppendEntriesArgs)
		reply := &AppendEntriesReply{}
		if ok := rf.peers[msg.To].Call("Raft.AppendEntries", &args, reply); ok {
			rf.recv <- Message{
				Type:    AppendResponse,
				Term:    reply.Term,
				From:    msg.To,
				Content: *reply,
			}
		} else {
			rf.retry(msg)
		}
		break

	case InstallSnapshotRequest:
		args := msg.Content.(InstallSnapshotArgs)
		reply := &InstallSnapshotReply{}
		if ok := rf.peers[msg.To].Call("Raft.InstallSnapshot", &args, reply); ok {
			rf.recv <- Message{
				Type: InstallSnapshotResponse,
				From: msg.To,
				Term: reply.Term,
				Content: InstallSnapshotReply{
					Term:  reply.Term,
					Index: args.LastIncludedIndex,
				},
			}
		} else {
			rf.retry(msg)
		}
		break
	}
}

func (rf *Raft) entryAt(index int) Entry {
	if index < rf.logs[0].Index || index > rf.logs[len(rf.logs)-1].Index {
		return Entry{
			Index: None,
			Term:  None,
		}
	}
	return rf.logs[index-rf.logs[0].Index]
}

func (rf *Raft) entriesAfter(startIndex int, limit int) (Entry, []Entry) {
	firstIndex := rf.logs[0].Index
	lastIndex := rf.logs[len(rf.logs)-1].Index
	if startIndex <= firstIndex {
		panic(fmt.Sprintf("index %v outof bound [%v, _]", startIndex, rf.logs[0].Index))
	}
	if startIndex > lastIndex {
		return rf.logs[len(rf.logs)-1], nil
	} else {
		var ents []Entry
		if limit == None {
			ents = make([]Entry, lastIndex-startIndex+1)
		} else {
			ents = make([]Entry, Max(lastIndex-startIndex+1, limit))
		}
		copy(ents, rf.logs[startIndex-firstIndex:])
		return rf.logs[startIndex-firstIndex-1], ents
	}
}

func (rf *Raft) appendCommand(command interface{}) Entry {
	entry := Entry{
		Command: command,
		Term:    rf.currentTerm,
		Index:   rf.logs[len(rf.logs)-1].Index + 1,
	}
	rf.logs = append(rf.logs, entry)
	rf.persist()
	return entry
}

func (rf *Raft) appendEntries(matchIndex int, logs []Entry) int {
	appendIndex := 0
	keepIndex := matchIndex + 1 - rf.logs[0].Index
	for appendIndex < len(logs) && keepIndex < len(rf.logs) {
		if logs[appendIndex].Term != rf.logs[keepIndex].Term {
			break
		}
		appendIndex++
		keepIndex++
	}
	if appendIndex < len(logs) {
		rf.logs = append(rf.logs[:keepIndex], logs[appendIndex:]...)
	}

	rf.persist()
	if len(logs) == 0 {
		return matchIndex
	} else {
		return logs[len(logs)-1].Index
	}
}

func (rf *Raft) doSnapshot(index int, term int, sn []byte) {
	dummy := Entry{
		Term:  term,
		Index: index,
	}
	if index >= rf.logs[len(rf.logs)-1].Index {
		rf.logs = []Entry{dummy}
	} else {
		ents := []Entry{dummy}
		for i := index - rf.logs[0].Index + 1; i < len(rf.logs); i++ {
			ents = append(ents, rf.logs[i])
		}
		rf.logs = ents
	}
	rf.sn = sn
	if rf.lastApplied < index {
		rf.lastApplied = index
	}
	if rf.commitIndex < index {
		rf.commitIndex = index
	}
	DPrintf("server %v update snapshot [(t:%v, i:%v), (t:%v, i:%v)]", rf.me,
		rf.logs[0].Term, rf.logs[0].Index, rf.logs[len(rf.logs)-1].Term, rf.logs[len(rf.logs)-1].Index)
	rf.persist()
}

func (rf *Raft) isUpToDate(otherIndex int, otherTerm int) bool {
	entry := rf.logs[len(rf.logs)-1]
	return entry.Term > otherTerm || (entry.Term == otherTerm && entry.Index > otherIndex)
}

func (rf *Raft) match(otherIndex int, otherTerm int) bool {
	if otherIndex < rf.logs[0].Index || otherIndex > rf.logs[len(rf.logs)-1].Index {
		return false
	}
	return rf.logs[otherIndex-rf.logs[0].Index].Term == otherTerm
}

func (rf *Raft) commit(mayCommitIndex int) {
	if mayCommitIndex <= rf.commitIndex {
		return
	}
	rf.commitIndex = mayCommitIndex
	DPrintf("server %v commit index to %v", rf.me, rf.commitIndex)
	rf.cond.L.Lock()
	for rf.lastApplied < rf.commitIndex {
		entry := rf.entryAt(rf.lastApplied + 1)
		if entry.Index == None {
			panic(fmt.Sprintf("Wrong state, server %v not have such entry in index %v, rf.logs:[(t: %v, i: %v), (t: %v, i: %v)]",
				rf.me, rf.lastApplied+1, rf.logs[0].Term, rf.logs[0].Index,
				rf.logs[len(rf.logs)-1].Term, rf.logs[len(rf.logs)-1].Index))
		}
		rf.applyBuf = append(rf.applyBuf, ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandIndex: entry.Index,
		})
		rf.lastApplied = entry.Index
	}
	rf.cond.Signal()
	rf.cond.L.Unlock()

}

func (rf *Raft) conflict(index int) Entry {
	if index > rf.logs[len(rf.logs)-1].Index {
		return Entry{
			Term:  None,
			Index: rf.logs[len(rf.logs)-1].Index + 1,
		}
	} else if index <= rf.logs[0].Index {
		return Entry{
			Term:  None,
			Index: rf.logs[0].Index + 1,
		}
	} else {
		term := rf.entryAt(index).Term
		for i := 1; i < len(rf.logs); i++ {
			if rf.logs[i].Term == term {
				return Entry{
					Term:  rf.logs[i].Term,
					Index: rf.logs[i].Index,
				}
			}
		}
		return Entry{} // impossible goto here
	}
}

func (rf *Raft) lastIndexOfTerm(term int) int {
	if term == None {
		return None
	}
	for i := len(rf.logs) - 1; i > 0; i-- {
		if rf.logs[i].Term == term {
			return rf.logs[i].Index
		}
	}
	return None
}
