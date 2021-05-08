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
	"errors"
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
	Follower ServerState = iota
	Candidate
	Leader
)

var (
	NoSuchEntryError = errors.New("(No Such A Entry)")
)

const None int = -1

const (
	TickUnit           = 5 * time.Millisecond
	ElectionBase int64 = 50
	Heartbeat    int64 = 20
)

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

	transport *Transport
	storage   *Storage

	tickCh      chan interface{}
	elecTimeout int64
	hbTimeout   int64
	elecElapsed int64
	hbElapsed   int64

	stepFunc func(rf *Raft, msg Message)
}

type HardState struct {
	CurrentTerm int
	VoteFor     int
	Logs        []LogEntry
	LastApplied int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	hd := HardState{
		CurrentTerm: rf.currentTerm,
		VoteFor:     rf.voteFor,
		Logs:        rf.storage.ents,
		LastApplied: rf.storage.lastApplied,
	}
	e.Encode(hd)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte, snapshot []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var hd HardState
	if d.Decode(&hd) != nil {
		panic("error when decode state")
	} else {
		rf.currentTerm = hd.CurrentTerm
		rf.voteFor = hd.VoteFor
		rf.storage.ents = hd.Logs

		rf.storage.lastApplied = hd.LastApplied
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
	resCh := rf.transport.waitForRPCRes(Message{
		MType:       MsgCondInstallSnapshot,
		From:        None,
		PrevLogIdx:  lastIncludedIndex,
		PrevLogTerm: lastIncludedTerm,
		Data:        snapshot,
	})
	select {
	case msg := <-resCh:
		return msg.Agreed
	}
}

// the service says it has created a sn that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	<-rf.transport.waitForRPCRes(Message{
		MType:      MsgSnapshot,
		From:       None,
		PrevLogIdx: index,
		Data:       snapshot,
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
	resCh := rf.transport.waitForRPCRes(Message{
		MType:       MsgVoteRequest,
		From:        args.CandidateId,
		To:          rf.me,
		Term:        args.Term,
		PrevLogIdx:  args.LastLogIndex,
		PrevLogTerm: args.LastLogTerm,
	})
	select {
	case msg := <-resCh:
		reply.Term = msg.Term
		reply.VoteGranted = msg.Agreed
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
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool

	LastLogIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	resCh := rf.transport.waitForRPCRes(Message{
		MType:        MsgAppendRequest,
		From:         args.LeaderId,
		To:           rf.me,
		Term:         args.Term,
		PrevLogIdx:   args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      args.Entries,
		LeaderCommit: args.LeaderCommit,
	})
	select {
	case msg := <-resCh:
		reply.Term = msg.Term
		reply.Success = msg.Agreed
		reply.LastLogIndex = msg.PrevLogIdx
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
	LastIncludedIndex int
	Term              int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	resCh := rf.transport.waitForRPCRes(Message{
		MType:       MsgInstallSnapshotRequest,
		From:        args.LeaderId,
		To:          rf.me,
		Term:        args.Term,
		PrevLogIdx:  args.LastIncludedIndex,
		PrevLogTerm: args.LastIncludedTerm,
		Data:        args.Data,
	})
	select {
	case msg := <-resCh:
		reply.Term = msg.Term
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
	resCh := rf.transport.waitForRPCRes(Message{
		MType:   MsgAppendCommand,
		Command: command,
		From:    None,
	})
	select {
	case msg := <-resCh:
		isLeader := msg.PrevLogIdx != -1
		return msg.PrevLogIdx, msg.PrevLogTerm, isLeader
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
	rf.transport.stop()
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

	rf.currentTerm = 0
	rf.voteFor = None
	rf.state = Follower

	rf.votes = make([]bool, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))

	transport := &Transport{
		rf:   rf,
		recv: make(chan Message, 128),
		out:  make(chan Message, 128),
		rpcm: make(map[int64]chan Message),
	}
	rf.transport = transport

	snapshot := Snapshot{
		LastIncludedTerm:  0,
		LastIncludedIndex: 0,
		Data:              nil,
	}
	storage := &Storage{
		rf:          rf,
		me:          rf.me,
		sn:          snapshot,
		term:        rf.currentTerm,
		ents:        make([]LogEntry, 0),
		lastApplied: 0,
		commitIndex: 0,
		applyCh:     applyCh,
	}
	rf.storage = storage

	rf.tickCh = make(chan interface{}, 32)

	rf.hbTimeout = Heartbeat
	rf.resetElecTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())
	rf.becomeFollower(rf.currentTerm)

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.run()
	go rf.transport.run()

	return rf
}

func (rf *Raft) run() {
	for rf.killed() == false {
		select {
		case <-rf.tickCh:
			rf.tick()
		case msg := <-rf.transport.recv:
			rf.step(msg)
		}
	}
}

func (rf *Raft) tick() {
	if rf.state == Leader {
		rf.hbElapsed++
		if rf.hbElapsed > rf.hbTimeout {
			rf.heartbeat()
		}
	} else {
		rf.elecElapsed++
		if rf.elecElapsed > rf.elecTimeout {
			rf.becomeCandidate()
		}
	}
}

func (rf *Raft) heartbeat() {
	for server := 0; server < len(rf.peers); server++ {
		if rf.nextIndex[server] <= rf.storage.sn.LastIncludedIndex {
			rf.sendInstallSnapshot(server)
		} else {
			rf.sendAppendEntries(server)
		}
	}
	rf.hbElapsed = 0
}

func (rf *Raft) sendAppendEntries(server int) {
	if server != rf.me {
		prevLogTerm, prevLogIndex, entries := rf.storage.fromEntries(rf.nextIndex[server])
		rf.sendMessage(Message{
			MType:        MsgAppendRequest,
			From:         rf.me,
			To:           server,
			Term:         rf.currentTerm,
			PrevLogIdx:   prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.storage.commitIndex,
		})
	}
}

func (rf *Raft) sendInstallSnapshot(server int) {
	if server != rf.me {
		rf.sendMessage(Message{
			MType:       MsgInstallSnapshotRequest,
			From:        rf.me,
			To:          server,
			Term:        rf.currentTerm,
			PrevLogIdx:  rf.storage.sn.LastIncludedIndex,
			PrevLogTerm: rf.storage.sn.LastIncludedTerm,
			Data:        rf.storage.sn.Data,
		})
	}
}

func (rf *Raft) becomeFollower(term int) {
	rf.stepFunc = stepFollower

	rf.restElecElapsed()
	rf.state = Follower

	if term > rf.currentTerm {
		rf.voteFor = None
	}

	rf.currentTerm = term
	rf.storage.term = term
	rf.persist()
}

func (rf *Raft) becomeCandidate() {
	rf.stepFunc = stepCandidate

	rf.currentTerm++
	rf.storage.term = rf.currentTerm
	rf.voteFor = rf.me
	rf.state = Candidate
	rf.resetElecTimeout()

	rf.persist()
	rf.startElection()
}

func (rf *Raft) startElection() {
	lastTerm, lastIndex := rf.storage.mostContainsInfo()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.votes[i] = false
			rf.sendMessage(Message{
				MType:       MsgVoteRequest,
				From:        rf.me,
				To:          i,
				Term:        rf.currentTerm,
				PrevLogIdx:  lastIndex,
				PrevLogTerm: lastTerm,
			})
		} else {
			rf.votes[i] = true
		}
	}
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.stepFunc = stepLeader

	_, lastIndex := rf.storage.mostContainsInfo()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.matchIndex[i] = lastIndex
		} else {
			rf.matchIndex[i] = 0
		}
		rf.nextIndex[i] = lastIndex + 1
	}

	rf.triggerHeartbeat(true)
}

func (rf *Raft) step(msg Message) {
	if msg.From == rf.me {
		// need send out
		DPrintf("error, can't send msg to mySelf")

	} else if msg.From == None {
		// client msg
		switch msg.MType {
		case MsgAppendCommand:
			if rf.state == Leader {
				DPrintf("leader %v append command: %v", rf.me, msg.Command)
				term, index := rf.storage.appendCommand(msg.Command)
				rf.matchIndex[rf.me] = index
				rf.sendMessage(Message{
					Id:          msg.Id,
					From:        rf.me,
					To:          None,
					PrevLogIdx:  index,
					PrevLogTerm: term,
				})
				rf.triggerHeartbeat(false)
			} else {
				rf.sendMessage(Message{
					Id:          msg.Id,
					From:        rf.me,
					To:          None,
					PrevLogIdx:  -1,
					PrevLogTerm: -1,
				})
			}
			break
		case MsgSnapshot:
			rf.doWithSnapshot(msg)
			break
		case MsgCondInstallSnapshot:
			rf.doWithCondSnapshot(msg)
			break
		default:
			DPrintf("unsupported msg type: %v", msg)
		}

	} else {
		// process msg
		if msg.Term < rf.currentTerm {
			rf.reject(msg)
			return
		}

		if msg.Term > rf.currentTerm ||
			(msg.MType == MsgAppendRequest && rf.state == Candidate) {
			rf.becomeFollower(msg.Term)
		}

		rf.stepFunc(rf, msg)
	}
}

func (rf *Raft) reject(msg Message) {
	var mtype MessageType
	switch msg.MType {
	case MsgVoteRequest:
		mtype = MsgVoteResponse
		break
	case MsgAppendRequest:
		mtype = MsgAppendResponse
		break
	case MsgInstallSnapshotRequest:
		mtype = MsgInstallSnapshotResponse
		break
	}
	rf.sendMessage(Message{
		MType:  mtype,
		From:   rf.me,
		To:     msg.From,
		Id:     msg.Id,
		Agreed: false,
		Term:   rf.currentTerm,
	})
}

func (rf *Raft) sendMessage(msg Message) {
	rf.transport.out <- msg
}

func (rf *Raft) grantedVote(msg Message) {
	rf.voteFor = msg.From
	rf.restElecElapsed()
	rf.persist()

	rf.sendMessage(Message{
		MType:  MsgVoteResponse,
		From:   rf.me,
		To:     msg.From,
		Id:     msg.Id,
		Agreed: true,
		Term:   rf.currentTerm,
	})
}

func (rf *Raft) restElecElapsed() {
	rf.elecElapsed = 0
}

func (rf *Raft) resetElecTimeout() {
	rf.elecTimeout = ElectionBase + rand.Int63n(ElectionBase)
	rf.elecElapsed = 0
}

func (rf *Raft) triggerHeartbeat(syncNow bool) {
	if syncNow {
		rf.hbElapsed = rf.hbTimeout
	} else {
		rf.hbElapsed++
	}
}

func stepFollower(rf *Raft, msg Message) {
	switch msg.MType {
	case MsgVoteRequest:
		if rf.voteFor == None && !rf.storage.isUpToDate(msg.PrevLogTerm, msg.PrevLogIdx) {
			rf.grantedVote(msg)
		} else {
			rf.reject(msg)
		}
		break

	case MsgAppendRequest:
		rf.restElecElapsed()

		if rf.storage.match(msg.PrevLogTerm, msg.PrevLogIdx) {
			matchIndex := rf.storage.appendLogEntries(msg.PrevLogIdx, msg.Entries)
			mayCommitIndex := Min(msg.LeaderCommit, matchIndex)
			rf.storage.commit(mayCommitIndex, false)
			rf.sendMessage(Message{
				MType:      MsgAppendResponse,
				Id:         msg.Id,
				From:       rf.me,
				To:         msg.From,
				PrevLogIdx: matchIndex,
				Agreed:     true,
			})
		} else {
			rf.reject(msg)
		}
		break

	case MsgInstallSnapshotRequest:
		rf.storage.snapshot(msg.PrevLogTerm, msg.PrevLogIdx, msg.Data)
		rf.sendMessage(Message{
			MType:  MsgInstallSnapshotResponse,
			Id:     msg.Id,
			From:   rf.me,
			To:     msg.From,
			Agreed: true,
		})
		break

	default:
		// do nothing
	}
}

func stepCandidate(rf *Raft, msg Message) {
	switch msg.MType {
	case MsgVoteResponse:
		if msg.Agreed {
			rf.votes[msg.From] = true
			if Count(rf.votes) > len(rf.peers)/2 {
				rf.becomeLeader()
			}
		}
		break

	case MsgInstallSnapshotRequest:
		rf.storage.snapshot(msg.PrevLogTerm, msg.PrevLogIdx, msg.Data)
		rf.sendMessage(Message{
			MType:  MsgInstallSnapshotResponse,
			Id:     msg.Id,
			From:   rf.me,
			To:     msg.From,
			Agreed: true,
		})
		break

	default:
		// do nothing
	}
}

func stepLeader(rf *Raft, msg Message) {
	switch msg.MType {
	case MsgAppendResponse:
		if msg.Agreed {
			if msg.PrevLogIdx > rf.matchIndex[msg.From] {
				rf.matchIndex[msg.From] = msg.PrevLogIdx
				rf.nextIndex[msg.From] = msg.PrevLogIdx + 1

				mayCommitIndex := Majority(rf.matchIndex)
				rf.storage.commit(mayCommitIndex, true)
			}
		} else {
			rf.nextIndex[msg.From] = Max(rf.matchIndex[msg.From]+1, rf.nextIndex[msg.From]-1)
			rf.sendAppendEntries(msg.From)
		}
		break
	case MsgInstallSnapshotResponse:
		if msg.Agreed && rf.matchIndex[msg.From] < rf.storage.sn.LastIncludedIndex {
			rf.matchIndex[msg.From] = rf.storage.sn.LastIncludedIndex
			rf.nextIndex[msg.From] = rf.matchIndex[msg.From] + 1
		}
		break
	default:
		break
	}
}

func (rf *Raft) doWithSnapshot(msg Message) {
	if entry, err := rf.storage.entryAt(msg.PrevLogIdx); err == nil {
		rf.storage.installSnapshot(entry.Term, entry.Index, msg.Data)
	} else {
		DPrintf("warning, not have such entry in storage of index: %v", msg.PrevLogIdx)
	}
	rf.sendMessage(Message{
		Id:   msg.Id,
		From: rf.me,
		To:   None,
	})
}

func (rf *Raft) doWithCondSnapshot(msg Message) {
	ok := rf.storage.installSnapshot(msg.PrevLogTerm, msg.PrevLogIdx, msg.Data)
	rf.sendMessage(Message{
		Id:     msg.Id,
		From:   rf.me,
		To:     None,
		Agreed: ok,
	})
}

/****      Storage Begin                ******/

type Storage struct {
	rf          *Raft
	me          int
	sn          Snapshot
	term        int
	ents        []LogEntry
	lastApplied int
	commitIndex int
	applyCh     chan ApplyMsg
}

func (storage *Storage) firstLogInfo() (int, int) {
	if len(storage.ents) == 0 {
		return -1, -1
	} else {
		return storage.ents[0].Term, storage.ents[0].Index
	}
}

func (storage *Storage) lastLogInfo() (int, int) {
	if len(storage.ents) == 0 {
		return -1, -1
	} else {
		return storage.ents[len(storage.ents)-1].Term, storage.ents[len(storage.ents)-1].Index
	}
}

func (storage *Storage) mostContainsInfo() (int, int) {
	if len(storage.ents) == 0 {
		return storage.sn.LastIncludedTerm, storage.sn.LastIncludedIndex
	} else {
		return storage.ents[len(storage.ents)-1].Term, storage.ents[len(storage.ents)-1].Index
	}
}

func (storage *Storage) fromEntries(index int) (int, int, []LogEntry) {
	var prevLogTerm, prevLogIndex int
	var entries []LogEntry
	if _, err := storage.entryAt(index); err == nil {
		if index == storage.ents[0].Index {
			prevLogTerm, prevLogIndex = storage.sn.LastIncludedTerm, storage.sn.LastIncludedIndex
		} else {
			prevEntry, _ := storage.entryAt(index - 1)
			prevLogTerm, prevLogIndex = prevEntry.Term, prevEntry.Index
		}
		entries = storage.ents[index-storage.ents[0].Index:]
	} else {
		prevLogTerm, prevLogIndex = storage.mostContainsInfo()
		entries = nil
	}
	return prevLogTerm, prevLogIndex, entries
}

func (storage *Storage) entryAt(index int) (LogEntry, error) {
	if len(storage.ents) == 0 || index < storage.ents[0].Index || index > storage.ents[len(storage.ents)-1].Index {
		return LogEntry{}, NoSuchEntryError
	} else {
		logIndex := index - storage.ents[0].Index
		return storage.ents[logIndex], nil
	}
}

func (storage *Storage) isUpToDate(otherTerm int, otherIndex int) bool {
	lastTerm, lastIndex := storage.mostContainsInfo()
	return lastTerm > otherTerm || (lastTerm == otherTerm && lastIndex > otherIndex)
}

func (storage *Storage) match(otherTerm int, otherIndex int) bool {
	if otherIndex == storage.sn.LastIncludedIndex {
		return otherTerm == storage.sn.LastIncludedTerm
	}
	if entry, err := storage.entryAt(otherIndex); err != nil {
		return false
	} else {
		return entry.Term == otherTerm
	}
}

func (storage *Storage) commit(mayCommitIndex int, checkTerm bool) {
	if mayCommitIndex <= storage.commitIndex {
		return
	}

	if entry, err := storage.entryAt(mayCommitIndex); err == nil {
		if checkTerm {
			if storage.term == entry.Term {
				DPrintf("storage %v update commit index to %v", storage.me, storage.commitIndex)
				storage.commitIndex = mayCommitIndex
				storage.apply()
			}
		} else {
			DPrintf("storage %v update commit index to %v", storage.me, storage.commitIndex)
			storage.commitIndex = mayCommitIndex
			storage.apply()
		}
	} else {
		DPrintf("error, storage %v didn't has a entry in index %v", storage.me, mayCommitIndex)
	}
}

func (storage *Storage) apply() {
	if storage.lastApplied < storage.commitIndex {
		prevApplyIndex := storage.lastApplied

		for i := 0; i < len(storage.ents); i++ {
			index := storage.ents[i].Index
			if index > prevApplyIndex && index <= storage.commitIndex {
				storage.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      storage.ents[i].Command,
					CommandIndex: storage.ents[i].Index,
				}
				storage.lastApplied = index
			}
		}
		storage.rf.persist()
	}
}

func (storage *Storage) appendCommand(command interface{}) (int, int) {
	var entry LogEntry
	if len(storage.ents) == 0 {
		entry = LogEntry{
			Command: command,
			Term:    storage.term,
			Index:   storage.sn.LastIncludedIndex + 1,
		}
	} else {
		_, index := storage.lastLogInfo()
		entry = LogEntry{
			Command: command,
			Term:    storage.term,
			Index:   index + 1,
		}
	}
	storage.ents = append(storage.ents, entry)
	storage.rf.persist()
	DPrintf("storage %v append command, after: %v", storage.me, storage.String())
	return entry.Term, entry.Index
}

func (storage *Storage) appendLogEntries(matchIndex int, logs []LogEntry) int {
	if len(logs) == 0 {
		return matchIndex
	}

	if len(storage.ents) == 0 {
		storage.ents = logs
	} else {
		appendIndex := 0
		keepIndex := matchIndex + 1 - storage.ents[0].Index
		for appendIndex < len(logs) && keepIndex < len(storage.ents) {
			if logs[appendIndex].Term != storage.ents[keepIndex].Term {
				break
			}
			appendIndex++
			keepIndex++
		}
		if appendIndex < len(logs) {
			storage.ents = append(storage.ents[:keepIndex], logs[appendIndex:]...)
		}
	}
	DPrintf("storage %v append logs, after: %v", storage.me, storage.String())
	storage.rf.persist()
	return logs[len(logs)-1].Index
}

func (storage *Storage) installSnapshot(term int, index int, snapshot []byte) bool {
	if index <= storage.sn.LastIncludedIndex {
		return false
	}

	storage.sn.LastIncludedIndex = index
	storage.sn.LastIncludedTerm = term
	storage.sn.Data = snapshot
	if _, err := storage.entryAt(index + 1); err == nil {
		firstIndex := storage.ents[0].Index
		storage.ents = storage.ents[index+1-firstIndex:]
	} else {
		storage.ents = []LogEntry{}
	}
	storage.rf.persist()
	return true
}

func (storage *Storage) snapshot(term int, index int, data []byte) {
	storage.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      data,
		SnapshotTerm:  term,
		SnapshotIndex: index,
	}
}

func (storage *Storage) String() string {
	l := len(storage.ents)
	if l == 0 {
		return fmt.Sprintf("{%v, %v, %v}", storage.commitIndex,
			storage.sn.LastIncludedTerm, storage.sn.LastIncludedIndex)
	} else {
		return fmt.Sprintf("{%v, %v, %v, [(%v, %v), (%v, %v)]}", storage.commitIndex,
			storage.sn.LastIncludedTerm, storage.sn.LastIncludedIndex,
			storage.ents[0].Term, storage.ents[0].Index,
			storage.ents[l-1].Term, storage.ents[l-1].Index)
	}

}

/****      Storage End                ******/

/****      Transport Start   ****/

const MAX_RETRY_TIMES = 3

type Transport struct {
	rf *Raft

	recv chan Message
	out  chan Message
	rpcm map[int64]chan Message

	lock sync.Mutex

	flag int32

	rpcCount int
}

func (ts *Transport) waitForRPCRes(request Message) <-chan Message {
	id := rand.Int63()
	ch := make(chan Message)
	request.Id = id

	ts.lock.Lock()
	ts.rpcm[id] = ch
	ts.lock.Unlock()

	ts.recv <- request

	return ch
}

func (ts *Transport) run() {
	for !ts.stopped() {
		select {
		case msg := <-ts.out:
			if msg.To != None {
				if msg.MType == MsgVoteRequest || msg.MType == MsgAppendRequest || msg.MType == MsgInstallSnapshotRequest {
					ts.rpcCount++
					go func() {
						ts.remote(msg)
					}()
				} else if msg.MType == MsgVoteResponse || msg.MType == MsgAppendResponse || msg.MType == MsgInstallSnapshotResponse {
					ts.responseRPC(msg)
				} else {
					DPrintf("warning: error msg type: %v", msg.MType)
				}
			} else {
				ts.responseRPC(msg)
			}
		}
	}
}

func (ts *Transport) remote(msg Message) {
	if msg.Term < ts.rf.currentTerm {
		// discard outdate msg
		return
	}
	switch msg.MType {

	// only send when candidate
	case MsgVoteRequest:
		if ts.rf.state == Candidate {
			args := &RequestVoteArgs{
				Term:         msg.Term,
				CandidateId:  msg.From,
				LastLogIndex: msg.PrevLogIdx,
				LastLogTerm:  msg.PrevLogTerm,
			}
			reply := &RequestVoteReply{}
			if ok := ts.rf.peers[msg.To].Call("Raft.RequestVote", args, reply); ok {
				ts.recv <- Message{
					MType:  MsgVoteResponse,
					From:   msg.To,
					To:     msg.From,
					Term:   reply.Term,
					Agreed: reply.VoteGranted,
				}
			} else {
				ts.out <- msg
			}
		}
		break

	// only send when Leader
	case MsgAppendRequest:
		if ts.rf.state == Leader {
			args := &AppendEntriesArgs{
				Term:         msg.Term,
				LeaderId:     msg.From,
				PrevLogIndex: msg.PrevLogIdx,
				PrevLogTerm:  msg.PrevLogTerm,
				Entries:      msg.Entries,
				LeaderCommit: msg.LeaderCommit,
			}
			reply := &AppendEntriesReply{}
			if ok := ts.rf.peers[msg.To].Call("Raft.AppendEntries", args, reply); ok {
				ts.recv <- Message{
					MType:      MsgAppendResponse,
					Term:       msg.Term,
					From:       msg.To,
					To:         msg.From,
					PrevLogIdx: reply.LastLogIndex,
					Agreed:     reply.Success,
				}
			} else {
				if msg.Retry < MAX_RETRY_TIMES {
					msg.Retry++
					ts.out <- msg
				}
			}
		}
		break

	// only send when Leader
	case MsgInstallSnapshotRequest:
		if ts.rf.state == Leader {
			args := &InstallSnapshotArgs{
				Term:              msg.Term,
				LeaderId:          msg.From,
				LastIncludedIndex: msg.PrevLogIdx,
				LastIncludedTerm:  msg.PrevLogTerm,
				Data:              msg.Data,
			}
			reply := &InstallSnapshotReply{}
			if ok := ts.rf.peers[msg.To].Call("Raft.InstallSnapshot", args, reply); ok {
				ts.recv <- Message{
					MType:      MsgInstallSnapshotResponse,
					From:       msg.To,
					To:         msg.From,
					PrevLogIdx: msg.PrevLogIdx,
					Term:       msg.Term,
				}
			} else {
				ts.out <- msg
			}
		}
		break
	}
}

func (ts *Transport) responseRPC(msg Message) {
	ts.lock.Lock()
	defer ts.lock.Unlock()

	if ch, ok := ts.rpcm[msg.Id]; ok {
		delete(ts.rpcm, msg.Id)
		ch <- msg
	}
}

func (ts *Transport) stopped() bool {
	flag := atomic.LoadInt32(&ts.flag)
	return flag == 1
}

func (ts *Transport) stop() {
	atomic.StoreInt32(&ts.flag, 1)

	ts.lock.Lock()
	defer ts.lock.Unlock()
	for _, ch := range ts.rpcm {
		ch <- Message{
			Term:   ts.rf.currentTerm,
			Agreed: false,
		}
	}
}

/****      Transport End    ****/
