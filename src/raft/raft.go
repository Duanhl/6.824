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
	Follower  ServerState = 0
	Candidate ServerState = 1
	Leader    ServerState = 2
)

const None int = -1
const TickUnit = 5 * time.Millisecond

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	ps    PersistentState
	vs    VolatileState
	sn    []byte
	state ServerState

	applyCh chan ApplyMsg
	msgCh   chan Message
	resMap  map[int64]chan Message

	tickCh           chan interface{}
	electionTimeout  int64
	heartbeatTimeout int64
	elapsed          int64
}

func (rf *Raft) applyMsg() {
	if rf.vs.commitIdx > rf.ps.LastApplied {
		first := rf.firstLogIndex()
		if first == -1 {
			DPrintf("error, No log entry need apply")
		}
		for i := 0; i < len(rf.ps.Logs); i++ {
			idx := rf.ps.Logs[i].Index
			if idx > rf.ps.LastApplied && idx <= rf.vs.commitIdx {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.ps.Logs[i].Command,
					CommandIndex: rf.ps.Logs[i].Index,
				}
			}
			rf.ps.LastApplied = rf.ps.Logs[i].Index
			rf.persist()
		}
		DPrintf("server %v apply msg, lastLog:{%v, %v}", rf.me,
			rf.lastLogTerm(), rf.lastLogIndex())
	}
}

func (rf *Raft) commit() {
	mayCommitIdx := Majority(rf.vs.matchIdx)
	if mayCommitIdx > rf.vs.commitIdx && rf.ps.Logs[mayCommitIdx].Term == rf.ps.CurrentTerm {
		rf.vs.commitIdx = mayCommitIdx
	}
}

type PersistentState struct {
	CurrentTerm       int
	VoteFor           int
	Logs              []LogEntry
	LastIncludedIndex int
	LastIncludedTerm  int
	LastApplied       int
}

func (rf *Raft) lastLogIndex() int {
	ll := len(rf.ps.Logs)
	if ll == 0 {
		return rf.ps.LastIncludedIndex
	}
	return rf.ps.Logs[ll-1].Index
}

func (rf *Raft) lastLogTerm() int {
	ll := len(rf.ps.Logs)
	if ll == 0 {
		return rf.ps.LastIncludedTerm
	}
	return rf.ps.Logs[ll-1].Term
}

func (rf *Raft) firstLogIndex() int {
	return rf.ps.LastIncludedIndex
}

func (rf *Raft) firstLogTerm() int {
	return rf.ps.LastIncludedTerm
}

func (rf *Raft) appendCommand(command interface{}) (int, int) {
	index := rf.lastLogIndex() + 1
	entry := LogEntry{
		Command: command,
		Term:    rf.ps.CurrentTerm,
		Index:   index,
	}
	rf.ps.Logs = append(rf.ps.Logs, entry)
	rf.persist()
	DPrintf("leader %v add command %v in log:{%v, %v}", rf.me, command,
		rf.lastLogTerm(), rf.lastLogIndex())
	return entry.Index, entry.Term
}

func (rf *Raft) isUpToDate(otherLastTerm int, otherLastIndex int) bool {
	lastIndex := rf.lastLogIndex()
	lastTerm := rf.lastLogTerm()
	return lastTerm > otherLastTerm || (lastTerm == otherLastTerm && lastIndex > otherLastIndex)
}

func (rf *Raft) match(otherTerm int, otherIndex int) bool {
	if otherIndex > rf.lastLogIndex() || otherIndex < rf.firstLogIndex() {
		return false
	}
	if otherIndex == rf.ps.LastIncludedIndex {
		return otherTerm == rf.ps.LastIncludedTerm
	} else {
		return rf.ps.Logs[otherIndex-rf.ps.LastIncludedIndex-1].Term == otherTerm
	}
}

func (rf *Raft) firstIndexOfTheTerm(term int) int {
	if rf.ps.LastIncludedTerm == term {
		return rf.ps.LastIncludedIndex
	}
	idx := 0
	for ; idx < len(rf.ps.Logs); idx++ {
		if rf.ps.Logs[idx].Term == term {
			break
		}
	}
	if idx == len(rf.ps.Logs) {
		return -1
	}
	return rf.ps.Logs[idx].Index
}

func (rf *Raft) lastIndexOfTheTerm(term int) int {
	idx := len(rf.ps.Logs) - 1
	for ; idx > -1; idx-- {
		if rf.ps.Logs[idx].Term == term {
			break
		}
	}
	if idx == -1 {
		if term == rf.ps.LastIncludedTerm {
			return rf.ps.LastIncludedIndex
		} else {
			return -1
		}
	}
	return rf.ps.Logs[idx].Index
}

func (rf *Raft) conflictInfo(otherTerm int, otherIndex int) (int, int) {
	if otherIndex <= rf.firstLogIndex() {
		return rf.firstLogTerm(), rf.firstLogIndex()
	}
	if otherIndex > rf.lastLogIndex() {
		return 0, rf.lastLogIndex() + 1
	}
	term := rf.ps.Logs[otherIndex-rf.firstLogIndex()-1].Term
	return term, rf.firstIndexOfTheTerm(term)
}

func (rf *Raft) appendLogs(matchIdx int, entries []LogEntry) (int, int) {
	if len(entries) > 0 {
		othIdx := 0
		for ; othIdx < len(entries); othIdx++ {
			idx := entries[othIdx].Index
			if idx > rf.lastLogIndex() {
				break
			}
			if rf.ps.Logs[idx-rf.firstLogIndex()].Term != entries[othIdx].Term {
				break
			}
		}
		if othIdx < len(entries) {
			rf.ps.Logs = append(rf.ps.Logs[:entries[othIdx].Index-rf.firstLogIndex()-1], entries[othIdx:]...)
			DPrintf("follower %v append log, last log is {%v, %v}", rf.me, rf.lastLogTerm(), rf.lastLogIndex())
			rf.persist()
		}
		return entries[len(entries)-1].Term, entries[len(entries)-1].Index
	} else {
		if matchIdx == rf.ps.LastIncludedIndex {
			return rf.ps.LastIncludedIndex, rf.ps.LastIncludedTerm
		} else {
			return rf.ps.Logs[matchIdx-rf.firstLogIndex()-1].Term, matchIdx
		}
	}
}

type VolatileState struct {
	commitIdx int
	nextIdx   []int
	matchIdx  []int
	quorum    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.ps.CurrentTerm
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
	e.Encode(rf.ps)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
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
	var ps PersistentState
	if d.Decode(&ps) != nil {
		DPrintf("error when decode state")
	} else {
		rf.ps = ps
	}

}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	if rf.ps.LastIncludedIndex < lastIncludedIndex {
		rf.ps.LastIncludedIndex = lastIncludedIndex
		rf.ps.LastIncludedTerm = lastIncludedTerm
		rf.sn = snapshot
		firstIndex := rf.firstLogIndex()
		if firstIndex != -1 && rf.ps.LastIncludedIndex > firstIndex &&
			rf.ps.LastIncludedIndex < rf.lastLogIndex() {
			rf.ps.Logs = rf.ps.Logs[rf.ps.LastIncludedIndex-firstIndex+1:]
		} else {
			rf.ps.Logs = []LogEntry{}
		}
		rf.persist()
	}
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.sn = clone(snapshot)
	msg := rf.waitForRes(Message{
		MType:      MsgSnapshot,
		From:       -1,
		PrevLogIdx: index,
		Data:       snapshot,
	})
	msg.Agreed = true
}

var IDGenerator int64 = 0

func (rf *Raft) waitForRes(msg Message) Message {
	ch := make(chan Message)
	id := int64(rf.me*1000000000) + atomic.AddInt64(&IDGenerator, 1)
	rf.mu.Lock()
	rf.resMap[id] = ch
	rf.mu.Unlock()
	msg.Id = id
	rf.msgCh <- msg

	select {
	case res := <-ch:
		return res
	}
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
	msg := rf.waitForRes(Message{
		MType:       MsgVoteRequest,
		From:        args.CandidateId,
		To:          rf.me,
		Term:        args.Term,
		PrevLogIdx:  args.LastLogIndex,
		PrevLogTerm: args.LastLogTerm,
	})
	reply.Term = msg.Term
	reply.VoteGranted = msg.Agreed
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
	LastLogTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	msg := rf.waitForRes(Message{
		MType:        MsgAppendRequest,
		From:         args.LeaderId,
		To:           rf.me,
		Term:         args.Term,
		PrevLogIdx:   args.PrevLogIndex,
		PrevLogTerm:  args.PrevLogTerm,
		Entries:      args.Entries,
		LeaderCommit: args.LeaderCommit,
	})
	reply.Term = msg.Term
	reply.Success = msg.Agreed
	reply.LastLogIndex = msg.PrevLogIdx
	reply.LastLogTerm = msg.PrevLogTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	res := rf.waitForRes(Message{
		MType:       MsgInstallSnapshotRequest,
		From:        args.LeaderId,
		To:          rf.me,
		Term:        args.Term,
		PrevLogIdx:  args.LastIncludedIndex,
		PrevLogTerm: args.LastIncludedTerm,
		Data:        args.Data,
	})
	reply.Term = res.Term
}

func (rf *Raft) sendInstallSnapshot(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
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
	msg := rf.waitForRes(Message{
		MType:   MsgAppendCommand,
		Command: command,
		From:    -1,
	})

	isLeader := msg.PrevLogIdx != -1

	return msg.PrevLogIdx, msg.PrevLogTerm, isLeader
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
	for _, v := range rf.resMap {
		v <- Message{
			Term:   rf.ps.CurrentTerm,
			Agreed: false,
		}
	}
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	ch := time.Tick(TickUnit)
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-ch:
			rf.tickCh <- struct{}{}
		}
	}

}

func (rf *Raft) tick() {
	rf.elapsed++

	if rf.elapsed > rf.electionTimeout && rf.state != Leader {
		rf.becomeCandidate()
	}

	if rf.elapsed > rf.heartbeatTimeout && rf.state == Leader {
		rf.distribute()
		rf.elapsed = 0
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
	logs := []LogEntry{{
		Command: nil,
		Term:    0,
		Index:   0,
	}}
	rf.ps = PersistentState{
		CurrentTerm:       0,
		VoteFor:           None,
		Logs:              logs,
		LastIncludedIndex: 0,
		LastIncludedTerm:  0,
		LastApplied:       0,
	}
	rf.vs = VolatileState{
		commitIdx: 0,
		nextIdx:   make([]int, len(rf.peers)),
		matchIdx:  make([]int, len(rf.peers)),
	}
	rf.state = Follower

	rf.applyCh = applyCh
	rf.msgCh = make(chan Message, 128)
	rf.resMap = make(map[int64]chan Message)

	rf.tickCh = make(chan interface{}, 128)
	rf.electionTimeout = 30 + rand.Int63n(30)
	rf.heartbeatTimeout = rf.electionTimeout / 3
	rf.elapsed = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.mainLoop()

	return rf
}

func (rf *Raft) mainLoop() {
	for rf.killed() == false {
		select {
		case <-rf.tickCh:
			rf.tick()
		case msg := <-rf.msgCh:
			rf.stepMessage(msg)
		}
	}
}

func (rf *Raft) stepMessage(msg Message) {
	if msg.Term > rf.ps.CurrentTerm {
		rf.becomeFollower(msg.Term)
	}

	var ch chan Message
	if msg.Id != 0 {
		rf.mu.Lock()
		ch = rf.resMap[msg.Id]
		delete(rf.resMap, msg.Id)
		rf.mu.Unlock()
	}

	if msg.To == rf.me {
		switch msg.MType {
		case MsgVoteRequest:
			rf.stepVoteReq(msg, ch)
		case MsgVoteResponse:
			rf.stepVoteRes(msg)
		case MsgAppendRequest:
			rf.stepAppendReq(msg, ch)
		case MsgAppendResponse:
			rf.stepAppendRes(msg)
		case MsgInstallSnapshotRequest:
			rf.stepInstallSnapshotRequest(msg, ch)
		case MsgInstallSnapshotResponse:
		default:
		}
	}

	if msg.From == rf.me {
		rf.sendMsg(msg)
	}

	// local msg
	if msg.From == -1 {
		switch msg.MType {
		case MsgAppendCommand:
			rf.stepAddCommand(msg, ch)
		case MsgSnapshot:
			rf.stepSnapshot(msg.PrevLogIdx, msg.Data)
		default:
		}
	}
}

func (rf *Raft) stepVoteReq(msg Message, ch chan<- Message) {
	if msg.Term < rf.ps.CurrentTerm {
		ch <- Message{
			Term:   rf.ps.CurrentTerm,
			Agreed: false,
		}
	} else {
		if rf.ps.VoteFor == None && !rf.isUpToDate(msg.PrevLogTerm, msg.PrevLogIdx) {
			rf.grantedVote(msg.From)
			ch <- Message{
				Term:   rf.ps.CurrentTerm,
				Agreed: true,
			}
		} else {
			ch <- Message{
				Term:   rf.ps.CurrentTerm,
				Agreed: false,
			}
		}
	}
}

func (rf *Raft) stepVoteRes(msg Message) {
	if msg.Term == rf.ps.CurrentTerm && msg.Agreed && rf.state == Candidate {
		rf.elapsed = 0
		rf.vs.quorum++
	}

	if rf.vs.quorum >= len(rf.peers)/2+1 && rf.state == Candidate {
		DPrintf("candidate %v become leader in term %v", rf.me, rf.ps.CurrentTerm)
		rf.becomeLeader()
	}
}

func (rf *Raft) stepAppendReq(msg Message, ch chan<- Message) {
	if msg.Term < rf.ps.CurrentTerm {
		ch <- Message{
			Term:   rf.ps.CurrentTerm,
			Agreed: false,
		}
	} else {
		rf.becomeFollower(msg.Term)

		if rf.match(msg.PrevLogTerm, msg.PrevLogIdx) {
			lTerm, lIndex := rf.appendLogs(msg.PrevLogIdx, msg.Entries)
			if msg.LeaderCommit > rf.vs.commitIdx {
				rf.vs.commitIdx = Min(msg.LeaderCommit, rf.lastLogIndex())
				rf.applyMsg()
			}
			ch <- Message{
				Term:        rf.ps.CurrentTerm,
				PrevLogIdx:  lIndex,
				PrevLogTerm: lTerm,
				Agreed:      true,
			}
		} else {
			cTerm, cIdx := rf.conflictInfo(msg.PrevLogTerm, msg.PrevLogIdx)
			ch <- Message{
				Term:        rf.ps.CurrentTerm,
				Agreed:      false,
				PrevLogIdx:  cIdx,
				PrevLogTerm: cTerm,
			}
		}
	}
}

func (rf *Raft) stepAppendRes(msg Message) {
	if msg.Term == rf.ps.CurrentTerm && rf.state == Leader {
		if msg.Agreed {
			rf.vs.matchIdx[msg.From] = Max(msg.PrevLogIdx, rf.vs.matchIdx[msg.From])
			rf.vs.nextIdx[msg.From] = msg.PrevLogIdx + 1
			if msg.PrevLogIdx > rf.vs.commitIdx {
				rf.commit()
				rf.applyMsg()
			}
		} else {
			term := msg.PrevLogTerm
			if term == 0 || rf.lastIndexOfTheTerm(term) == -1 {
				rf.vs.nextIdx[msg.From] = msg.PrevLogIdx
			} else {
				rf.vs.nextIdx[msg.From] = rf.lastIndexOfTheTerm(term) + 1
			}
			rf.distributeLog(msg.From, false)
		}
	}
}

func (rf *Raft) stepAddCommand(msg Message, ch chan<- Message) {
	if rf.state != Leader {
		ch <- Message{
			PrevLogIdx:  -1,
			PrevLogTerm: -1,
		}
	} else {
		lastIdx, lastTerm := rf.appendCommand(msg.Command)
		ch <- Message{
			PrevLogIdx:  lastIdx,
			PrevLogTerm: lastTerm,
		}
		rf.vs.matchIdx[rf.me] = lastIdx
		// trigger distribute log entry
		rf.elapsed = rf.heartbeatTimeout
	}
}

func (rf *Raft) stepInstallSnapshotRequest(msg Message, ch chan<- Message) {
	if rf.state != Follower {
		ch <- Message{
			Agreed: false,
		}
	} else {
		if msg.Term < rf.ps.CurrentTerm {
			ch <- Message{
				Agreed: false,
			}
		} else {
			if msg.PrevLogIdx <= rf.ps.LastIncludedIndex {
				ch <- Message{
					Agreed: false,
				}
			} else {
				rf.applyCh <- ApplyMsg{
					SnapshotValid: true,
					Snapshot:      msg.Data,
					SnapshotTerm:  msg.PrevLogTerm,
					SnapshotIndex: msg.PrevLogIdx,
				}
				ch <- Message{
					Agreed: true,
				}
			}
		}
	}
}

func (rf *Raft) stepSnapshot(index int, snapshot []byte) {
	first := rf.firstLogIndex()
	if first == -1 {
		panic("invalid log state")
	}
	rf.ps.LastIncludedIndex = index
	rf.ps.LastIncludedTerm = rf.ps.Logs[index-first].Term
	if index-first+1 == rf.lastLogIndex() {
		rf.ps.Logs = nil
	} else {
		rf.ps.Logs = rf.ps.Logs[index-first+1:]
	}
	rf.persist()
}

func (rf *Raft) becomeFollower(term int) {
	rf.elapsed = 0

	oldTerm := rf.ps.CurrentTerm
	rf.ps.CurrentTerm = term
	rf.ps.VoteFor = None
	if term > oldTerm {
		rf.persist()
	}
	rf.state = Follower
}

func (rf *Raft) becomeCandidate() {
	rf.elapsed = 0
	rf.electionTimeout = 30 + rand.Int63n(30)
	rf.heartbeatTimeout = rf.electionTimeout / 3

	rf.ps.CurrentTerm += 1
	rf.ps.VoteFor = rf.me
	rf.persist()

	rf.vs.quorum = 1

	rf.state = Candidate
	for i := 0; i < len(rf.peers); i++ {
		if rf.me != i {
			rf.sendMsg(Message{
				MType:       MsgVoteRequest,
				From:        rf.me,
				To:          i,
				Term:        rf.ps.CurrentTerm,
				PrevLogIdx:  rf.lastLogIndex(),
				PrevLogTerm: rf.lastLogTerm(),
			})
		}
	}
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	for i := 0; i < len(rf.peers); i++ {
		rf.vs.nextIdx[i] = rf.lastLogIndex() + 1
		if i == rf.me {
			rf.vs.matchIdx[i] = rf.lastLogIndex()
		} else {
			rf.vs.matchIdx[i] = 0
		}

	}
	rf.elapsed = rf.heartbeatTimeout
}

func (rf *Raft) grantedVote(server int) {
	rf.ps.VoteFor = server
	rf.elapsed = 0
	rf.persist()
}

func (rf *Raft) distribute() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.distributeLog(i, true)
		}
	}
}

func (rf *Raft) distributeLog(server int, sync bool) {
	var entries []LogEntry
	var prevLogIdx, prevLogTerm int
	nextIndex := rf.vs.nextIdx[server]
	var msg Message
	if nextIndex <= rf.firstLogIndex() {
		msg = Message{
			MType:       MsgInstallSnapshotRequest,
			From:        rf.me,
			To:          server,
			Term:        rf.ps.CurrentTerm,
			PrevLogIdx:  rf.ps.LastIncludedIndex,
			PrevLogTerm: rf.ps.LastIncludedTerm,
			Data:        clone(rf.sn),
		}
	} else {
		if rf.lastLogIndex() >= nextIndex {
			entries = rf.ps.Logs[nextIndex-rf.firstLogIndex()-1:]
			prevLogIdx = nextIndex - 1
			if prevLogIdx == rf.ps.LastIncludedIndex {
				prevLogTerm = rf.ps.LastIncludedTerm
			} else {
				prevLogTerm = rf.ps.Logs[prevLogIdx-rf.firstLogIndex()-1].Term
			}
		} else {
			entries = nil
			prevLogIdx = rf.lastLogIndex()
			prevLogTerm = rf.lastLogTerm()
		}
		msg = Message{
			MType:        MsgAppendRequest,
			From:         rf.me,
			To:           server,
			Term:         rf.ps.CurrentTerm,
			PrevLogIdx:   prevLogIdx,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
			LeaderCommit: rf.vs.commitIdx,
		}
	}
	if sync {
		rf.sendMsg(msg)
	} else {
		rf.msgCh <- msg
	}
}

func (rf *Raft) sendMsg(msg Message) {
	if msg.From == rf.me {
		switch msg.MType {
		case MsgVoteRequest:
			if rf.state == Candidate && msg.Term == rf.ps.CurrentTerm {
				go func() {
					reply := &RequestVoteReply{}
					ok := rf.sendRequestVote(msg.To, &RequestVoteArgs{
						Term:         msg.Term,
						CandidateId:  rf.me,
						LastLogIndex: msg.PrevLogIdx,
						LastLogTerm:  msg.PrevLogTerm,
					}, reply)
					if !ok {
						time.AfterFunc(TickUnit, func() {
							rf.msgCh <- msg
						})
					} else {
						rf.msgCh <- Message{
							MType:  MsgVoteResponse,
							From:   msg.To,
							To:     rf.me,
							Term:   reply.Term,
							Agreed: reply.VoteGranted,
						}
					}
				}()
			}
		case MsgAppendRequest:
			if rf.state == Leader && msg.Term == rf.ps.CurrentTerm {
				go func() {
					reply := &AppendEntriesReply{}
					ok := rf.sendAppendEntries(msg.To, &AppendEntriesArgs{
						Term:         msg.Term,
						LeaderId:     rf.me,
						PrevLogIndex: msg.PrevLogIdx,
						PrevLogTerm:  msg.PrevLogTerm,
						Entries:      msg.Entries,
						LeaderCommit: msg.LeaderCommit,
					}, reply)
					if !ok {
						time.AfterFunc(TickUnit, func() {
							rf.msgCh <- msg
						})
					} else {
						rf.msgCh <- Message{
							MType:       MsgAppendResponse,
							From:        msg.To,
							To:          rf.me,
							Term:        reply.Term,
							Agreed:      reply.Success,
							PrevLogTerm: reply.LastLogTerm,
							PrevLogIdx:  reply.LastLogIndex,
						}
					}
				}()
			}
		}
	}
}
