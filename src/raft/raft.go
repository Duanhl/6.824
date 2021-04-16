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
	"sort"
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
	ps     PersistentState
	vs     VolatileState
	sn     []byte
	state  ServerState
	leader int

	applyCh chan ApplyMsg
	msgCh   chan Message
	resMap  map[int64]chan Message

	ready Ready

	tickCh           chan interface{}
	electionTimeout  int64
	heartbeatTimeout int64
	elapsed          int64
}

func (rf *Raft) applyMsg() {
	if rf.vs.commitIdx > rf.ps.LastApplied {
		for i := 0; i < len(rf.ps.Logs); i++ {
			idx := rf.ps.Logs[i].Index
			if idx > rf.ps.LastApplied && idx <= rf.vs.commitIdx {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.ps.Logs[i].Command,
					CommandIndex: rf.ps.Logs[i].Index,
				}
				rf.ps.LastApplied = rf.ps.Logs[i].Index
				rf.persist()
			}
		}
	}
}

func (rf *Raft) commit() {
	mayCommitIdx := Majority(rf.vs.matchIdx)
	entry := rf.entry(mayCommitIdx)
	if mayCommitIdx > rf.vs.commitIdx && entry.Term == rf.ps.CurrentTerm {
		rf.vs.commitIdx = mayCommitIdx
		fEntry := rf.ps.Logs[0]
		DPrintf("leader %v commit log, last commit log: (%v, %v), first log entry: (%v, %v)", rf.me, entry.Term, mayCommitIdx,
			fEntry.Term, fEntry.Index)
	}

}

/**
 * ----------------------  --------------------------------
 * |  lastIncludedIndex |  |  Index   | Index   | Index   |
 * |  lastIncludedTerm  |  |  Term    | Term    | Term    |
 * |  snapshot          |  |  Command | Command | Command |
 * |---------------------  --------------------------------
 * 日志分为两个部分，一个部分包含已经丢弃了的日志，这些日志都是已经执行了的，只需要记录
 * 执行后的快照和被丢弃的最后一个日志的信息，另外就是仍留存的日志
 */
type PersistentState struct {
	CurrentTerm       int
	VoteFor           int
	Logs              []LogEntry
	LastIncludedIndex int
	LastIncludedTerm  int
	LastApplied       int
}

func (rf *Raft) lastLogInfo() (int, int) {
	if len(rf.ps.Logs) == 0 {
		return rf.ps.LastIncludedTerm, rf.ps.LastIncludedIndex
	} else {
		entry := rf.ps.Logs[len(rf.ps.Logs)-1]
		return entry.Term, entry.Index
	}
}

func (rf *Raft) realLogIndex(index int) int {
	_, lIndex := rf.lastLogInfo()
	if index > lIndex {
		return -1
	} else {
		return index - rf.ps.LastIncludedIndex - 1
	}
}

func (rf *Raft) appendCommand(command interface{}) (int, int) {
	_, oldIndex := rf.lastLogInfo()
	index := oldIndex + 1
	entry := LogEntry{
		Command: command,
		Term:    rf.ps.CurrentTerm,
		Index:   index,
	}
	rf.ps.Logs = append(rf.ps.Logs, entry)
	rf.persist()
	return entry.Index, entry.Term
}

func (rf *Raft) isUpToDate(otherLastTerm int, otherLastIndex int) bool {
	lTerm, lIndex := rf.lastLogInfo()
	return lTerm > otherLastTerm || (lTerm == otherLastTerm && lIndex > otherLastIndex)
}

func (rf *Raft) match(otherTerm int, otherIndex int) bool {
	_, lIndex := rf.lastLogInfo()
	if otherIndex > lIndex || otherIndex < rf.ps.LastIncludedIndex {
		return false
	}
	return rf.entry(otherIndex).Term == otherTerm
}

func (rf *Raft) entry(index int) *LogEntry {
	_, lIndex := rf.lastLogInfo()
	if index < rf.ps.LastIncludedIndex || index > lIndex {
		return nil
	} else if index == rf.ps.LastIncludedIndex {
		return &LogEntry{
			Term:  rf.ps.LastIncludedTerm,
			Index: rf.ps.LastIncludedIndex,
		}
	} else {
		return &rf.ps.Logs[rf.realLogIndex(index)]
	}
}

func (rf *Raft) appendLogs(matchIdx int, entries []LogEntry) (int, int) {
	if len(entries) > 0 {
		othIdx := 0
		_, lIndex := rf.lastLogInfo()
		for ; othIdx < len(entries); othIdx++ {
			idx := entries[othIdx].Index
			if idx > lIndex {
				break
			}
			if rf.entry(idx).Term != entries[othIdx].Term {
				break
			}
		}
		if othIdx < len(entries) {
			rf.ps.Logs = append(rf.ps.Logs[:entries[othIdx].Index-rf.ps.LastIncludedIndex-1], entries[othIdx:]...)
			rf.persist()
		}
		return entries[len(entries)-1].Term, entries[len(entries)-1].Index
	} else {
		if matchIdx == rf.ps.LastIncludedIndex {
			return rf.ps.LastIncludedTerm, rf.ps.LastIncludedIndex
		} else {
			return rf.ps.Logs[rf.realLogIndex(matchIdx)].Term, matchIdx
		}
	}
}

type VolatileState struct {
	commitIdx int
	nextIdx   []int
	matchIdx  []int
	quorum    int
}

type Ready struct {
	msgs     []Message
	needSend bool
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
	var ps PersistentState
	if d.Decode(&ps) != nil {
		DPrintf("error when decode state")
	} else {
		rf.ps = ps
		rf.sn = snapshot
	}

}

func (rf *Raft) RaftStateSize() int {
	return rf.persister.RaftStateSize()
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.waitForRes(Message{
		MType:       MsgConInstallSnapshot,
		From:        -1,
		PrevLogIdx:  lastIncludedIndex,
		PrevLogTerm: lastIncludedTerm,
		Data:        snapshot,
	})
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.msgCh <- Message{
		MType:      MsgSnapshot,
		From:       -1,
		PrevLogIdx: index,
		Data:       snapshot,
	}
}

func (rf *Raft) waitForRes(msg Message) Message {
	ch := make(chan Message)
	id := rand.Int63()
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
	LastIncludedIndex int
	Term              int
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

	// quick check
	if rf.state == Leader {
		msg := rf.waitForRes(Message{
			MType:   MsgAppendCommand,
			Command: command,
			From:    -1,
		})
		isLeader := msg.PrevLogIdx != -1

		return msg.PrevLogIdx, msg.PrevLogTerm, isLeader
	} else {
		return -1, -1, false
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
	var logs []LogEntry
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
	rf.ready = Ready{msgs: []Message{}, needSend: false}

	rf.tickCh = make(chan interface{}, 128)
	rf.resetTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.mainLoop()

	return rf
}

func (rf *Raft) mainLoop() {
	for rf.killed() == false {
		if len(rf.ready.msgs) > 0 && rf.ready.needSend {
			msgs := rf.ready.msgs
			rf.remote(msgs)
			rf.ready.msgs = []Message{}
			rf.ready.needSend = false
		}
		select {
		case <-rf.tickCh:
			rf.tick()
			rf.ready.needSend = true
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
			rf.stepInstallSnapshotResponse(msg)
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
		case MsgConInstallSnapshot:
			rf.stepCondInstallSnapshot(msg.PrevLogIdx, msg.PrevLogTerm, msg.Data, ch)
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
			_, lIndex := rf.appendLogs(msg.PrevLogIdx, msg.Entries)
			mayCommitIndex := Min(msg.LeaderCommit, lIndex)
			if mayCommitIndex > rf.vs.commitIdx {
				DPrintf("follow %v update commitIndex %v -> %v in term %v", rf.me, rf.vs.commitIdx, mayCommitIndex, rf.ps.CurrentTerm)
				rf.vs.commitIdx = mayCommitIndex
				rf.applyMsg()
			}
			ch <- Message{
				Term:       rf.ps.CurrentTerm,
				PrevLogIdx: lIndex,
				Agreed:     true,
			}
		} else {
			ch <- Message{
				Term:   rf.ps.CurrentTerm,
				Agreed: false,
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
			rf.vs.nextIdx[msg.From]--
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
	if rf.state != Follower || msg.Term < rf.ps.CurrentTerm || msg.PrevLogIdx <= rf.ps.LastIncludedIndex {
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
			PrevLogIdx:  msg.PrevLogIdx,
			PrevLogTerm: msg.PrevLogTerm,
			Agreed:      true,
		}
	}
}

func (rf *Raft) stepInstallSnapshotResponse(msg Message) {
	if msg.Agreed {
		if msg.PrevLogIdx > rf.vs.matchIdx[msg.From] {
			rf.vs.matchIdx[msg.From] = msg.PrevLogIdx
			rf.vs.nextIdx[msg.From] = msg.PrevLogIdx + 1
		}
	}
}

func (rf *Raft) stepSnapshot(index int, snapshot []byte) {
	if index <= rf.ps.LastIncludedIndex {
		DPrintf("warning, already snapshot until index %v", index)
		return
	}
	_, lIndex := rf.lastLogInfo()
	if index > lIndex {
		DPrintf("error, index more than current log, index: %v, currentLogIndex: %v", index, lIndex)
		return
	}

	keepLogIndex := rf.realLogIndex(index + 1)
	entry := rf.entry(index)
	rf.ps.LastIncludedIndex = index
	rf.ps.LastIncludedTerm = entry.Term
	if keepLogIndex == -1 {
		rf.ps.Logs = []LogEntry{}
	} else {
		rf.ps.Logs = rf.ps.Logs[keepLogIndex:]
	}
	rf.sn = snapshot
	rf.persist()
	var st, si, lt, li int
	if len(rf.ps.Logs) == 0 {
		st, si, lt, li = -1, -1, -1, -1
	} else {
		st = rf.ps.Logs[0].Term
		si = rf.ps.Logs[0].Index
		lt = rf.ps.Logs[len(rf.ps.Logs)-1].Term
		li = rf.ps.Logs[len(rf.ps.Logs)-1].Index
	}
	DPrintf("server %v snapshot, current log: {(%v, %v),(%v, %v) (%v, %v)}, lastApplied: %v ",
		rf.me, rf.ps.LastIncludedTerm, rf.ps.LastIncludedIndex, st, si, lt, li, rf.ps.LastApplied)
}

func (rf *Raft) stepCondInstallSnapshot(lastIncludedIndex int, lastIncludedTerm int, snapshot []byte, ch chan<- Message) {
	if rf.ps.LastIncludedIndex < lastIncludedIndex {
		rf.ps.LastIncludedIndex = lastIncludedIndex
		rf.ps.LastIncludedTerm = lastIncludedTerm
		rf.sn = snapshot
		logIndex := 0
		find := false
		for ; logIndex < len(rf.ps.Logs); logIndex++ {
			if rf.ps.Logs[logIndex].Index > lastIncludedIndex {
				find = true
				break
			}
		}
		if find {
			rf.ps.Logs = rf.ps.Logs[logIndex:]
		} else {
			rf.ps.Logs = []LogEntry{}
		}
		prevApplied := rf.ps.LastApplied
		if rf.ps.LastApplied < rf.ps.LastIncludedIndex {
			rf.ps.LastApplied = rf.ps.LastIncludedIndex
		}
		nowApplied := rf.ps.LastApplied
		rf.persist()

		var st, si, lt, li int
		if len(rf.ps.Logs) == 0 {
			st, si, lt, li = -1, -1, -1, -1
		} else {
			st = rf.ps.Logs[0].Term
			si = rf.ps.Logs[0].Index
			lt = rf.ps.Logs[len(rf.ps.Logs)-1].Term
			li = rf.ps.Logs[len(rf.ps.Logs)-1].Index
		}

		DPrintf("server %v update current snapshot {(%v, %v), (%v, %v), (%v, %v)}, applied %v -> %v",
			rf.me, rf.ps.LastIncludedTerm, rf.ps.LastIncludedIndex, st, si, lt, li, prevApplied, nowApplied)
		ch <- Message{
			Agreed: true,
		}
	} else {
		ch <- Message{
			Agreed: false,
		}
	}
}

func (rf *Raft) becomeFollower(term int) {
	rf.elapsed = 0
	rf.state = Follower
	if term > rf.ps.CurrentTerm {
		rf.ps.CurrentTerm = term
		rf.ps.VoteFor = None
		rf.persist()
	}
}

func (rf *Raft) becomeCandidate() {
	rf.resetTimeout()

	rf.ps.CurrentTerm += 1
	rf.ps.VoteFor = rf.me
	rf.persist()

	rf.vs.quorum = 1

	rf.state = Candidate
	lt, li := rf.lastLogInfo()
	for i := 0; i < len(rf.peers); i++ {
		if rf.me != i {
			rf.sendMsg(Message{
				MType:       MsgVoteRequest,
				From:        rf.me,
				To:          i,
				Term:        rf.ps.CurrentTerm,
				PrevLogIdx:  li,
				PrevLogTerm: lt,
			})
		}
	}
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	for i := 0; i < len(rf.peers); i++ {
		_, li := rf.lastLogInfo()
		rf.vs.nextIdx[i] = li + 1
		if i == rf.me {
			rf.vs.matchIdx[i] = li
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

func (rf *Raft) resetTimeout() {
	rf.electionTimeout = 40 + rand.Int63n(40)
	rf.heartbeatTimeout = 20 + rand.Int63n(10)
	rf.elapsed = 0
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
	if nextIndex <= rf.ps.LastIncludedIndex {
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
		lTerm, lIndex := rf.lastLogInfo()
		if lIndex >= nextIndex {
			entries = rf.ps.Logs[rf.realLogIndex(nextIndex):]
			prevLogIdx = nextIndex - 1
			prevLogTerm = rf.entry(prevLogIdx).Term
		} else {
			entries = nil
			prevLogIdx = lIndex
			prevLogTerm = lTerm
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

func (rf *Raft) remote(msgs []Message) {
	voteReqs := make([]*Message, len(rf.peers))
	appendReqs := make(map[int][]Message)
	installReqs := make([]*Message, len(rf.peers))

	for i := 0; i < len(msgs); i++ {
		msg := msgs[i]
		if msg.Term == rf.ps.CurrentTerm {
			if rf.state == Candidate && msg.MType == MsgVoteRequest {
				voteReqs[msg.To] = &msg
			}
			if rf.state == Leader && msg.MType == MsgAppendRequest {
				if arr, ok := appendReqs[msg.To]; ok {
					arr = append(arr, msg)
				} else {
					appendReqs[msg.To] = []Message{msg}
				}
			}
			if rf.state == Leader && msg.MType == MsgInstallSnapshotRequest {
				installReqs[msg.To] = &msg
			}
		}
	}

	for i := 0; i < len(rf.peers); i++ {
		if voteReqs[i] != nil {
			rf.send(*voteReqs[i])
		}
		if arr, ok := appendReqs[i]; ok {
			if len(arr) > 0 {
				sort.Sort(MessageSorter(arr))
				rf.send(arr[len(arr)-1])
			}
		}
		if installReqs[i] != nil {
			rf.send(*installReqs[i])
		}
	}
}

func (rf *Raft) sendMsg(msg Message) {
	rf.ready.msgs = append(rf.ready.msgs, msg)
}

func (rf *Raft) send(msg Message) {
	switch msg.MType {
	case MsgVoteRequest:
		go func() {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(msg.To, &RequestVoteArgs{
				Term:         msg.Term,
				CandidateId:  rf.me,
				LastLogIndex: msg.PrevLogIdx,
				LastLogTerm:  msg.PrevLogTerm,
			}, reply)
			if !ok {
				rf.msgCh <- msg
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

	case MsgAppendRequest:
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
				rf.msgCh <- msg
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

	case MsgInstallSnapshotRequest:
		if rf.state == Leader && msg.Term == rf.ps.CurrentTerm {
			go func() {
				reply := &InstallSnapshotReply{}
				ok := rf.peers[msg.To].Call("Raft.InstallSnapshot", &InstallSnapshotArgs{
					Term:              msg.Term,
					LeaderId:          rf.me,
					LastIncludedIndex: msg.PrevLogIdx,
					LastIncludedTerm:  msg.PrevLogTerm,
					Data:              msg.Data,
				}, reply)
				if ok {
					rf.msgCh <- Message{
						MType:      MsgInstallSnapshotResponse,
						From:       msg.To,
						To:         rf.me,
						PrevLogIdx: reply.LastIncludedIndex,
						Agreed:     true,
					}
				} else {
					rf.msgCh <- msg
				}
			}()
		}
	}
}
