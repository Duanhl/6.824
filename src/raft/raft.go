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
	state ServerState

	applyCh chan ApplyMsg
	msgCh   chan Message
	resMap  map[int64]chan Message

	tickCh          chan interface{}
	electionTimeout int64
	republicTimeout int64
	elapsed         int64
}

func (rf *Raft) applyMsg() {
	if rf.vs.commitIdx > rf.vs.lastApplied {
		for i := rf.vs.lastApplied + 1; i <= rf.vs.commitIdx; i++ {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.ps.Logs[i].Command,
				CommandIndex: rf.ps.Logs[i].Index,
			}
			rf.vs.lastApplied++
		}
	}
}

type PersistentState struct {
	CurrentTerm int
	VoteFor     int
	Logs        []LogEntry
}

func (ps *PersistentState) lastLogIndex() int {
	return len(ps.Logs) - 1
}

func (ps *PersistentState) lastLogTerm() int {
	return ps.Logs[len(ps.Logs)-1].Term
}

func (ps *PersistentState) appendCommand(command interface{}) (int, int) {
	entry := LogEntry{
		Command: command,
		Term:    ps.CurrentTerm,
		Index:   len(ps.Logs),
	}
	ps.Logs = append(ps.Logs, entry)
	return entry.Index, entry.Term
}

func (ps *PersistentState) isUpToDate(otherLastTerm int, otherLastIndex int) bool {
	lastIndex := ps.lastLogIndex()
	lastTerm := ps.lastLogTerm()
	return lastTerm > otherLastTerm ||
		(lastTerm == otherLastTerm && lastIndex > otherLastIndex)
}

func (ps *PersistentState) match(otherTerm int, otherIndex int) bool {
	if otherIndex >= len(ps.Logs) {
		return false
	}
	return ps.Logs[otherIndex].Term == otherTerm
}

func (ps *PersistentState) appendLogs(entries []LogEntry) {
	if len(entries) == 0 {
		return
	}
	mMatch := -1
	eMatch := 0
	for i := 0; i < len(entries); i++ {
		idx := entries[i].Index
		if idx < len(ps.Logs) && ps.Logs[idx].Term != entries[i].Term {
			mMatch = idx
			eMatch = i
			break
		}
	}
	if mMatch > -1 {
		ps.Logs = append(ps.Logs[:mMatch], entries[eMatch:]...)
	}
}

type VolatileState struct {
	commitIdx   int
	lastApplied int
	nextIdx     []int
	matchIdx    []int
	quorum      int
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
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

var IDGenerator int64 = 0

func (rf *Raft) waitForRes(msg Message) Message {
	ch := make(chan Message)
	id := int64(rf.me*1000000000) + atomic.AddInt64(&IDGenerator, 1)
	rf.resMap[id] = ch
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
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

	if rf.elapsed > rf.republicTimeout && rf.state == Leader {
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
	logs := make([]LogEntry, 1)
	logs = append(logs, LogEntry{
		Command: nil,
		Term:    0,
		Index:   0,
	})
	rf.ps = PersistentState{
		CurrentTerm: 0,
		VoteFor:     None,
		Logs:        logs,
	}
	rf.vs = VolatileState{
		commitIdx:   0,
		lastApplied: 0,
		nextIdx:     make([]int, len(rf.peers)),
		matchIdx:    make([]int, len(rf.peers)),
	}
	rf.state = Follower

	rf.applyCh = applyCh
	rf.msgCh = make(chan Message, 128)
	rf.resMap = make(map[int64]chan Message)

	rf.tickCh = make(chan interface{}, 128)
	rf.electionTimeout = 30 + rand.Int63n(30)
	rf.republicTimeout = rf.electionTimeout / 3
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

	if msg.To == rf.me {
		switch msg.MType {
		case MsgVoteRequest:
			ch := rf.resMap[msg.Id]
			delete(rf.resMap, msg.Id)
			rf.stepVoteReq(msg, ch)
		case MsgVoteResponse:
			rf.stepVoteRes(msg)
		case MsgAppendRequest:
			ch := rf.resMap[msg.Id]
			delete(rf.resMap, msg.Id)
			rf.stepAppendReq(msg, ch)
		case MsgAppendResponse:
			rf.stepAppendRes(msg)
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
			ch := rf.resMap[msg.Id]
			delete(rf.resMap, msg.Id)
			rf.stepAddCommand(msg, ch)
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
		if rf.ps.VoteFor == None && !rf.ps.isUpToDate(msg.PrevLogTerm, msg.PrevLogIdx) {
			rf.ps.VoteFor = msg.From
			rf.elapsed = 0
			ch <- Message{
				Term:   rf.ps.CurrentTerm,
				Agreed: true,
			}
			DPrintf("follower %v vote for candidate %v in term: %v", rf.me, msg.From, rf.ps.CurrentTerm)
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
		DPrintf("candidate %v receive %v votes, become leader in term %v", rf.me, rf.vs.quorum, rf.ps.CurrentTerm)
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

		if rf.ps.match(msg.PrevLogTerm, msg.PrevLogIdx) {
			rf.ps.appendLogs(msg.Entries)
			if msg.LeaderCommit > rf.vs.commitIdx {
				rf.vs.commitIdx = Min(msg.LeaderCommit, rf.ps.lastLogIndex())
				rf.applyMsg()
			}
		} else {
			ch <- Message{
				Term:   rf.ps.CurrentTerm,
				Agreed: true,
			}
		}
	}
}

func (rf *Raft) stepAppendRes(msg Message) {
	if msg.Term == rf.ps.CurrentTerm && rf.state == Leader {
		if msg.Agreed {

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
		lastIdx, lastTerm := rf.ps.appendCommand(msg.Command)
		ch <- Message{
			PrevLogIdx:  lastIdx,
			PrevLogTerm: lastTerm,
		}
		// trigger distribute log entry
		rf.elapsed = rf.republicTimeout
	}
}

func (rf *Raft) becomeFollower(term int) {
	rf.elapsed = 0

	rf.ps.CurrentTerm = term
	rf.ps.VoteFor = None
	rf.state = Follower
}

func (rf *Raft) becomeCandidate() {
	rf.elapsed = 0
	rf.electionTimeout = 30 + rand.Int63n(30)
	rf.republicTimeout = rf.electionTimeout / 3

	rf.ps.CurrentTerm += 1
	rf.ps.VoteFor = rf.me
	rf.vs.quorum = 1

	rf.state = Candidate
	for i := 0; i < len(rf.peers); i++ {
		if rf.me != i {
			rf.sendMsg(Message{
				MType:       MsgVoteRequest,
				From:        rf.me,
				To:          i,
				Term:        rf.ps.CurrentTerm,
				PrevLogIdx:  rf.ps.lastLogIndex(),
				PrevLogTerm: rf.ps.lastLogTerm(),
			})
		}
	}
	DPrintf("server %v become candidate, term: %v", rf.me, rf.ps.CurrentTerm)
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	for i := 0; i < len(rf.peers); i++ {
		rf.vs.nextIdx[i] = rf.ps.lastLogIndex() + 1
		rf.vs.matchIdx[i] = 0
	}
	rf.elapsed = rf.republicTimeout
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
	if rf.ps.lastLogIndex() >= rf.vs.nextIdx[server] {
		entries = rf.ps.Logs[rf.vs.nextIdx[server]:]
	} else {
		entries = nil
	}
	prevLogIdx := rf.vs.nextIdx[server] - 1
	prevLogTerm := rf.ps.Logs[prevLogIdx].Term
	msg := Message{
		MType:        MsgAppendRequest,
		From:         rf.me,
		To:           server,
		Term:         rf.ps.CurrentTerm,
		PrevLogIdx:   prevLogIdx,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: rf.vs.commitIdx,
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
							MType:  MsgAppendResponse,
							From:   msg.To,
							To:     rf.me,
							Term:   reply.Term,
							Agreed: reply.Success,
						}
					}
				}()
			}
		}
	}
}
