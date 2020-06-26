package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft from.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same from.
//

import (
	"bytes"
	"labgob"
	"labrpc"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same from, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

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
	// state a Raft from must maintain.

	// persistent state
	currentTerm int
	votedFor    int
	myVote      int
	quorum      int
	logs        []LogEntry
	servState   ServState

	//volatile state
	commitIndex int
	lastApplied int
	applyChan   chan ApplyMsg

	nextIdxs  []int
	matchIdxs []int

	stopc chan struct{}
	procc chan Msg

	//for election
	elecTimeout int64
	hbTimeout   int64
	elecElapsed int64
	hbElapsed   int64
	elecTicker  *time.Ticker
}

const (
	ElectionPeriod  int64 = 50
	HeartbeatPeriod       = 6
)

const (
	Heartbeat time.Duration = 5 * time.Millisecond
)

type ServState int

const (
	Leader    ServState = 0
	Candidate           = 1
	Follower            = 2
)

const Non int = -1

func (rf *Raft) lastEntryInfo() (int, int) {
	return len(rf.logs) - 1, rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) upToDate(otherTerm int, otherIdx int) bool {
	lastIdx, lastTerm := rf.lastEntryInfo()
	return lastTerm > otherTerm ||
		(lastTerm == otherTerm && lastIdx > otherIdx)
}

func (rf *Raft) entryMatched(otherTerm int, otherIdx int) bool {
	return otherIdx < len(rf.logs) && rf.logs[otherIdx].Term == otherTerm
}

func (rf *Raft) findFirstIndexInTerm(otherTerm int) int {
	index := -1
	for i := len(rf.logs) - 1; i > -1; i-- {
		if rf.logs[i].Term == otherTerm {
			index = i
		} else if rf.logs[i].Term < otherTerm {
			break
		}
	}
	if index > -1 {
		return index
	} else {
		return rf.findFirstIndexInTerm(otherTerm - 1)
	}
}

func (rf *Raft) appendLogs(begin int, entries []LogEntry) {
	if len(entries) == 0 {
		rf.logs = rf.logs[:begin]
	} else {
		rf.logs = append(rf.logs[:begin], entries...)
	}
}

type LogEntry struct {
	Val  interface{}
	Term int
}

//add to proc channel
type Msg struct {
	msgType MsgType

	from         int
	to           int
	state        ServState
	term         int
	leaderCommit int

	logIndex int
	logTerm  int

	command interface{}
	entries []LogEntry

	result bool

	resc chan interface{}
}

type MsgType int

const (
	VoteReq    MsgType = 1
	VoteRes            = 2
	AppReq             = 3
	AppRes             = 4
	Command            = 5
	CommandRes         = 6
)

// return currentTerm and whether this from
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	term = rf.currentTerm
	isleader = rf.servState == Leader
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

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	e.Encode(rf.logs)

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
	var term int
	var voteFor int
	var commitIndex int
	var lastApplied int
	var logs []LogEntry
	if d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&commitIndex) != nil ||
		d.Decode(&lastApplied) != nil ||
		d.Decode(&logs) != nil {
		return
	}
	rf.currentTerm = term
	rf.votedFor = voteFor
	rf.commitIndex = commitIndex
	rf.lastApplied = lastApplied
	rf.logs = logs
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
	resc := make(chan interface{})

	rf.procc <- Msg{
		msgType:  VoteReq,
		from:     args.CandidateId,
		term:     args.Term,
		logIndex: args.LastLogIndex,
		logTerm:  args.LastLogTerm,
		resc:     resc,
	}

	select {
	case res := <-resc:
		resAsReply := res.(*RequestVoteReply)
		reply.Term = resAsReply.Term
		reply.VoteGranted = resAsReply.VoteGranted
		return
	}
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

	PrevLogIndex int
	PrevLogTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	resc := make(chan interface{})

	rf.procc <- Msg{
		msgType: AppReq,
		from:    args.LeaderId,
		term:    args.Term,

		leaderCommit: args.LeaderCommit,
		logTerm:      args.PrevLogTerm,
		logIndex:     args.PrevLogIndex,

		entries: args.Entries,

		resc: resc,
	}

	select {
	case res := <-resc:
		resAsReply := res.(*AppendEntriesReply)
		reply.Term = resAsReply.Term
		reply.Success = resAsReply.Success
		reply.PrevLogIndex = resAsReply.PrevLogIndex
		reply.PrevLogTerm = resAsReply.PrevLogTerm
		return
	}
}

func (rf *Raft) sendAppendEntriesReq(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example code to send a RequestVote RPC to a from.
// from is the index of the target from in rf.peers[].
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
// A false return can be caused by a dead from, a live from that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the from side does not return.  Thus there
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

type StartCommandRes struct {
	term     int
	index    int
	isLeader bool
}

//
// the service using Raft (e.g. a k/v from) wants to start
// agreement on the next command to be appended to Raft's log. if this
// from isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this from believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.servState != Leader {
		return -1, -1, false
	} else {
		resc := make(chan interface{})
		rf.procc <- Msg{
			msgType: Command,
			command: command,
			resc:    resc,
		}
		select {
		case rep := <-resc:
			repl := rep.(*StartCommandRes)
			index = repl.index
			term = repl.term
			isLeader = repl.isLeader
		}
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	close(rf.stopc)
	rf.persist()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft from. the ports
// of all the Raft servers (including this one) are in peers[]. this
// from's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this from to
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
	rf.servState = Follower
	rf.currentTerm = 0
	rf.votedFor = Non
	rf.myVote = 0
	rf.quorum = len(rf.peers)>>1 + 1

	rf.logs = append(make([]LogEntry, 0), LogEntry{
		Val:  nil,
		Term: 0,
	})

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyChan = applyCh

	rf.nextIdxs = make([]int, len(peers))
	rf.matchIdxs = make([]int, len(peers))

	rf.stopc = make(chan struct{})
	rf.procc = make(chan Msg)

	rf.elecTimeout = ElectionPeriod + rand.Int63n(ElectionPeriod)
	rf.hbTimeout = HeartbeatPeriod + rand.Int63n(HeartbeatPeriod)
	rf.elecElapsed = 0
	rf.hbElapsed = 0
	rf.elecTicker = time.NewTicker(Heartbeat)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.run()

	return rf
}

func (rf *Raft) run() {
	for {
		select {
		case <-rf.elecTicker.C:
			rf.dealTick()
		case msg := <-rf.procc:
			if msg.term > rf.currentTerm {
				rf.becomeFollower(msg.term)
			}

			switch msg.msgType {
			case VoteReq:
				rf.stepVoteReq(msg)
			case VoteRes:
				rf.stepVoteRes(msg)
			case AppReq:
				rf.stepAppendReq(msg)
			case AppRes:
				rf.stepAppendReply(msg)
			case Command:
				rf.stepCommand(msg)
			}
		case <-rf.stopc:
			return
		}
	}
}

func (rf *Raft) apply() {
	if rf.lastApplied < rf.commitIndex {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			log := rf.logs[i]
			rf.applyChan <- ApplyMsg{
				CommandValid: true,
				Command:      log.Val,
				CommandIndex: i,
			}
		}
		rf.lastApplied = rf.commitIndex
		rf.persist()

		lastIdx, lastTerm := rf.lastEntryInfo()
		DPrintf("server %v apply logs, now logs: (%v, %v)", rf.me, lastTerm, lastIdx)
	}
}

func (rf *Raft) stepVoteReq(msg Msg) {
	if msg.msgType != VoteReq {
		return
	}

	if msg.term < rf.currentTerm || rf.servState != Follower {
		rf.send(Msg{
			msgType: VoteRes,
			term:    rf.currentTerm,
			result:  false,
			resc:    msg.resc,
		})
		return
	}

	if rf.votedFor == Non &&
		!rf.upToDate(msg.logTerm, msg.logIndex) {

		rf.votedFor = msg.from
		rf.elecElapsed = 0
		rf.send(Msg{
			msgType: VoteRes,
			term:    rf.currentTerm,
			result:  true,
			resc:    msg.resc,
		})
		DPrintf("server %v vote for %v in term %v", rf.me, msg.from, msg.term)
	} else {

		rf.send(Msg{
			msgType: VoteRes,
			term:    rf.currentTerm,
			result:  false,
			resc:    msg.resc,
		})
	}
}

func (rf *Raft) stepVoteRes(msg Msg) {
	if msg.msgType != VoteRes || rf.servState != Candidate {
		return
	}

	rf.elecElapsed = 0
	if msg.result {
		rf.myVote++
	}

	if rf.myVote >= rf.quorum {
		rf.becomeLeader()
	}
}

func (rf *Raft) stepAppendReq(msg Msg) {
	if msg.msgType != AppReq {
		return
	}

	if msg.term < rf.currentTerm || rf.servState == Leader {
		rf.send(Msg{
			msgType: AppRes,
			term:    rf.currentTerm,
			result:  false,
			resc:    msg.resc,
		})
		return
	}

	rf.elecElapsed = 0
	if !rf.entryMatched(msg.logTerm, msg.logIndex) {
		rf.send(Msg{
			msgType: AppRes,
			term:    rf.currentTerm,
			result:  false,
			logTerm: msg.logTerm,
			resc:    msg.resc,
		})
	} else {
		rf.appendLogs(msg.logIndex+1, msg.entries)
		rf.send(Msg{
			msgType:  AppRes,
			term:     rf.currentTerm,
			logIndex: len(rf.logs) - 1,
			result:   true,
			resc:     msg.resc,
		})

		if msg.leaderCommit > rf.commitIndex {
			rf.commitIndex = min(msg.leaderCommit, len(rf.logs)-1)
		}
		rf.apply()
	}

}

func (rf *Raft) stepAppendReply(msg Msg) {
	if msg.msgType != AppRes || msg.term < rf.currentTerm || rf.servState != Leader {
		return
	}

	if msg.result {
		rf.matchIdxs[msg.from] = msg.logIndex
		rf.nextIdxs[msg.from] = msg.logIndex + 1

		if msg.logIndex > rf.commitIndex {
			match := 1
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIdxs[i] >= msg.logIndex {
					match++
				}
			}
			if match >= rf.quorum {
				DPrintf("leader %v update commitIndex from %v to %v", rf.me, rf.commitIndex, msg.logIndex)
				rf.commitIndex = msg.logIndex
				rf.apply()
			}
		}
	} else {
		// republic log failed, next decrement
		DPrintf("follower %v reject leader %v 's log", msg.from, rf.me)

		prevIndex := rf.findFirstIndexInTerm(msg.logTerm - 1)
		rf.nextIdxs[msg.from] = max(prevIndex, 1)
		rf.republicLog(msg.from)
	}
}

func (rf *Raft) stepCommand(msg Msg) {
	if msg.msgType != Command {
		return
	}

	if rf.servState != Leader {
		rf.send(Msg{
			msgType:  CommandRes,
			state:    rf.servState,
			term:     -1,
			logIndex: -1,
			resc:     msg.resc,
		})
	} else {
		rf.logs = append(rf.logs, LogEntry{
			Val:  msg.command,
			Term: rf.currentTerm,
		})

		lastIdx, lastTerm := rf.lastEntryInfo()
		DPrintf("Leader %v(term: %v) step command, logs: (%v, %v)",
			rf.me, rf.currentTerm, lastTerm, lastIdx)
		rf.send(Msg{
			msgType:  CommandRes,
			state:    rf.servState,
			term:     rf.currentTerm,
			logIndex: len(rf.logs) - 1,
			resc:     msg.resc,
		})
	}
}

func (rf *Raft) dealTick() {
	rf.elecElapsed++
	rf.hbElapsed++

	if rf.elecElapsed > rf.elecTimeout && rf.servState != Leader {
		rf.becomeCandidate()
	}
	if rf.hbElapsed > rf.hbTimeout && rf.servState == Leader {
		rf.republic()
		rf.hbElapsed = 0
	}
}

func (rf *Raft) becomeFollower(term int) {
	rf.updateTerm(term)
	rf.servState = Follower
	rf.votedFor = Non
	rf.elecElapsed = 0
	rf.hbElapsed = 0
}

func (rf *Raft) updateTerm(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.elecTimeout = ElectionPeriod + rand.Int63n(ElectionPeriod)
	}
}

func (rf *Raft) becomeLeader() {
	rf.servState = Leader
	rf.elecElapsed = 0
	rf.hbElapsed = rf.hbTimeout + 1
	rf.myVote = 0

	lastIdx, _ := rf.lastEntryInfo()
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIdxs[i] = 0
		rf.nextIdxs[i] = lastIdx + 1
	}

	DPrintf("candidate %v convert to leader, currentTerm: %v", rf.me, rf.currentTerm)
}

func (rf *Raft) becomeCandidate() {
	rf.servState = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.myVote = 1
	rf.elecElapsed = 0

	lastIdx, lastTerm := rf.lastEntryInfo()
	DPrintf("server %v convert to candidate, currentTerm: %v, lastLogInfo:(%v, %v)",
		rf.me, rf.currentTerm, lastTerm, lastIdx)
	rf.requestForVote()
}

func (rf *Raft) requestForVote() {
	lidx, lterm := rf.lastEntryInfo()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.send(Msg{
				msgType:  VoteReq,
				from:     rf.me,
				to:       i,
				term:     rf.currentTerm,
				logIndex: lidx,
				logTerm:  lterm,
			})
		}
	}
}

func (rf *Raft) republic() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.republicLog(i)
		}
	}
}

func (rf *Raft) republicLog(server int) {
	nextIdx := rf.nextIdxs[server]
	lastLogIdx, lastLogTerm := rf.lastEntryInfo()
	var prevLogIndex, prevLogTerm int
	var entries []LogEntry

	if nextIdx > lastLogIdx {
		prevLogIndex = lastLogIdx
		prevLogTerm = lastLogTerm
		entries = nil
	} else {
		prevLogIndex = nextIdx - 1
		prevLogTerm = rf.logs[nextIdx-1].Term
		entries = rf.logs[nextIdx:]
	}

	rf.send(Msg{
		msgType:      AppReq,
		from:         rf.me,
		to:           server,
		term:         rf.currentTerm,
		leaderCommit: rf.commitIndex,
		logIndex:     prevLogIndex,
		logTerm:      prevLogTerm,
		entries:      entries,
	})
}

func (rf *Raft) send(msg Msg) {
	switch msg.msgType {
	//internal
	case AppRes:
		msg.resc <- &AppendEntriesReply{
			Term:         msg.term,
			Success:      msg.result,
			PrevLogTerm:  msg.logTerm,
			PrevLogIndex: msg.logIndex,
		}
		break
	case VoteRes:
		msg.resc <- &RequestVoteReply{
			Term:        msg.term,
			VoteGranted: msg.result,
		}
		break
	case CommandRes:
		msg.resc <- &StartCommandRes{
			term:     msg.term,
			index:    msg.logIndex,
			isLeader: msg.state == Leader,
		}
		break

	//	external
	case VoteReq:
		if rf.servState == Candidate && rf.currentTerm == msg.term {
			go func() {
				voteArgs := &RequestVoteArgs{
					Term:         msg.term,
					CandidateId:  msg.from,
					LastLogIndex: msg.logIndex,
					LastLogTerm:  msg.logTerm,
				}
				voteReply := &RequestVoteReply{}
				if rf.sendRequestVote(msg.to, voteArgs, voteReply) {
					rf.procc <- Msg{
						msgType: VoteRes,
						from:    msg.to,
						to:      msg.from,
						term:    voteReply.Term,
						result:  voteReply.VoteGranted,
					}
				} else {
					rf.send(msg)
				}
			}()
		}
		break
	case AppReq:
		if rf.servState == Leader && rf.currentTerm == msg.term {
			go func() {
				appArgs := &AppendEntriesArgs{
					Term:         msg.term,
					LeaderId:     msg.from,
					PrevLogIndex: msg.logIndex,
					PrevLogTerm:  msg.logTerm,
					Entries:      msg.entries,
					LeaderCommit: msg.leaderCommit,
				}
				appReply := &AppendEntriesReply{}
				if rf.sendAppendEntriesReq(msg.to, appArgs, appReply) {
					rf.procc <- Msg{
						msgType:  AppRes,
						from:     msg.to,
						to:       msg.from,
						term:     appReply.Term,
						logIndex: appReply.PrevLogIndex,
						logTerm:  appReply.PrevLogTerm,
						result:   appReply.Success,
					}
				} else {
					rf.send(msg)
				}
			}()
		}
	default:
		return
	}
}
