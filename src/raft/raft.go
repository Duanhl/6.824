package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
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
// tester) on the same server, via the applyCh passed to Make(). set
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
	// state a Raft server must maintain.

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

func (rf *Raft) appendLogs(begin int, entries []LogEntry) {
	if len(entries) == 0 {
		rf.logs = rf.logs[:begin]
	} else {
		rf.logs = append(rf.logs[:begin], entries...)
	}

	//conflict := begin
	//for conflict := begin; conflict < len(rf.logs) && conflict < len(entries)+begin; conflict++ {
	//	if rf.logs[conflict].Term != entries[conflict-begin].Term {
	//		break
	//	}
	//}
	//if conflict < len(entries)+begin {
	//	rf.logs = append(rf.logs[:conflict], entries[conflict-begin:]...)
	//}
}

type LogEntry struct {
	Val  interface{}
	Term int
}

//add to proc channel
type Msg struct {
	msgType MsgType

	server       int
	term         int
	leaderCommit int

	prevLogIndex int
	prevLogTerm  int

	command interface{}
	entries []LogEntry

	result bool

	res chan interface{}
}

type MsgType int

const (
	VoteReq MsgType = 1
	VoteRes         = 2
	AppReq          = 3
	AppRes          = 4
	Command         = 5
)

// return currentTerm and whether this server
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
	var logs []LogEntry
	if d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil {
		return
	}
	rf.currentTerm = term
	rf.votedFor = voteFor
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
		msgType:      VoteReq,
		server:       args.CandidateId,
		term:         args.Term,
		prevLogIndex: args.LastLogIndex,
		prevLogTerm:  args.LastLogTerm,
		res:          resc,
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

	Server       int
	LastLogIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	resc := make(chan interface{})

	rf.procc <- Msg{
		msgType: AppReq,
		server:  args.LeaderId,
		term:    args.Term,

		leaderCommit: args.LeaderCommit,
		prevLogTerm:  args.PrevLogTerm,
		prevLogIndex: args.PrevLogIndex,

		entries: args.Entries,

		res: resc,
	}

	select {
	case res := <-resc:
		resAsReply := res.(*AppendEntriesReply)
		reply.Term = resAsReply.Term
		reply.Success = resAsReply.Success
		reply.LastLogIndex = resAsReply.LastLogIndex
		return
	}
}

func (rf *Raft) sendAppendEntriesReq(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

type StartCommandRes struct {
	term     int
	index    int
	isLeader bool
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
// Term. the third return value is true if this server believes it is
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
			res:     resc,
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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

	rf.elecTimeout = 500 + rand.Int63n(200)
	rf.hbTimeout = 100 + rand.Int63n(10)
	rf.elecElapsed = 0
	rf.hbElapsed = 0
	rf.elecTicker = time.NewTicker(time.Millisecond)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.run()

	return rf
}

func (rf *Raft) run() {
	for {
		select {
		case <-rf.elecTicker.C:
			rf.dealElec()
		case proc := <-rf.procc:
			if proc.term > rf.currentTerm {
				rf.becomeFollower(proc.term)
			}

			switch proc.msgType {
			case VoteReq:
				rf.stepVoteReq(&RequestVoteArgs{
					Term:         proc.term,
					CandidateId:  proc.server,
					LastLogIndex: proc.prevLogIndex,
					LastLogTerm:  proc.prevLogTerm,
				}, proc.res)
			case VoteRes:
				rf.stepVoteRes(&RequestVoteReply{
					Term:        proc.term,
					VoteGranted: proc.result,
				})
			case AppReq:
				rf.stepAppendReq(&AppendEntriesArgs{
					Term:         proc.term,
					LeaderId:     proc.server,
					PrevLogIndex: proc.prevLogIndex,
					PrevLogTerm:  proc.prevLogTerm,
					Entries:      proc.entries,
					LeaderCommit: proc.leaderCommit,
				}, proc.res)
			case AppRes:
				rf.stepAppendReply(&AppendEntriesReply{
					Term:         proc.term,
					Success:      proc.result,
					Server:       proc.server,
					LastLogIndex: proc.prevLogIndex,
				})
			case Command:
				rf.stepCommand(proc.command, proc.res)
			}
		case <-rf.stopc:
			return
		}
	}
}

func (rf *Raft) checkAndSwitch(term int) bool {
	if term > rf.currentTerm {
		rf.becomeFollower(term)
		return true
	}
	return false
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

		applyTerm := rf.logs[rf.lastApplied].Term
		DPrintf("server %v apply logs: (term: %v, index: %v)", rf.me, applyTerm, rf.lastApplied)
	}
}

func (rf *Raft) stepVoteReq(args *RequestVoteArgs, replyc chan<- interface{}) {
	if args.Term < rf.currentTerm {
		replyc <- &RequestVoteReply{
			rf.currentTerm,
			false,
		}
		return
	}

	if rf.votedFor == Non &&
		!rf.upToDate(args.LastLogTerm, args.LastLogIndex) {
		DPrintf("server %v vote for %v in term %v", rf.me, args.CandidateId, args.Term)
		rf.votedFor = args.CandidateId
		replyc <- &RequestVoteReply{
			rf.currentTerm,
			true,
		}
	} else {
		replyc <- &RequestVoteReply{
			rf.currentTerm,
			false,
		}
	}
}

func (rf *Raft) stepVoteRes(reply *RequestVoteReply) {
	if rf.servState != Candidate {
		return
	}

	rf.elecElapsed = 0
	if reply.VoteGranted {
		rf.myVote++
	}

	if rf.myVote >= rf.quorum {
		rf.becomeLeader()
	}
}

func (rf *Raft) stepAppendReq(args *AppendEntriesArgs, replyc chan<- interface{}) {
	if args.Term < rf.currentTerm {
		replyc <- &AppendEntriesReply{
			Term:    rf.currentTerm,
			Success: false,
		}
		return
	}

	rf.becomeFollower(args.Term)
	if !rf.entryMatched(args.PrevLogTerm, args.PrevLogIndex) {
		replyc <- &AppendEntriesReply{
			Term:    rf.currentTerm,
			Success: false,
		}
		return
	} else {
		rf.appendLogs(args.PrevLogIndex+1, args.Entries)
		replyc <- &AppendEntriesReply{
			Term:         rf.currentTerm,
			Success:      true,
			LastLogIndex: len(rf.logs) - 1,
		}

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		}
		rf.apply()
	}

}

func (rf *Raft) stepAppendReply(reply *AppendEntriesReply) {
	if rf.servState != Leader {
		return
	}
	if reply.Success {
		rf.matchIdxs[reply.Server] = reply.LastLogIndex
		rf.nextIdxs[reply.Server] = reply.LastLogIndex + 1

		if reply.LastLogIndex > rf.commitIndex {
			match := 1
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me && rf.matchIdxs[i] >= reply.LastLogIndex {
					match++
				}
			}
			if match >= rf.quorum {
				DPrintf("server %v update commitIndex from %v to %v", rf.me, rf.commitIndex, reply.LastLogIndex)
				rf.commitIndex = reply.LastLogIndex
				rf.apply()
			}
		}
	} else {
		// republic log failed, next decrement
		DPrintf("server %v reject leader %v 's log", reply.Server, rf.me)

		//fixme
		rf.nextIdxs[reply.Server] = rf.nextIdxs[reply.Server] - 1
		rf.republicLog(reply.Server)
	}
}

func (rf *Raft) stepCommand(command interface{}, replyc chan interface{}) {
	if rf.servState != Leader {
		replyc <- &StartCommandRes{
			term:     -1,
			index:    -1,
			isLeader: false,
		}
	} else {
		rf.logs = append(rf.logs, LogEntry{
			Val:  command,
			Term: rf.currentTerm,
		})

		lastIdx, lastTerm := rf.lastEntryInfo()
		DPrintf("Leader %v(term: %v) step command, logs: (term: %v, idx: %v)", rf.me, rf.currentTerm, lastTerm, lastIdx)
		replyc <- &StartCommandRes{
			term:     rf.currentTerm,
			index:    len(rf.logs) - 1,
			isLeader: true,
		}
		rf.republic()
	}
}

func (rf *Raft) dealElec() {
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
	rf.currentTerm = term
	rf.servState = Follower
	rf.votedFor = Non
	rf.elecElapsed = 0
	rf.hbElapsed = 0

	rf.persist()
}

func (rf *Raft) becomeLeader() {
	rf.servState = Leader
	rf.elecElapsed = 0
	rf.hbTimeout = rf.hbElapsed
	rf.myVote = 0

	lastIdx, _ := rf.lastEntryInfo()
	for i := 0; i < len(rf.peers); i++ {
		rf.matchIdxs[i] = 0
		rf.nextIdxs[i] = lastIdx + 1
	}

	rf.persist()
	DPrintf("server %v convert to leader, currentTerm: %v", rf.me, rf.currentTerm)
}

func (rf *Raft) becomeCandidate() {
	rf.servState = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.myVote = 1
	rf.elecElapsed = 0

	DPrintf("server %v convert to candidate, currentTerm: %v", rf.me, rf.currentTerm)
	rf.persist()
	rf.sendVoteReq()
}

func (rf *Raft) sendVoteReq() {
	lidx, lterm := rf.lastEntryInfo()
	arg := &RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		lidx,
		lterm,
	}
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(server, arg, reply)
				if ok {
					rf.procc <- Msg{
						msgType: VoteRes,
						server:  server,
						term:    reply.Term,
						result:  reply.VoteGranted,
					}
				}
			}(i)
		}
	}
}

func (rf *Raft) republic() {
	rf.hbElapsed = 0
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.republicLog(i)
		}
	}
}

func (rf *Raft) republicLog(server int) {
	if rf.servState != Leader {
		return
	}

	nextIdx := rf.nextIdxs[server]
	lastLogIdx, lastLogTerm := rf.lastEntryInfo()
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}

	if nextIdx > lastLogIdx {
		args.PrevLogTerm = lastLogTerm
		args.PrevLogIndex = lastLogIdx
		args.Entries = nil
	} else {
		args.PrevLogIndex = nextIdx - 1
		args.PrevLogTerm = rf.logs[nextIdx-1].Term
		args.Entries = rf.logs[nextIdx:]
	}

	reply := &AppendEntriesReply{}
	go func() {
		if rf.sendAppendEntriesReq(server, args, reply) {
			rf.procc <- Msg{
				msgType:      AppRes,
				term:         reply.Term,
				server:       server,
				result:       reply.Success,
				prevLogIndex: reply.LastLogIndex,
			}
		} else {
			rf.republicLog(server)
		}
	}()
}
