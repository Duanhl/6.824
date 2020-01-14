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
	"labrpc"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state  ServState
	quorum int

	//persist states
	currentTerm int
	preVotedFor int
	leader      int
	logs        *Log

	stopc chan struct{} //stop msg
	repc  chan ReplyMsg //reply send to main loop
	procc chan ProcMsg  //other work gorouting send to main loop

	applyChan chan ApplyMsg // as state machine

	//candidate
	myVote int

	//leader
	nextIndex  []int
	matchIndex []int

	ticker           *time.Ticker
	tick             func()
	tickMu           sync.Mutex
	electionElapsed  int
	heartbeatElapsed int
	electionTimeout  int
	heartbeatTimeout int
}

const None int = -1

type Log struct {
	mu *sync.Mutex

	entries []LogEntry

	commitIndex int

	lastApplied int

	me int
}

// give term and logIndex is up-to-date than me
func (raftLog *Log) upToDateThan(term int, logIndex int) bool {
	lastLog := LastEntry(raftLog.entries)
	return lastLog.Term > term ||
		(lastLog.Term == term && lastLog.LogIndex > logIndex)
}

func (raftLog *Log) match(prevLogIndex int, prevLogTerm int) bool {
	return len(raftLog.entries) > prevLogIndex &&
		raftLog.entries[prevLogIndex].Term == prevLogTerm
}

//for follower
func (raftLog *Log) updateCommit(commitIndex int, lastLogIndex int, applyChan chan ApplyMsg) {
	if commitIndex > raftLog.commitIndex {
		raftLog.commitIndex = Min(commitIndex, lastLogIndex)
	}
	if raftLog.lastApplied < raftLog.commitIndex {
		//apply to state machine
		for idx := raftLog.lastApplied + 1; idx <= raftLog.commitIndex; idx++ {
			entry := raftLog.entries[idx]
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.LogIndex,
			}
			applyChan <- applyMsg
		}
		raftLog.lastApplied = raftLog.commitIndex
	}
}

func (raftLog *Log) addCommand(command interface{}, term int) int {
	raftLog.mu.Lock()
	defer raftLog.mu.Unlock()

	idx := len(raftLog.entries)
	entry := LogEntry{
		Command:  command,
		Term:     term,
		LogIndex: idx,
	}
	raftLog.entries = append(raftLog.entries, entry)
	DPrintf("leader %v add command %v, current log: %s", raftLog.me, command, raftLog)
	return idx
}

func (raftLog *Log) String() string {
	s := "["
	for i := 1; i < len(raftLog.entries); i++ {
		s += strconv.Itoa(raftLog.entries[i].Command.(int)) + ","
	}
	s += "]"
	return s
}

func (raftLog *Log) appendLogs(entries []LogEntry) {
	raftLog.mu.Lock()
	defer raftLog.mu.Unlock()

	if len(entries) == 0 {
		return
	}

	cr := len(raftLog.entries)
	ce := 0
	for i := 0; i < len(entries); i++ {
		index, term := entries[i].LogIndex, entries[i].Term
		if index >= len(raftLog.entries) {
			break
		} else if raftLog.entries[index].Term != term {
			cr = index
			ce = i
			break
		}
	}
	raftLog.entries = append(raftLog.entries[:cr], entries[ce:]...)
}

// server state
type ServState uint

const (
	Candidate ServState = 0
	Leader              = 1
	Follower            = 2
)

//request type
type RequestType string

const (
	VoteReq   RequestType = "voteReq"
	AppendReq             = "appendReq"
)

//reply type
type ReplyType string

const (
	VoteReply   ReplyType = "voteReply"
	AppendReply           = "appendReply"
)

type ReplyMsg struct {
	Type     ReplyType
	Term     int
	Success  bool
	Server   int
	LogIndex int //server end msg
}

type ProcType string

const (
	Republic ProcType = "republicLog"
)

type ProcMsg struct {
	Type     ProcType
	logIndex int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here (2A).

	term = rf.currentTerm
	isLeader = rf.state == Leader
	return term, isLeader
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm { //$5.1 first check
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm { //$5.1
		rf.becomeFollower(args.Term, None)
		rf.preVotedFor = None
	}

	if rf.preVotedFor == None &&
		rf.state != Candidate &&
		!rf.logs.upToDateThan(args.LastLogTerm, args.LastLogIndex) { //no vote for other candidate

		rf.preVotedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
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

//append entries rpc request
type AppendEntriesRequest struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type LogEntry struct {
	Command  interface{}
	Term     int
	LogIndex int
}

//append entries rpc response
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// handle append entries request
func (rf *Raft) AppendEntries(args *AppendEntriesRequest, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm { //$5.1
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm { //$5.1
		rf.becomeFollower(args.Term, args.LeaderId)
	}

	if !rf.logs.match(args.PrevLogIndex, args.PrevLogTerm) { //$5.3, log inconsistency
		reply.Term = rf.currentTerm
		reply.Success = false

		rf.becomeFollower(args.Term, args.LeaderId)
	} else {
		rf.logs.appendLogs(args.Entries)
		rf.logs.updateCommit(args.LeaderCommit, LastEntry(rf.logs.entries).LogIndex, rf.applyChan)

		rf.becomeFollower(args.Term, args.LeaderId)

		reply.Term = rf.currentTerm
		reply.Success = true
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesRequest, reply *AppendEntriesReply) bool {
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
	isLeader := true

	// Your code here (2B).
	isLeader = rf.state == Leader
	if !isLeader {
		return -1, -1, false
	} else {
		DPrintf("leader %v receive command %v", rf.me, command)
		term := rf.currentTerm
		logIdx := rf.logs.addCommand(command, term)
		rf.procc <- ProcMsg{
			Republic,
			logIdx,
		}
		return logIdx, term, true
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	close(rf.stopc)
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
	rf.ticker = time.NewTicker(time.Millisecond)
	rf.electionTimeout = 500 + rand.Intn(200)
	rf.heartbeatTimeout = 100 + rand.Intn(10)
	rf.electionElapsed = 0
	rf.heartbeatElapsed = 0

	rf.stopc = make(chan struct{})
	rf.repc = make(chan ReplyMsg, 1)
	rf.procc = make(chan ProcMsg, 1)

	rf.applyChan = applyCh

	rf.leader = None
	rf.quorum = len(rf.peers)/2 + 1 //majority
	rf.preVotedFor = None
	rf.currentTerm = 0

	//flag log entry
	emptyEntry := LogEntry{
		Command:  nil,
		Term:     0,
		LogIndex: 0,
	}
	rf.logs = &Log{
		mu:          new(sync.Mutex),
		entries:     []LogEntry{emptyEntry},
		commitIndex: 0,
		lastApplied: 0,
		me:          rf.me,
	}

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.run()

	return rf
}

// main loop
func (rf *Raft) run() {
	rf.becomeFollower(0, None)
	for {
		select {
		case <-rf.ticker.C:
			rf.tickMu.Lock()
			rf.tick()
			rf.tickMu.Unlock()

		case msg := <-rf.repc:
			rf.StepReply(msg)

		case msg := <-rf.procc:
			rf.StepProc(msg)

		case <-rf.stopc:
			close(rf.applyChan)
			return
		}
	}
}

func (rf *Raft) becomeFollower(term int, leader int) {
	rf.leader = leader
	rf.state = Follower
	rf.currentTerm = term
	rf.tick = rf.electionTick
	rf.resetElect()
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate

	rf.mu.Lock()
	rf.currentTerm += 1
	rf.preVotedFor = rf.me
	rf.leader = None
	rf.myVote = 1
	rf.mu.Unlock()
	rf.tick = rf.electionTick

	go rf.election()
}

func (rf *Raft) becomeLeader() {
	DPrintf("server %v become leader in term: %v", rf.me, rf.currentTerm)
	rf.state = Leader
	rf.tick = rf.heartbeatTick
	rf.leader = rf.me
	rf.resetHeartbeat()

	nextIdx := len(rf.logs.entries)
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = nextIdx //leader contains most commit entries
		rf.matchIndex[i] = 0
	}
}

//reset current election elapsed to 0 to defer become candidate
func (rf *Raft) resetElect() {
	rf.tickMu.Lock()
	defer rf.tickMu.Unlock()

	rf.electionElapsed = 0
}

//reset current hbElapsed to timeout trigger first heartbeat
func (rf *Raft) resetHeartbeat() {
	rf.tickMu.Lock()
	defer rf.tickMu.Unlock()

	rf.heartbeatElapsed = rf.heartbeatTimeout
}

func (rf *Raft) electionTick() {
	rf.electionElapsed++

	if rf.state == Leader {
		return
	}

	if rf.electionElapsed > rf.electionTimeout {
		rf.electionElapsed = 0
		rf.electionTimeout = 500 + rand.Intn(200)
		rf.becomeCandidate()
	}
}

func (rf *Raft) heartbeatTick() {
	rf.heartbeatElapsed++

	if rf.state != Leader {
		return
	}

	if rf.heartbeatElapsed > rf.heartbeatTimeout {
		rf.heartbeatElapsed = 0
		go rf.heartbeat()
	}
}

func (rf *Raft) StepReply(msg ReplyMsg) {
	if msg.Term > rf.currentTerm {
		rf.becomeFollower(msg.Term, None) //$5.1
		return
	}

	if msg.Term < rf.currentTerm {
		return
	}

	switch msg.Type {
	case VoteReply:
		if msg.Success {
			rf.myVote += 1
			if rf.myVote == rf.quorum {
				rf.becomeLeader()
			}
			rf.resetElect()
		}

	case AppendReply:
		if msg.Success {
			rf.matchIndex[msg.Server] = msg.LogIndex
			rf.nextIndex[msg.Server] = msg.LogIndex + 1
			count := 0
			for idx := 0; idx < len(rf.peers); idx++ {
				if rf.matchIndex[idx] >= msg.LogIndex {
					count += 1
				}
			}
			if count == rf.quorum {
				commit := Max(msg.LogIndex, rf.logs.commitIndex)
				if commit > rf.logs.commitIndex {
					DPrintf("leader %v update commit to %v", rf.me, msg.LogIndex)
					rf.logs.updateCommit(commit, commit, rf.applyChan)
				}
			}
		} else {
			DPrintf("leader %v received from %v because inconsistency log, leader logIndex %v, follower commit index: %v",
				rf.me, msg.Server, len(rf.logs.entries)-1, msg.LogIndex)
			rf.nextIndex[msg.Server] = rf.nextIndex[msg.Server] - 1
			rf.send(msg.Server, AppendReq)
		}
	default:
		DPrintf("error msg type")
	}
}

func (rf *Raft) StepProc(msg ProcMsg) {
	switch msg.Type {
	case Republic:
		DPrintf("leader %v republic log, logIndex: %v", rf.me, len(rf.logs.entries)-1)
		rf.matchIndex[rf.me] = len(rf.logs.entries) - 1
		rf.nextIndex[rf.me] = len(rf.logs.entries)
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				rf.send(i, AppendReq)
			}
		}
	default:
		DPrintf("error msg type")
	}
}

// begin elect
func (rf *Raft) election() {
	DPrintf("server %v start election, term: %v", rf.me, rf.currentTerm)

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.send(i, VoteReq)
		}
	}
}

//heart beat
func (rf *Raft) heartbeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.send(i, AppendReq)
		}
	}
}

func (rf *Raft) send(server int, reqType RequestType) {
	if server == rf.me {
		return
	}
	switch reqType {
	case VoteReq:
		go func() {
			if rf.state != Candidate {
				return
			}

			lastEntry := LastEntry(rf.logs.entries)
			idx, term := lastEntry.LogIndex, lastEntry.Term
			req := &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: idx,
				LastLogTerm:  term,
			}

			reply := &RequestVoteReply{}

			if rf.sendRequestVote(server, req, reply) {
				rf.repc <- ReplyMsg{
					Type:    VoteReply,
					Term:    reply.Term,
					Success: reply.VoteGranted,
					Server:  server,
				}
			}
		}()

	case AppendReq:
		go func() {
			if rf.state != Leader {
				return
			}

			lastLog := rf.logs.entries[rf.nextIndex[server]-1]
			end := len(rf.logs.entries)
			req := &AppendEntriesRequest{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: lastLog.LogIndex,
				PrevLogTerm:  lastLog.Term,
				Entries:      rf.logs.entries[rf.nextIndex[server]:end],
				LeaderCommit: rf.logs.commitIndex,
			}

			reply := &AppendEntriesReply{}

			if rf.sendAppendEntries(server, req, reply) {
				rf.repc <- ReplyMsg{
					Type:     AppendReply,
					Term:     reply.Term,
					Success:  reply.Success,
					Server:   server,
					LogIndex: end - 1,
				}
			}
		}()
	}
}
