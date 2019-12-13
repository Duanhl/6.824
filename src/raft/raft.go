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
	log         []*LogEntry

	//all server
	commitIndex int
	lastApplied int
	stopCh      chan bool
	replyCh     chan *ReplyMsg
	applyChan   chan int

	//candidate
	myVote int

	//leader
	newIndex   []int
	matchIndex []int

	ticker           *time.Ticker
	tick             func()
	tickMu           sync.Mutex
	electionElapsed  int
	heartbeatElapsed int
	electionTimeout  int
	heartbeatTimeout int
}

type ServState uint

const (
	Candidate ServState = 0
	Leader              = 1
	Follower            = 2
)

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
		rf.preVotedFor = -1
		rf.becomeFollower(args.Term)
	}

	if rf.preVotedFor == -1 { //no vote for other candidate
		var lastEntry *LogEntry
		if len(rf.log) == 0 {
			lastEntry = nil
		} else {
			lastEntry = rf.log[len(rf.log)-1]
		}

		if lastEntry == nil || args.LastLogTerm > lastEntry.Term || //$5.2 5.4
			(args.LastLogTerm == lastEntry.Term && args.LastLogIndex >= len(rf.log)-1) {
			rf.preVotedFor = args.CandidateId
			rf.resetElect()

			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			return
		}
	}
	reply.VoteGranted = false
	reply.Term = rf.currentTerm
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
	Entries      []*LogEntry
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

type ReplyType string

const (
	VoteReply   ReplyType = "voteReply"
	AppendReply           = "appendReply"
)

type ReplyMsg struct {
	Type      ReplyType
	Term      int
	Success   bool
	Server    int
	NextIndex int
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
		rf.leader = args.LeaderId
		rf.becomeFollower(args.Term)
	}

	if args.PrevLogIndex > -1 && //$5.3 consistency check
		(len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.resetElect()

	conflictSrc, conflictDest := -1, -1 //$5.3 conflict check
	for j := 0; j < len(args.Entries); j++ {
		entry := args.Entries[j]
		idx := entry.LogIndex
		if idx < len(rf.log) && rf.log[idx].Term != entry.Term {
			conflictSrc = idx
			conflictDest = j
			break
		}
	}
	if conflictSrc > -1 {
		rf.log = append(rf.log[:conflictSrc], args.Entries[conflictDest:]...)
	} else {
		rf.log = append(rf.log, args.Entries...)
	}

	if args.LeaderCommit > rf.commitIndex {
		var lastEntry *LogEntry
		if args.Entries != nil {
			lastEntry = args.Entries[len(args.Entries)-1]
		} else {
			lastEntry = nil
		}
		if lastEntry == nil {
			rf.commitIndex = args.LeaderCommit
		} else {
			if args.LeaderCommit > lastEntry.LogIndex {
				rf.commitIndex = lastEntry.LogIndex
			} else {
				rf.commitIndex = args.LeaderCommit
			}
		}

		DPrintf("server %v commitIndex update to %v", rf.me, rf.commitIndex)
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	return
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	isLeader = rf.state == Leader
	if !isLeader {
		return -1, -1, false
	}

	index = rf.commitIndex + 1
	term = rf.currentTerm
	rf.replicateLog(command)

	return index, term, isLeader
}

func (rf *Raft) replicateLog(command interface{}) {
	if rf.state != Leader {
		return
	}

	entry := &LogEntry{
		Command:  command,
		Term:     rf.currentTerm,
		LogIndex: len(rf.log),
	}
	rf.log = append(rf.log, entry)
	rf.newIndex[rf.me] = len(rf.log)

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				rf.appendLogs(server)
			}(i)
		}
	}
}

func (rf *Raft) appendLogs(server int) {
	if server == rf.me {
		return
	}

	prevLogIndex := -1
	prevLogTerm := 0
	if rf.newIndex[server] > 0 {
		prevLogIndex = rf.newIndex[server] - 1
		prevLogTerm = rf.log[rf.newIndex[server]-1].Term
	}

	req := &AppendEntriesRequest{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      rf.log[rf.newIndex[server]:],
		LeaderCommit: rf.commitIndex,
	}

	reply := &AppendEntriesReply{}
	if rf.sendAppendEntries(server, req, reply) {
		rf.replyCh <- &ReplyMsg{
			Type:      AppendReply,
			Term:      reply.Term,
			Success:   reply.Success,
			Server:    server,
			NextIndex: len(rf.log),
		}
	} else {
		time.AfterFunc(time.Millisecond*150, func() {
			rf.appendLogs(server)
		})
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

	rf.stopCh = make(chan bool)
	rf.replyCh = make(chan *ReplyMsg)
	rf.applyChan = make(chan int)

	rf.leader = -1
	rf.quorum = len(rf.peers)/2 + 1 //majority
	rf.preVotedFor = -1
	rf.currentTerm = 0

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.newIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.mainLoop()
	go rf.applyCommand()

	return rf
}

// main loop
func (rf *Raft) mainLoop() {
	rf.becomeFollower(0)
	for {
		select {
		case <-rf.ticker.C:
			rf.tickMu.Lock()
			rf.tick()
			rf.tickMu.Unlock()
		case msg := <-rf.replyCh:
			rf.Step(msg)
		case <-rf.stopCh:
			rf.Kill()
			return
		}
	}
}

//apply command to state machine should in one gorouting
func (rf *Raft) applyCommand() {
	for {
		select {
		case <-rf.applyChan:
			if rf.lastApplied < rf.commitIndex {
				//do apply command
				rf.lastApplied = rf.commitIndex
			}
		}
	}
}

func (rf *Raft) becomeFollower(term int) {
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
	rf.leader = -1
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

	for i := 0; i < len(rf.peers); i++ {
		rf.newIndex[i] = len(rf.log) //leader contains most commit log
		rf.matchIndex[i] = -1
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

func (rf *Raft) Step(reply *ReplyMsg) {
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term) //$5.1
	} else {
		switch reply.Type {
		case VoteReply:
			if reply.Success {
				rf.myVote += 1
			}
			rf.resetElect()
			if rf.myVote == rf.quorum {
				rf.becomeLeader()
			}
		case AppendReply:
			if reply.Success {
				rf.newIndex[reply.Server] = reply.NextIndex
				count := 0
				for idx := 0; idx < len(rf.peers); idx++ {
					if rf.newIndex[idx] >= reply.NextIndex {
						count += 1
					}
				}
				if count == rf.quorum {
					if reply.NextIndex-1 > rf.commitIndex {
						rf.commitIndex = reply.NextIndex - 1
					}
				}
				if reply.NextIndex < len(rf.log) {
					go rf.appendLogs(reply.Server)
				}
			}
		default:
			DPrintf("error msg type")
		}
	}
}

// begin elect
func (rf *Raft) election() {
	DPrintf("server %v start election, term: %v", rf.me, rf.currentTerm)

	lastLogIndex := -1
	lastLogTerm := 0
	if len(rf.log) > 0 {
		lastLogIndex = rf.log[len(rf.log)-1].LogIndex
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	voteArg := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int, args *RequestVoteArgs) {
				reply := &RequestVoteReply{}
				if rf.sendRequestVote(server, voteArg, reply) {
					rf.replyCh <- &ReplyMsg{
						Type:    VoteReply,
						Term:    reply.Term,
						Success: reply.VoteGranted, //ignore
						Server:  server,
					}
				}
			}(i, voteArg)
		}
	}
}

func (rf *Raft) heartbeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.appendLogs(i)
		}
	}
}
