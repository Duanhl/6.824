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
//   each time a new entry is Committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"bytes"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// Committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// Committed log entry.
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

type RaftState int

const (
	Leader    RaftState = 3
	Candidate           = 2
	Follower            = 1
)

var StateStringMap = map[RaftState]string{
	Leader:    "Leader",
	Candidate: "Candidate",
	Follower:  "Follower",
}

const TickUnit = 5 * time.Millisecond
const HBDeadlineBase = 15
const ElecDeadlineBase = 40

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
	rsm         *Rsm
	currentTerm int
	votedFor    int

	state RaftState

	nextIndex  []int
	matchIndex []int
	voteForMe  int

	recv    chan Message     // local call, rpc handle, rpc response handle
	errC    chan ErrorEvent  // rpc call failed handle
	tc      chan interface{} // lock event handle
	applyCh chan ApplyMsg
	closeCh chan interface{}

	// local lock
	elapsed  int
	deadline int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	ch := make(chan Message)
	rf.recv <- Message{Type: GetState, Term: -1, replyC: ch}
	select {
	case msg := <-ch:
		return msg.Term, msg.Success
	case <-rf.closeCh:
		return 0, false
	}
}

func (rf *Raft) RaftStateSize() int {
	return -1
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	hardRsm, sn := rf.rsm.getState()
	e.Encode(hardRsm)
	rf.persister.SaveStateAndSnapshot(w.Bytes(), sn)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte, sn []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voteFor int
	var hardRsm HardRsm
	if d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&hardRsm) != nil {
		log.Fatalln("decode persist state error")
	} else {
		rf.currentTerm = term
		rf.votedFor = voteFor
		rf.rsm.Initialize(hardRsm, sn)
	}
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
	ch := make(chan Message)
	rf.recv <- Message{
		Type:         CondInstallSnapshot,
		LastLogIndex: lastIncludedIndex,
		LastLogTerm:  lastIncludedTerm,
		Snapshot:     snapshot,
		replyC:       ch,
	}
	return (<-ch).Success
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	ch := make(chan Message)
	rf.recv <- Message{
		Type:         DoSnapshot,
		LastLogIndex: index,
		Term:         -1,
		Snapshot:     snapshot,
		replyC:       ch,
	}
	<-ch
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
	ch := make(chan Message)
	rf.recv <- Message{
		Type:         VoteRequest,
		From:         args.CandidateId,
		LastLogIndex: args.LastLogIndex,
		LastLogTerm:  args.LastLogTerm,
		Term:         args.Term,
		replyC:       ch,
	}
	select {
	case msg := <-ch:
		reply.Term = msg.Term
		reply.VoteGranted = msg.Success
		return
	case <-rf.closeCh:
		return
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

func (rf *Raft) requestVoteLoop(server int) {
	lastEntry, _ := rf.rsm.LastEntry()
	arg := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastEntry.Index,
		LastLogTerm:  lastEntry.Term,
	}
	reply := &RequestVoteReply{}
	go func() {
		ok := rf.sendRequestVote(server, arg, reply)
		if ok {
			rf.recv <- Message{
				Type:    VoteResponse,
				From:    server,
				Term:    reply.Term,
				Success: reply.VoteGranted,
			}
		} else {
			rf.errC <- ErrorEvent{MType: VoteRequest, Server: server, Term: arg.Term}
		}
	}()
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
	ch := make(chan Message)
	rf.recv <- Message{
		Type:         AppendRequest,
		From:         args.LeaderId,
		LastLogIndex: args.PrevLogIndex,
		LastLogTerm:  args.PrevLogTerm,
		Term:         args.Term,
		Entries:      args.Entries,
		LeaderCommit: args.LeaderCommit,
		replyC:       ch,
	}
	select {
	case msg := <-ch:
		reply.Term = msg.Term
		reply.Success = msg.Success
		reply.LastLogIndex = msg.LastLogIndex
		reply.LastLogTerm = msg.LastLogTerm
		return
	case <-rf.closeCh:
		return
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) appendEntriesLoop(server int) {
	entries, prevTerm, prevIndex, err := rf.rsm.EntryFrom(rf.nextIndex[server], 10)
	if err != nil {
		log.Fatalf("leader %v has no entry from %v", rf.me, rf.nextIndex[server])
	}
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rf.rsm.CommitIndex(),
	}
	reply := &AppendEntriesReply{}
	go func() {
		ok := rf.sendAppendEntries(server, args, reply)
		if ok {
			rf.recv <- Message{
				Type:         AppendResponse,
				From:         server,
				Term:         reply.Term,
				LastLogIndex: reply.LastLogIndex,
				LastLogTerm:  reply.LastLogTerm,
				Success:      reply.Success,
			}
		} else {
			rf.errC <- ErrorEvent{MType: AppendRequest, Server: server, Term: args.Term}
		}
	}()
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type InstallSnapshotReply struct {
	Term         int
	LastLogIndex int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ch := make(chan Message)
	rf.recv <- Message{
		Type:         InstallSnapshotRequest,
		From:         args.LeaderId,
		Term:         args.Term,
		LastLogIndex: args.LastIncludedIndex,
		LastLogTerm:  args.LastIncludedTerm,
		Snapshot:     args.Snapshot,
	}
	msg := <-ch
	reply.Term = msg.Term
	reply.LastLogIndex = msg.LastLogIndex
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be Committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever Committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rc := make(chan Message)
	rf.recv <- Message{
		Type:   AppendCommand,
		Val:    command,
		Term:   -1,
		replyC: rc,
	}
	msg := <-rc

	return msg.LastLogIndex, msg.LastLogTerm, msg.Success
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
	close(rf.closeCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	ticker := time.NewTicker(TickUnit)
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-ticker.C:
			rf.tc <- struct{}{}
		}
	}
}

func (rf *Raft) mainLoop() {
	for rf.killed() == false {
		select {
		case <-rf.tc:
			rf.handleTick()
		case msg := <-rf.recv:
			err := rf.stepMessage(msg)
			if err != nil {
				log.Fatalln(err)
			}
		case event := <-rf.errC:
			rf.handleError(event)
		}
	}
}

func (rf *Raft) apply(applyCh chan ApplyMsg) {
	for rf.killed() == false {
		select {
		case msg := <-rf.applyCh:
			applyCh <- msg
		}
	}
}

func (rf *Raft) stepMessage(msg Message) error {
	if msg.Term == -1 {
		// local message
		rf.stepLocalMessage(msg)
		return nil
	} else if msg.Term < rf.currentTerm {
		rf.reject(msg)
		return nil
	} else if msg.Term > rf.currentTerm {
		rf.becomeFollower(msg.Term)
	}

	// always msg.Term == rf.currentTerm
	if msg.Type == AppendRequest && rf.state == Candidate {
		rf.becomeFollower(msg.Term)
	}

	switch rf.state {
	case Follower:
		return rf.stepFollower(msg)
	case Candidate:
		return rf.stepCandidate(msg)
	case Leader:
		return rf.stepLeader(msg)
	default:
		return errors.New("unexcepted raft state")
	}
}

func (rf *Raft) stepFollower(msg Message) error {
	switch msg.Type {
	case VoteRequest:
		if rf.votedFor == -1 && !rf.rsm.UpToDate(msg.LastLogTerm, msg.LastLogIndex) {
			//DPrintf("server %v vote for %v in term %v", rf.me, msg.From, rf.currentTerm)
			rf.voteForOther(msg.From)
			msg.replyC <- Message{Type: VoteResponse, Term: rf.currentTerm, Success: true}
		} else {
			rf.reject(msg)
		}
		return nil
	case VoteResponse:
		return nil
	case AppendRequest:
		rf.elapsed = 0
		ok, lastIndex := rf.rsm.Compact(msg.LastLogTerm, msg.LastLogIndex, msg.Entries)
		if !ok {
			msg.replyC <- Message{Term: rf.currentTerm, Success: false}
		} else {
			mayCommit := Min(msg.LeaderCommit, lastIndex)
			if rf.rsm.Commit(mayCommit) {
				rf.elapsed = rf.deadline
				DPrintf("follower %v commit to %v", rf.me, mayCommit)
			}
			rf.persist()
			msg.replyC <- Message{Term: rf.currentTerm, Success: true, LastLogIndex: lastIndex}
		}
		return nil
	case AppendResponse:
		// pass
		return nil
	default:
		return nil
	}
}

func (rf *Raft) voteForOther(server int) {
	rf.votedFor = server
	rf.persist()
	// reset local lock
	rf.elapsed = 0
}

func (rf *Raft) stepLeader(msg Message) error {
	switch msg.Type {
	case VoteRequest:
		rf.reject(msg)
		return nil
	case VoteResponse:
		// pass, do nothing
		return nil
	case AppendResponse:
		if msg.Success {
			rf.matchIndex[msg.From] = Max(rf.matchIndex[msg.From], msg.LastLogIndex)
			rf.nextIndex[msg.From] = rf.matchIndex[msg.From] + 1
			// raft commit rule 5.3, 5.4
			maybeCommit := Majority(rf.matchIndex)
			entry, err := rf.rsm.EntryAt(maybeCommit)
			if err != nil {
				return errors.New(fmt.Sprintf("leader %v has not entry exists in %v", rf.me, maybeCommit))
			}
			if entry.Term == rf.currentTerm {
				if rf.rsm.Commit(maybeCommit) {
					DPrintf("leader %v commit to %v", rf.me, maybeCommit)
				}
			}
		} else {
			rf.nextIndex[msg.From] -= 1
			if rf.nextIndex[msg.From] == rf.matchIndex[msg.From] {
				return errors.New(fmt.Sprintf("invalid state for server %v: [%v, %v]", msg.From,
					rf.matchIndex[msg.From], rf.nextIndex[msg.From]))
			}
			rf.appendEntriesLoop(msg.From)
		}
		return nil
	default:
		return errors.New(fmt.Sprintf("unexcepted msgType [%s] for Leader", MessageTypeMap[msg.Type]))
	}
}

func (rf *Raft) stepCandidate(msg Message) error {
	switch msg.Type {
	case VoteRequest:
		rf.reject(msg)
		return nil
	case VoteResponse:
		if msg.Success {
			rf.voteForMe += 1
			if rf.voteForMe > len(rf.peers)/2 {
				rf.becomeLeader()
			}
		}
		return nil
	default:
		return errors.New(fmt.Sprintf("unexcepted msgType [%s] for Candidate", MessageTypeMap[msg.Type]))
	}
}

func (rf *Raft) stepLocalMessage(msg Message) {
	switch msg.Type {
	case AppendCommand:
		if rf.state == Leader {
			logIndex := rf.rsm.Append(rf.currentTerm, msg.Val)
			rf.persist()
			rf.matchIndex[rf.me] = logIndex
			msg.replyC <- Message{
				LastLogIndex: logIndex,
				LastLogTerm:  rf.currentTerm,
				Success:      true,
			}
		} else {
			msg.replyC <- Message{
				LastLogIndex: -1,
				LastLogTerm:  -1,
				Success:      false,
			}
		}
		break
	case GetState:
		msg.replyC <- Message{
			Term:    rf.currentTerm,
			Success: rf.state == Leader,
		}
		break
	case DoSnapshot:
		break
	case CondInstallSnapshot:
		break
	default:
		log.Fatalf("unexcepted message type: [%s] for local message", MessageTypeMap[msg.Type])
	}
}

func (rf *Raft) reject(msg Message) {
	if msg.Type == VoteRequest || msg.Type == AppendRequest || msg.Type == InstallSnapshotRequest {
		msg.replyC <- Message{
			Term:    rf.currentTerm,
			Success: false,
		}
	}
	// response ignore
}

func (rf *Raft) handleError(event ErrorEvent) {
	if event.Term == rf.currentTerm {
		if event.MType == VoteRequest && rf.state == Candidate {
			rf.requestVoteLoop(event.Server)
		}
		if event.MType == AppendRequest && rf.state == Leader {
			rf.appendEntriesLoop(event.Server)
		}
	}
}

func (rf *Raft) handleTick() {
	rf.elapsed += 1
	if rf.elapsed >= rf.deadline {
		switch rf.state {
		case Follower:
			rf.becomeCandidate()
			rf.startElection()
			break
		case Candidate:
			rf.becomeCandidate()
			rf.startElection()
			break
		case Leader:
			rf.heartbeat()
			break
		}
		rf.elapsed = 0
		return
	}
	if rf.state == Leader && rf.rsm.hasUncommitted() {
		rf.heartbeat()
	}
}

func (rf *Raft) heartbeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.appendEntriesLoop(i)
		}
	}
}

func (rf *Raft) startElection() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.requestVoteLoop(i)
		}
	}
}

func (rf *Raft) becomeLeader() {
	DPrintf("server %v become leader in term %v", rf.me, rf.currentTerm)
	rf.state = Leader
	rf.deadline = rand.Intn(HBDeadlineBase) + HBDeadlineBase
	rf.elapsed = rf.deadline
	lastEntry, _ := rf.rsm.LastEntry()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.matchIndex[i] = 0
			rf.nextIndex[i] = lastEntry.Index + 1
		} else {
			rf.matchIndex[i] = lastEntry.Index
			rf.nextIndex[i] = lastEntry.Index + 1
		}
	}
}

func (rf *Raft) becomeCandidate() {
	//DPrintf("server %v become candidate in term %v", rf.me, rf.currentTerm+1)
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.voteForMe = 1
	rf.persist()
	rf.state = Candidate
	rf.deadline = rand.Intn(ElecDeadlineBase) + ElecDeadlineBase
}

func (rf *Raft) becomeFollower(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
	}
	rf.voteForMe = 0
	rf.state = Follower
	rf.deadline = rand.Intn(ElecDeadlineBase) + ElecDeadlineBase
	rf.elapsed = 0
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
	rf.rsm = &Rsm{
		sn:        nil,
		committed: 0,
		applied:   0,
		logs:      []Entry{{Command: nil, Term: 0, Index: 0}},
		applyFunc: func(entry Entry) {
			rf.applyCh <- ApplyMsg{CommandValid: true, Command: entry.Command, CommandIndex: entry.Index}
		},
	}
	rf.currentTerm = 1
	rf.votedFor = -1

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.recv = make(chan Message, 100)
	rf.errC = make(chan ErrorEvent, 100)
	rf.tc = make(chan interface{}, 100)
	rf.applyCh = make(chan ApplyMsg, 100)
	rf.closeCh = make(chan interface{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())
	rf.becomeFollower(rf.currentTerm)

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.mainLoop()
	go rf.apply(applyCh)

	return rf
}
