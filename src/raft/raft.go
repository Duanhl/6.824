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
	voteCount  int

	hasNewEntry bool

	recv    chan Message     // local call, rpc handle, rpc response handle
	errC    chan Message     // rpc call failed handle
	tc      chan interface{} // lock event handle
	applyCh chan ApplyMsg
	closeCh chan interface{}

	tickFunc func(rf *Raft)
	stepFunc func(rf *Raft, msg Message) error

	// local lock
	elapsed  int
	deadline int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	ch := make(chan Message)
	rf.recv <- Message{MType: GetState, replyC: ch}
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
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	ch := make(chan Message)
	rf.recv <- Message{
		MType:    CondInstallSnapshot,
		LogIndex: lastIncludedIndex,
		LogTerm:  lastIncludedTerm,
		Snapshot: snapshot,
		replyC:   ch,
	}
	select {
	case msg := <-ch:
		return msg.Success
	case <-rf.closeCh:
		return false
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	ch := make(chan Message)
	rf.recv <- Message{
		MType:    DoSnapshot,
		LogIndex: index,
		Snapshot: snapshot,
		replyC:   ch,
	}
	select {
	case <-ch:
	case <-rf.closeCh:
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
	ch := make(chan Message)
	rf.recv <- Message{
		MType:    VoteRequest,
		Server:   args.CandidateId,
		LogIndex: args.LastLogIndex,
		LogTerm:  args.LastLogTerm,
		Term:     args.Term,
		replyC:   ch,
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
	lastEntry := rf.rsm.LastEntry()
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
				MType:   VoteResponse,
				Server:  server,
				Term:    reply.Term,
				Success: reply.VoteGranted,
			}
		} else {
			rf.errC <- Message{MType: VoteRequest, Server: server, Term: arg.Term}
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
		MType:        AppendRequest,
		Server:       args.LeaderId,
		LogIndex:     args.PrevLogIndex,
		LogTerm:      args.PrevLogTerm,
		Term:         args.Term,
		Entries:      args.Entries,
		LeaderCommit: args.LeaderCommit,
		replyC:       ch,
	}
	select {
	case msg := <-ch:
		reply.Term = msg.Term
		reply.Success = msg.Success
		reply.LastLogIndex = msg.LogIndex
		reply.LastLogTerm = msg.LogTerm
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
		LeaderCommit: rf.rsm.committed,
	}
	reply := &AppendEntriesReply{}
	go func() {
		ok := rf.sendAppendEntries(server, args, reply)
		if ok {
			rf.recv <- Message{
				MType:    AppendResponse,
				Server:   server,
				Term:     reply.Term,
				LogIndex: reply.LastLogIndex,
				LogTerm:  reply.LastLogTerm,
				Success:  reply.Success,
			}
		} else {
			rf.errC <- Message{MType: AppendRequest, Server: server, Term: args.Term}
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
		MType:    InstallSnapshotRequest,
		Server:   args.LeaderId,
		Term:     args.Term,
		LogIndex: args.LastIncludedIndex,
		LogTerm:  args.LastIncludedTerm,
		Snapshot: args.Snapshot,
	}
	select {
	case msg := <-ch:
		reply.Term = msg.Term
		reply.LastLogIndex = msg.LogIndex
	case <-rf.closeCh:
	}
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
		MType:  AppendCommand,
		Val:    command,
		replyC: rc,
	}
	select {
	case msg := <-rc:
		return msg.LogIndex, msg.LogTerm, msg.Success
	case <-rf.closeCh:
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
			rf.tickFunc(rf)
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

func (rf *Raft) vote(server int) {
	rf.votedFor = server
	rf.persist()
	// reset local lock
	rf.elapsed = 0
}

func (rf *Raft) stepMessage(msg Message) error {
	if msg.Term == 0 {
		// localMessage
		rf.stepLocalMessage(msg)
		return nil
	}

	if msg.Term > rf.currentTerm {
		rf.becomeFollower(msg.Term)
	} else if msg.Term < rf.currentTerm {
		rf.reject(msg)
		return nil
	}

	if msg.MType == AppendRequest && rf.state == Candidate {
		rf.becomeFollower(msg.Term)
	}

	return rf.stepFunc(rf, msg)
}

func (rf *Raft) stepLocalMessage(msg Message) {
	switch msg.MType {
	case AppendCommand:
		if rf.state == Leader {
			logIndex := rf.rsm.Append(rf.currentTerm, msg.Val)
			rf.persist()
			rf.hasNewEntry = true
			rf.matchIndex[rf.me] = logIndex
			msg.replyC <- Message{LogIndex: logIndex, LogTerm: rf.currentTerm, Success: true}
		} else {
			msg.replyC <- Message{Success: false}
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
		log.Fatalf("unexcepted message type: [%s] for local message", MessageTypeMap[msg.MType])
	}
}

func (rf *Raft) reject(msg Message) {
	if msg.MType == VoteRequest || msg.MType == AppendRequest || msg.MType == InstallSnapshotRequest {
		msg.replyC <- Message{
			Term:    rf.currentTerm,
			Success: false,
		}
	}
	// response ignore
}

func (rf *Raft) handleError(msg Message) {
	if msg.Term == rf.currentTerm {
		if msg.MType == VoteRequest && rf.state == Candidate {
			rf.requestVoteLoop(msg.Server)
		}
		if msg.MType == AppendRequest && rf.state == Leader {
			rf.appendEntriesLoop(msg.Server)
		}
	}
}

func (rf *Raft) doHeartbeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.appendEntriesLoop(i)
		}
	}
	rf.elapsed = 0
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

	lastEntry := rf.rsm.LastEntry()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			rf.matchIndex[i] = 0
		} else {
			rf.matchIndex[i] = lastEntry.Index
		}
		rf.nextIndex[i] = lastEntry.Index + 1
	}

	rf.tickFunc = leaderTick
	rf.stepFunc = stepLeader
}

func (rf *Raft) becomeCandidate() {
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.persist()

	rf.state = Candidate
	rf.deadline = rand.Intn(ElecDeadlineBase) + ElecDeadlineBase
	rf.elapsed = 0
	rf.tickFunc = candidateTick
	rf.stepFunc = stepCandidate

	rf.startElection()
}

func (rf *Raft) becomeFollower(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
	}

	rf.state = Follower
	rf.deadline = rand.Intn(ElecDeadlineBase) + ElecDeadlineBase
	rf.elapsed = 0

	rf.tickFunc = followerTick
	rf.stepFunc = stepFollower
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

	rf.recv = make(chan Message, 128)
	rf.errC = make(chan Message, 128)
	rf.tc = make(chan interface{}, 128)
	rf.applyCh = make(chan ApplyMsg, 128)
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

func followerTick(rf *Raft) {
	rf.elapsed++
	if rf.elapsed > rf.deadline {
		rf.becomeCandidate()
	}
}

func candidateTick(rf *Raft) {
	rf.elapsed++
	if rf.elapsed > rf.deadline {
		rf.becomeCandidate()
	}
}

func leaderTick(rf *Raft) {
	rf.elapsed++
	if rf.hasNewEntry || rf.elapsed >= rf.deadline {
		rf.doHeartbeat()
		rf.hasNewEntry = false
	}
}

func stepLeader(rf *Raft, msg Message) error {
	switch msg.MType {
	case AppendResponse:
		if msg.Success {
			if msg.LogIndex > rf.matchIndex[msg.Server] {
				rf.matchIndex[msg.Server] = msg.LogIndex
				rf.nextIndex[msg.Server] = msg.LogIndex + 1
				mayCommit := Majority(rf.matchIndex)
				e, err := rf.rsm.EntryAt(mayCommit)
				if err != nil && e.Term == rf.currentTerm && rf.rsm.Commit(mayCommit) {
					rf.doHeartbeat()
				}
			}
		} else {
			rf.nextIndex[msg.Server] -= 1
			rf.appendEntriesLoop(msg.Server)
		}
		break
	case VoteRequest, VoteResponse:
		break
	default:
		return errors.New(fmt.Sprintf("unsupported msg type [%v] for leader", MessageTypeMap[msg.MType]))
	}
	return nil
}

func stepFollower(rf *Raft, msg Message) error {
	switch msg.MType {
	case VoteRequest:
		if rf.votedFor == -1 && !rf.rsm.UpToDate(msg.LogTerm, msg.LogIndex) {
			rf.vote(msg.Server)
			msg.replyC <- Message{Success: true, Term: rf.currentTerm}
		} else {
			rf.reject(msg)
		}
		break
	// transfer from follower
	case VoteResponse:
		break
	case AppendRequest:
		rf.elapsed = 0
		ok, lastIndex := rf.rsm.Compact(msg.LogTerm, msg.LogIndex, msg.Entries)
		if ok {
			msg.replyC <- Message{Success: true, LogIndex: lastIndex, Term: rf.currentTerm}
			mayCommitIndex := Min(msg.LeaderCommit, lastIndex)
			rf.rsm.Commit(mayCommitIndex)
		} else {
			msg.replyC <- Message{Success: false, Term: rf.currentTerm}
		}
		break
	// transfer from leader and receive new response
	case AppendResponse:
		break
	default:
		return errors.New(fmt.Sprintf("unsupported msg type [%v] for follower", MessageTypeMap[msg.MType]))
	}
	return nil
}

func stepCandidate(rf *Raft, msg Message) error {
	switch msg.MType {
	case VoteRequest:
		rf.reject(msg)
		break
	case VoteResponse:
		if msg.Success {
			rf.voteCount += 1
			rf.elapsed = 0
			if rf.voteCount > len(rf.peers)/2 {
				rf.becomeLeader()
			}
		}
		break
	default:
		return errors.New(fmt.Sprintf("unsupported msg type [%v] for candidate", MessageTypeMap[msg.MType]))
	}
	return nil
}
