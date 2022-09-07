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
	"sync"
	"sync/atomic"
	"time"

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

type RaftState int

const (
	Leader    RaftState = 3
	Candidate           = 2
	Follower            = 1
)

const HBDeadlineBase = 30
const ElecDeadlineBase = 60

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

	recv chan Message
	tc   chan interface{}

	// local lock
	elapsed  int
	deadline int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	ch := make(chan Message)
	rf.recv <- Message{
		Type: GetState,
		Term: -1,
	}
	msg := <-ch
	return msg.Term, msg.Success
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	ticker := time.NewTicker(10)
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
			rf.stepMessage(msg)
		}
	}
}

func (rf *Raft) stepMessage(msg Message) {
	// local message
	if msg.Term == -1 {

		return
	}

	// remote message
	if msg.Term < rf.currentTerm {
		rf.reject(msg)
		return
	}

	if msg.Term > rf.currentTerm {
		rf.becomeFollower(msg.Term)
	}

	if msg.Type == VoteRequest {
		if rf.needVote(msg) {

		} else {
			rf.reject(msg)
		}
	}
}

func (rf *Raft) stepLocalMessage(msg Message) {
	if msg.Type == AppendCommand {
		if rf.state == Leader {
			logIndex := rf.rsm.Append(rf.currentTerm, msg.Val)
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
	} else if msg.Type == GetState {
		msg.replyC <- Message{
			Term:    rf.currentTerm,
			Success: rf.state == Leader,
		}
	} else if msg.Type == DoSnapshot {

	} else if msg.Type == CondInstallSnapshot {

	}
}

func (rf *Raft) needVote(msg Message) bool {
	return rf.votedFor == -1
}

func (rf *Raft) reject(msg Message) {
	if msg.Type == VoteRequest || msg.Type == AppendRequest || msg.Type == InstallSnapshotRequest {
		msg.replyC <- Message{
			Term:    rf.currentTerm,
			Success: false,
		}
	}
}

func (rf *Raft) handleTick() {
	rf.elapsed += 1
	if rf.elapsed >= rf.deadline {
		switch rf.state {
		case Follower:
		case Candidate:
			rf.becomeCandidate()
			rf.startElection()
			break
		case Leader:
			rf.heartbeat()
			break
		}
		rf.elapsed = 0
	}
}

func (rf *Raft) heartbeat() {

}

func (rf *Raft) startElection() {

}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.deadline = rand.Intn(HBDeadlineBase) + HBDeadlineBase
	rf.elapsed = rf.deadline
}

func (rf *Raft) becomeCandidate() {
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.persist()
	rf.state = Candidate
	rf.deadline = rand.Intn(ElecDeadlineBase) + ElecDeadlineBase
	rf.elapsed = rf.deadline
}

func (rf *Raft) becomeFollower(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.mainLoop()

	return rf
}
