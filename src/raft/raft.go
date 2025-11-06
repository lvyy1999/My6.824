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
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// the role in raft
const (
	RaftRoleLeader int = iota
	RaftRoleFollower
	RaftRoleCandidate
)

// the constant about time
const (
	HeartbeatInterval    = 100 // the interval of leader to send heartbeat, unit : milliseconds
	MinElectionTimeout   = 200 // the minimum of election timeout, unit : milliseconds
	SpanElectionTimeout  = 200 // the span of election timeout, unit : milliseconds
	CheckTimeoutInterval = 5   // the interval of follower to check if timeout, unit : milliseconds
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

type Log struct {
	Term    int
	Index   int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	role              int // the role of this peer in raft
	applyCh           chan ApplyMsg
	logCount          int   // the count of log entries (not include the initial one virtual log, so the last log is log[logCount])
	electionTimeout   int64 // the random election timeout, reset it in rf.resetElectionTimeout
	lastHeartbeatTime int64 // the time of last received heartbeat, update it in rf.resetElectionTimeout

	// Persistent state on all servers (Updated on stable storage before responding to RPCs):
	log         []Log // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	votedFor    int   // candidateId that received vote in current term (or null if none)
	currentTerm int   // latest term server has seen (initialized to 0 on first boot, increases monotonically)

	// Volatile state on all servers:
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// Volatile state on leaders (Reinitialized after election):
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

}

// If RPC request or response contains term T > currentTerm:
// set currentTerm = T, convert to follower, and reset voteFor
// Call this function when receive a request or response firstly.
func (rf *Raft) checkAndUpdateCurrentTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = term
		rf.role = RaftRoleFollower
	}
}

// will br called in three cases : receive leader's heartbeat; begin an election; vote for a candidate
// reset the electionTimeout and update the lastHeartbeatTime
func (rf *Raft) resetElectionTimeout() {
	atomic.StoreInt64(&rf.lastHeartbeatTime, time.Now().UnixMilli())
	atomic.StoreInt64(&rf.electionTimeout, int64(MinElectionTimeout+rand.Intn(SpanElectionTimeout)))
}

func (rf *Raft) checkElectionTimeout() bool {
	return time.Now().UnixMilli()-atomic.LoadInt64(&rf.electionTimeout) > atomic.LoadInt64(&rf.lastHeartbeatTime)
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.role == RaftRoleLeader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// restore previously persisted state.
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogTerm  int // term of candidate’s last log entry
	LastLogIndex int // index of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.checkAndUpdateCurrentTerm(args.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	lastLog := rf.log[rf.logCount]
	if args.Term < rf.currentTerm {
		// Reply false if term < currentTerm
		reply.VoteGranted = false
	} else if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		// Reply false if already voted for another candidate
		reply.VoteGranted = false
	} else if args.LastLogTerm > lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index) {
		// Then if candidate’s log is at least as up-to-date as receiver’s log, grant vote
		reply.VoteGranted = true
		rf.resetElectionTimeout()
		rf.votedFor = args.CandidateId
	}

	reply.Term = rf.currentTerm
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.checkAndUpdateCurrentTerm(reply.Term)
	}
	return ok
}

type AppendEntriesArgs struct {
	Term         int   // leader’s term
	Entries      []Log // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderId     int   // so follower can redirect clients
	PrevLogTerm  int   // index of log entry immediately preceding new ones
	PrevLogIndex int   // term of prevLogIndex entry
	LeaderCommit int   // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.checkAndUpdateCurrentTerm(args.Term)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//fmt.Println("server ", rf.me, " receive heartbeat from leader ", args.LeaderId,
	//	", args.Term = ", args.Term,
	//	", rf.currentTerm = ", rf.currentTerm)

	if args.Term < rf.currentTerm {
		// Reply false if term < currentTerm
		reply.Success = false
	} else {
		reply.Success = true
		rf.resetElectionTimeout()
	}

	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.checkAndUpdateCurrentTerm(reply.Term)
	}
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		Entries:      make([]Log, 0),
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	// Send heartbeat to all other servers asynchronously
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			reply := AppendEntriesReply{}
			go func(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
				ok := rf.sendAppendEntries(server, args, reply)
				if ok && !reply.Success {
					rf.mu.Lock()
					rf.role = RaftRoleFollower
					rf.mu.Unlock()
				}
			}(i, &args, &reply)
		}
	}
}

// To begin an election, a follower increments its current term and transitions to candidate state.
// It then votes for itself and issues RequestVote RPCs in parallel to each of the other servers in the cluster.
// Return true only if this candidate wins the election.
func (rf *Raft) beginAnElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.role = RaftRoleCandidate
	rf.votedFor = rf.me
	rf.resetElectionTimeout()
	half := int32(len(rf.peers) / 2) // need at least (half + 1) votes to win the election
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  rf.log[rf.logCount].Term,
		LastLogIndex: rf.log[rf.logCount].Index,
	}
	rf.mu.Unlock()

	// Send RequestVote RPCs to all other servers asynchronously and collect the votes
	var votedCount, unvotedCount int32 = 1, 0
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			reply := RequestVoteReply{}
			go func(server int, args *RequestVoteArgs, reply *RequestVoteReply, votedCount, unvotedCount *int32) {
				ok := rf.sendRequestVote(server, args, reply)
				if ok && reply.VoteGranted {
					atomic.AddInt32(votedCount, 1)
				} else {
					atomic.AddInt32(unvotedCount, 1)
				}
			}(i, &args, &reply, &votedCount, &unvotedCount)
		}
	}

	// if received votes is not enough, to spin wait
	for atomic.LoadInt32(&votedCount) <= half && atomic.LoadInt32(&unvotedCount) <= half {
		time.Sleep(1 * time.Millisecond)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// may receive heartbeat from a new leader during waiting for voting, so need to check state
	if rf.currentTerm == args.Term && rf.role == RaftRoleCandidate {
		// check the result of election and transit role
		if atomic.LoadInt32(&votedCount) > half {
			rf.role = RaftRoleLeader
		} else {
			rf.role = RaftRoleFollower
		}
		rf.votedFor = -1
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		rf.mu.Lock()
		role := rf.role
		rf.mu.Unlock()
		if role == RaftRoleLeader {
			// send heartbeat
			rf.sendHeartbeat()
			// according to lab requirement, sleep for 100 ms to send heartbeat again
			time.Sleep(time.Millisecond * time.Duration(HeartbeatInterval))
		} else {
			// if election timeout, to begin an election
			if rf.checkElectionTimeout() {
				rf.beginAnElection()
			}
			// sleep for a little time to check again
			time.Sleep(time.Millisecond * time.Duration(CheckTimeoutInterval))
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.log = make([]Log, 0)
	// log's first index is 1, so append a virtual log
	rf.log = append(rf.log, Log{-1, 0, nil})
	rf.role = RaftRoleFollower
	rf.applyCh = applyCh
	rf.logCount = 0
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.resetElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
