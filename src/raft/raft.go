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
	"fmt"
	"labrpc"
	"math/rand"
	"strconv"
	"sync"
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers.
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers.
	commitIndex int
	lastApplied int
	role        Role

	// Volitile state on leaders.
	nextIndex  []int
	matchIndex []int

	// Channels
	appendEntriesChan chan bool
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type Role int

const (
	follower Role = iota
	candidate
	leader
)

const heartBeatPeriod = time.Duration(150) * time.Millisecond

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.role == leader)
	rf.mu.Unlock()

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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
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

	rf.printLog("Received RequestVote")
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// initialize follower
	if args.Term > rf.currentTerm {
		rf.resetFollower(args.Term)
	}

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm || rf.votedFor != -1 {
		reply.VoteGranted = false
		return
	}

	// Up-to-date check
	lastLogTerm := rf.log[len(rf.log)-1].Term
	lastLogIndex := len(rf.log) - 1
	reply.VoteGranted = (args.LastLogTerm > lastLogTerm) || (args.LastLogIndex == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if reply.VoteGranted {
		rf.votedFor = args.CandidateID
	}

	return
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != leader {
		return -1, -1, false
	}

	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})

	return len(rf.log) - 1, rf.currentTerm, true
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{LogEntry{}}

	rf.role = follower
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.appendEntriesChan = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			switch rf.role {
			case follower:
				rf.runFollower()
			case candidate:
				rf.runCandidate()
			case leader:
				rf.runLeader()
			}
		}
	}()

	return rf
}

func (rf *Raft) runFollower() {
	for rf.role == follower {
		select {
		case <-time.After(randomElectionTimeout()):
			rf.mu.Lock()
			rf.role = candidate
			rf.mu.Unlock()
		case <-rf.appendEntriesChan:
		}
	}
}

func (rf *Raft) runCandidate() {
	for rf.role == candidate {
		quit := make(chan bool, 1)
		select {
		case result := <-rf.startElection(quit):
			rf.mu.Lock()
			if result && rf.role == candidate {
				rf.printLog("won election")
				rf.initLeader()
			}
			rf.mu.Unlock()
		case <-time.After(randomElectionTimeout()):
			quit <- true
		case <-rf.appendEntriesChan:
		}
	}
}

func (rf *Raft) runLeader() {
	for rf.role == leader {
		select {
		case <-time.After(heartBeatPeriod):
			rf.broadcastAppendEntries()
		case <-rf.appendEntriesChan:
		}
	}
}

func (rf *Raft) startElection(quit chan bool) <-chan bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.votedFor = rf.me
	rf.currentTerm++
	rf.printLog("Starting election")

	args := &RequestVoteArgs{
		CandidateID:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
		Term:         rf.currentTerm,
	}

	nPeers := len(rf.peers)
	voteChan := make(chan bool, nPeers-1)
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				reply := new(RequestVoteReply)
				for !rf.sendRequestVote(server, args, reply) {
				}

				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.resetFollower(reply.Term)
				}
				rf.mu.Unlock()

				voteChan <- reply.VoteGranted
			}(i)
		}
	}

	resultChan := make(chan bool, 1)
	go func() {
		totalVotes := 1
		for i := 0; i < nPeers-1; i++ {
			select {
			case result := <-voteChan:
				if result {
					totalVotes++
				}
			case <-quit:
				resultChan <- false
				return
			}

			if totalVotes > nPeers/2 {
				resultChan <- true
				return
			}
		}

		resultChan <- false
	}()

	return resultChan
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				reply := new(AppendEntriesReply)
				rf.mu.Lock()
				args := &AppendEntriesArgs{
					Entries:      []LogEntry{},
					LeaderCommit: rf.commitIndex,
					LeaderID:     rf.me,
					PrevLogIndex: len(rf.log) - 1,
					PrevLogTerm:  rf.log[len(rf.log)-1].Term,
				}
				rf.mu.Unlock()

				rf.printLog("Sending AppendEntries to " + strconv.Itoa(server))
				for {
					for !rf.sendAppendEntries(server, args, reply) {
					}

					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.resetFollower(reply.Term)
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
				}
			}(i)
		}
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.printLog("Received AppendEntries")
	// Stepdown
	if args.Term > rf.currentTerm {
		rf.printLog("stepping down")
		rf.resetFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	if args.Term == rf.currentTerm && rf.role == follower {
		reply.Success = rf.appendEntriesToLogs(args)
	} else {
		reply.Success = false
	}

	go func() { rf.appendEntriesChan <- true }()

	return
}

func (rf *Raft) appendEntriesToLogs(args *AppendEntriesArgs) bool {
	if len(rf.log)-1 < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return false
	}

	for i := 0; i < len(args.Entries); i++ {
		pos := args.PrevLogIndex + 1 + i
		if pos == len(rf.log) || rf.log[pos].Term != args.Entries[i].Term {
			rf.log = append(rf.log[:pos], args.Entries[i:]...)
			break
		}
	}

	rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)

	leaderCommit := args.LeaderCommit
	lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)
	if lastNewEntryIndex < leaderCommit {
		leaderCommit = lastNewEntryIndex
	}

	if leaderCommit > rf.commitIndex {
		rf.commitIndex = leaderCommit
	}

	return true
}

func (rf *Raft) printLog(message string) {
	var role string
	switch rf.role {
	case follower:
		role = "follower "
	case candidate:
		role = "candidate"
	case leader:
		role = "leader   "
	}

	fmt.Printf("term: %v, raft: %v, role: %v: %v\n", rf.currentTerm, rf.me, role, message)
}

func (rf *Raft) resetFollower(term int) {
	rf.currentTerm = term
	rf.role = follower
	rf.votedFor = -1
}

func (rf *Raft) initLeader() {
	rf.role = leader
	n := len(rf.peers)
	rf.nextIndex = make([]int, n, n)
	rf.matchIndex = make([]int, n, n)

	for i := 0; i < n; i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
}

func randomElectionTimeout() time.Duration {
	return time.Duration(500+rand.Int()%500) * time.Millisecond
}
