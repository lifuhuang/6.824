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
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"math/rand"
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

	// Extra fields
	heartBeat          chan struct{}
	commitIndexUpdated chan struct{}
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
	dead
)

const heartBeatPeriod = time.Millisecond * 100

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
	// rf.printLog("Persist from %v --- rf.currentTerm = %v, rf.votedFor = %v, log = %v", location, rf.currentTerm, rf.votedFor, rf.log)
	buffer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(buffer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)
	encoder.Encode(rf.role == dead)
	data := buffer.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		rf.printLog("WARNING: data is empty, bootstraping with initial state.")
		return
	}

	buffer := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(buffer)

	var currentTerm int
	var votedFor int
	var log []LogEntry
	var isDead bool

	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&log) != nil ||
		decoder.Decode(&isDead) != nil {
		rf.printLog("WARNING: failed to readPersist, bootstraping with initial state.")
		return
	}
	rf.printLog("rf.currentTerm = %v, rf.votedFor = %v, log = %v", rf.currentTerm, rf.votedFor, rf.log)

	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	if isDead {
		rf.role = dead
	}
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// initialize follower
	if args.Term > rf.currentTerm {
		rf.initFollower(args.Term)
		rf.postHeartBeat()
	}

	// rf.printLog("Received RequestVote")
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm || rf.votedFor != -1 {
		reply.VoteGranted = false
		return
	}

	// Up-to-date check
	lastLogTerm := rf.log[len(rf.log)-1].Term
	lastLogIndex := len(rf.log) - 1
	reply.VoteGranted = (args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
	rf.printLog("voteGranted = %v, candidate = %v, args.LastLogTerm = %v, lastLogTerm = %v, args.LastLogIndex = %v, lastLogIndex = %v", reply.VoteGranted, args.CandidateID, args.LastLogTerm, lastLogTerm, args.LastLogIndex, lastLogIndex)
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
	rf.printLog("Sending sendRequestVote to %v, args: %v", server, args)
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
	rf.persist()
	rf.printLog("New command %v stored at %v", command, len(rf.log)-1)

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
	rf.mu.Lock()
	rf.role = dead
	rf.persist()
	close(rf.commitIndexUpdated)
	close(rf.heartBeat)
	rf.mu.Unlock()
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
	// Persistent state on all servers
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{LogEntry{}}

	// Volatile state on all servers
	rf.role = follower
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.heartBeat = make(chan struct{})
	rf.commitIndexUpdated = make(chan struct{}, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	if rf.role == dead {
		return rf
	}

	// rf.printLog("Read persist")
	go func() {
		for {
			switch rf.getRoleWithLock() {
			case follower:
				rf.runFollower()
			case candidate:
				rf.runCandidate()
			case leader:
				rf.runLeader()
			case dead:
				return
			}
		}
	}()

	go rf.applyComittedLogEntries(applyCh)

	return rf
}

func (rf *Raft) applyComittedLogEntries(applyCh chan ApplyMsg) {
	defer close(applyCh)
	for range rf.commitIndexUpdated {
		for {
			rf.mu.Lock()
			if rf.role == dead {
				rf.mu.Unlock()
				return
			}

			if rf.lastApplied >= rf.commitIndex {
				rf.mu.Unlock()
				break
			}

			nextIndex := rf.lastApplied + 1
			msg := ApplyMsg{
				Command:      rf.log[nextIndex].Command,
				CommandIndex: nextIndex,
				CommandValid: true,
			}

			rf.lastApplied = nextIndex
			rf.mu.Unlock()

			rf.printLog("Applying %v", msg)
			applyCh <- msg
		}
	}
}

func (rf *Raft) runFollower() {
	for rf.getRoleWithLock() == follower {
		timer := time.NewTimer(randomElectionTimeout())
		select {
		case <-timer.C:
			rf.mu.Lock()
			rf.initCandidate()
			rf.mu.Unlock()
		case <-rf.heartBeat:
		}
		timer.Stop()
	}
}

func (rf *Raft) getRoleWithLock() Role {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role
}

func (rf *Raft) runCandidate() {
	//rf.printLog("New candidate")
	for rf.getRoleWithLock() == candidate {
		cancel := make(chan struct{})
		done := rf.startElection(cancel)
		timeout := time.After(randomElectionTimeout())

		restartElection := false
		for rf.getRoleWithLock() == candidate && !restartElection {
			select {
			case <-done:
			case <-timeout:
				restartElection = true
			case <-rf.heartBeat:
			}
		}
		close(cancel)
	}
}

func (rf *Raft) runLeader() {
	// rf.printLog("Start running as leader!")
	rf.broadcastAppendEntries()
	ticker := time.NewTicker(heartBeatPeriod)
	for rf.getRoleWithLock() == leader {
		select {
		case <-ticker.C:
			// rf.printLog("broadcasting appendEntries.")
			rf.broadcastAppendEntries()
		case <-rf.heartBeat:
		}
	}
	ticker.Stop()
}

func (rf *Raft) startElection(cancel <-chan struct{}) <-chan struct{} {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.votedFor = rf.me
	rf.currentTerm++
	rf.persist()
	// rf.printLog("Starting election")

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
				if !rf.sendRequestVote(server, args, reply) {
					rf.printLog("SendRequestVote failed.")
					voteChan <- false
					return
				}

				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.initFollower(reply.Term)
				}
				rf.mu.Unlock()

				voteChan <- reply.VoteGranted
			}(i)
		}
	}

	done := make(chan struct{})
	go func() {
		defer func() { close(done) }()
		totalVotes := 1
		for i := 0; i < nPeers-1; i++ {
			select {
			case result := <-voteChan:
				if result {
					totalVotes++
				}
			case <-cancel:
				return
			}

			if totalVotes > nPeers/2 {
				rf.mu.Lock()
				if rf.role == candidate && rf.currentTerm == args.Term {
					rf.initLeader()
					rf.printLog("Won election")
				}
				rf.mu.Unlock()
				return
			}
		}
	}()

	return done
}

func (rf *Raft) createAppendEntriesArgs(server int) *AppendEntriesArgs {
	nextIndex := rf.nextIndex[server]
	args := &AppendEntriesArgs{
		Entries:      []LogEntry{},
		LeaderCommit: rf.commitIndex,
		LeaderID:     rf.me,
		PrevLogIndex: nextIndex - 1,
		PrevLogTerm:  rf.log[nextIndex-1].Term,
		Term:         rf.currentTerm,
	}

	for i := nextIndex; i < len(rf.log); i++ {
		args.Entries = append(args.Entries, rf.log[i])
	}

	return args
}

func (rf *Raft) sendAppendEntriesToServer(server int, args *AppendEntriesArgs) {
	rf.printLog("entering sendAppendEntriesToServer")
	reply := new(AppendEntriesReply)

	if !rf.sendAppendEntries(server, args, reply) {
		rf.printLog("sendAppendEntries failed")
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role == leader && args.Term == rf.currentTerm {
		if reply.Term > rf.currentTerm {
			rf.initFollower(reply.Term)
			return
		}
		if reply.Success {
			rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			if len(args.Entries) > 0 {
				left := args.PrevLogIndex + 1
				right := args.PrevLogIndex + len(args.Entries)
				newLeft := SearchFirst(left, right, func(x int) bool { return rf.log[x].Term == rf.currentTerm })
				rf.printLog("left = %v, right = %v, newLeft = %v, entries = %v", left, right, newLeft, args.Entries)
				if newLeft == -1 {
					return
				}
				SearchLast(newLeft, right, rf.tryCommit)
			}
		} else {
			nextIndex := rf.nextIndex[server]
			for nextIndex-1 >= reply.SuggestedNextLogIndex && rf.log[nextIndex-1].Term != reply.SuggestedNextLogTerm {
				nextIndex--
			}
			rf.nextIndex[server] = nextIndex
		}
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != leader {
		return
	}

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			args := rf.createAppendEntriesArgs(i)
			go rf.sendAppendEntriesToServer(i, args)
		}
	}
}

func (rf *Raft) tryCommit(index int) bool {
	if index >= len(rf.log) {
		return false
	}

	if index <= rf.commitIndex {
		return true
	}

	count := 1
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me && rf.matchIndex[i] >= index {
			count++
		}
	}

	rf.printLog("tryCommit(%v), count = %v", index, count)
	if count > len(rf.peers)/2 {
		rf.setCommitIndex(index)
		return true
	}

	return false
}

func (rf *Raft) setCommitIndex(index int) {
	rf.commitIndex = index
	rf.printLog("Committed rf.log[%v]= {Term: %v, Command: %v}", index, rf.log[index].Term, rf.log[index].Command)
	select {
	case rf.commitIndexUpdated <- struct{}{}:
	default:
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
	Term                  int
	Success               bool
	SuggestedNextLogTerm  int
	SuggestedNextLogIndex int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.printLog("Sending AppendEntries to %v, args.Entries: %v, args.Term: %v, args.LeaderCommit: %v", server, args.Entries, args.Term, args.LeaderCommit)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	rf.printLog("Received AppendEntries from %v, args.Term = %v, args.PrevLogTerm = %v, args.PrevLogIndex = %v,  args.Entries = %v", args.LeaderID, args.Term, args.PrevLogTerm, args.PrevLogIndex, args.Entries)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.SuggestedNextLogTerm = -1
		reply.SuggestedNextLogIndex = -1
		reply.Success = false
		return
	}

	if args.Term >= rf.currentTerm {
		rf.initFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	if args.PrevLogIndex > len(rf.log)-1 {
		reply.SuggestedNextLogIndex = len(rf.log)
		reply.SuggestedNextLogTerm = -1
		reply.Success = false
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.SuggestedNextLogIndex = args.PrevLogIndex
		reply.SuggestedNextLogTerm = rf.log[args.PrevLogIndex].Term
		for reply.SuggestedNextLogIndex-1 > 0 && rf.log[reply.SuggestedNextLogIndex-1].Term == reply.SuggestedNextLogTerm {
			reply.SuggestedNextLogIndex--
		}
		reply.Success = false
	} else {
		rf.appendEntriesToLogs(args)
		reply.Success = true
	}

	rf.postHeartBeat()

	return
}

func (rf *Raft) postHeartBeat() {
	select {
	case rf.heartBeat <- struct{}{}:
	default:
	}
}

func (rf *Raft) appendEntriesToLogs(args *AppendEntriesArgs) {
	for i := 0; i < len(args.Entries); i++ {
		pos := args.PrevLogIndex + 1 + i
		if pos == len(rf.log) || rf.log[pos].Term != args.Entries[i].Term {
			rf.log = append(rf.log[:pos], args.Entries[i:]...)
			break
		}
	}

	lastNewEntryIndex := args.PrevLogIndex + len(args.Entries)

	rf.setCommitIndex(Min(lastNewEntryIndex, args.LeaderCommit))

	return
}

func (rf *Raft) printLogWithLock(format string, values ...interface{}) {
	rf.mu.Lock()
	rf.printLog(format, values...)
	rf.mu.Unlock()
}

func (rf *Raft) printLog(format string, values ...interface{}) {
	if !enableLog || rf.role == dead {
		return
	}

	var role string
	state := rf.role
	me := rf.me
	currentTerm := rf.currentTerm

	switch state {
	case follower:
		role = "follower "
	case candidate:
		role = "candidate"
	case leader:
		role = "leader   "
	}

	message := fmt.Sprintf(format, values...)

	fmt.Printf("time: %v, term: %v, raft: %v, role: %v: %v\n", time.Now().Format("15:04:05.000"), currentTerm, me, role, message)
}

func (rf *Raft) initFollower(term int) {
	rf.currentTerm = term
	rf.role = follower
	rf.votedFor = -1
}

func (rf *Raft) initCandidate() {
	rf.role = candidate
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
	return time.Duration(250+rand.Int()%250) * time.Millisecond
}
