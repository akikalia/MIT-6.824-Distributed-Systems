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
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

type RaftState string

const (
	Leader    RaftState = "Leader"
	Candidate           = "Candidate"
	Follower            = "Follower"
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

type LogEntry struct {
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	temp sync.Mutex // Lock to protect shared access to this peer's state

	cond *sync.Cond

	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	//2A{//
	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state RaftState
	//follower
	rpcTriggered bool
	//candidate
	votesReceived int
	//leader

	persisted bool
	//}2A//

	applyCh chan ApplyMsg
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// func (rf *Raft) printLog() {
// 	rf.temp.Lock()
// 	defer rf.temp.Unlock()
// 	//DPrintf("%d printing log{", rf.me)
// 	for i, entry := range rf.log {
// 		//DPrintf("%d	index: %d command: %s  term: %d ",rf.me, i, entry.Command, entry.Term)
// 	}
// 	//DPrintf("}")
// }

// return currentTerm and whether this server
// believes it is the leader.

func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	if rf.state == Leader {
		isleader = true
	}
	term = rf.currentTerm
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
	rf.mu.Lock()

	Log := rf.log
	VotedFor := rf.votedFor
	CurrentTerm := rf.currentTerm

	//DPrintf("%d persisting data", rf.me)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(Log)
	e.Encode(VotedFor)
	e.Encode(CurrentTerm)
	data := w.Bytes()
	rf.mu.Unlock()

	//DPrintf("%d finished persisting data", rf.me)
	// //DPrintf("persisting term:%d id:%d", CurrentTerm, rf.me)
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
	var CurrentTerm int
	var VotedFor int
	var Log []LogEntry
	if d.Decode(&Log) != nil || d.Decode(&VotedFor) != nil || d.Decode(&CurrentTerm) != nil {
		//DPrintf("fatal error")
		os.Exit(1)
	} else {

		//DPrintf("%d reading persisted data ", rf.me)
		//   //DPrintf("recovering")
		rf.log = Log
		rf.currentTerm = CurrentTerm
		rf.votedFor = VotedFor
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).

	Term         int //candidate’s term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate’s last log entry (§5.4)
	LastLogTerm  int //term of candidate’s last log entry (§5.4)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

//
// example RequestVote RPC handler.
//

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//DPrintf("id: %d  - RequestVote()", rf.me)

	rf.mu.Lock()
	// rf.printLog()
	defer rf.mu.Unlock()
	rf.checkPersisted(true)

	if rf.checkTerm(args.Term) {

	}
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	LIndex := len(rf.log) - 1
	LTerm := rf.log[LIndex].Term

	if rf.state == Candidate {
		//DPrintf("id: %d  -candidate asked to vote from %d", rf.me, args.CandidateId)

	}

	// 1. Reply false if term < currentTerm (§5.1)
	// 2. If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if args.Term >= rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		// if candidate’s log is at
		// least as up-to-date as receiver’s log,
		if (args.LastLogTerm > LTerm) || ((args.LastLogTerm == LTerm) && (args.LastLogIndex >= LIndex)) {
			rf.rpcTriggered = true // or up
			reply.VoteGranted = true
			if rf.votedFor != args.CandidateId {
				rf.votedFor = args.CandidateId
				rf.setUnpersisted()
			}
			//DPrintf("id: %d  - voting for %d", rf.me, args.CandidateId)
		} else {
			//DPrintf("%d args llt: %d, LTerm: %d ,  lli : %d, lindex : %d", rf.me, args.LastLogTerm,LTerm,args.LastLogIndex,LIndex)
		}
	} else {
		//DPrintf("second %d args llt: %d, LTerm: %d ,  lli : %d, lindex : %d", rf.me, args.LastLogTerm,LTerm,args.LastLogIndex,LIndex)

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
	// rf.mu.Lock()
	rf.checkPersisted(false)
	// rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	//names must start with uppercase letter
	Term         int        //leader’s term
	LeaderID     int        //so follower can redirect clients
	PrevLogIndex int        //index of log entry immediately precedingnew ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entries      []LogEntry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

type AppendEntriesReply struct {
	//names must start with uppercase letter
	Term      int  //currentTerm, for leader to update itself
	Success   bool // true if follower contained entry matching PrevLogIndex and prevLogTerm
	CurrLen   int
	CurrTerm  int
	CurrIndex int
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//DPrintf("id: %d  - AppendEntries()", rf.me)
	rf.mu.Lock()
	reply.CurrLen = -1
	reply.CurrTerm = -1
	reply.CurrIndex = -1
	defer rf.mu.Unlock()
	rf.checkPersisted(true)

	if rf.checkTerm(args.Term) {
		return
	}
	if rf.state == Candidate {
		rf.becomeFollower()
		// rf.setUnpersisted()
	}
	rf.rpcTriggered = true // or up

	reply.Success = true
	reply.Term = rf.currentTerm //if uncommented error is gone - weird!!!!!!!!! should be replyyy

	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term != rf.currentTerm {
		reply.Success = false
		return
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if !(args.PrevLogIndex == 0 || ((args.PrevLogIndex < len(rf.log)) && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm)) {
		reply.Success = false
		if args.PrevLogIndex >= len(rf.log) {
			reply.CurrLen = len(rf.log)
		} else {
			reply.CurrTerm = rf.log[args.PrevLogIndex].Term
			for i := len(rf.log) - 1; i > 0; i-- {
				if rf.log[i].Term == reply.CurrTerm && i-1 >= 0 && rf.log[i-1].Term != reply.CurrTerm {
					reply.CurrIndex = i
					break
				} else if rf.log[i].Term < reply.CurrTerm {
					break
				}
			}
		}
		return
	}

	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	// 4. Append any new entries not already in the log
	for i := 0; i < len(args.Entries); i++ {
		if args.PrevLogIndex+i+1 < len(rf.log) {
			if args.Entries[i].Term != rf.log[args.PrevLogIndex+i+1].Term {
				rf.log = rf.log[0 : args.PrevLogIndex+i+1]
				rf.log = append(rf.log, args.Entries[i])
			}
		} else {
			rf.log = append(rf.log, args.Entries[i])

		}
	}

	rf.setUnpersisted()
	// 5. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		rf.cond.Signal()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// rf.mu.Lock()
	rf.checkPersisted(false)
	// rf.mu.Unlock()
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) forOneAppendEntries(server int, heartbeat bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.state == Leader {
		var entries []LogEntry = nil
		if rf.killed() {
			return
		}
		if rf.nextIndex[server] < len(rf.log) {
			entries = rf.log[rf.nextIndex[server]:len(rf.log)]
		}
		if len(entries) == 0 && heartbeat == false {
			return
		}
		args := &AppendEntriesArgs{}
		reply := &AppendEntriesReply{}
		args.Term = rf.currentTerm
		args.LeaderID = rf.me
		args.PrevLogIndex = rf.nextIndex[server] - 1
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		args.Entries = entries
		args.LeaderCommit = rf.commitIndex
		rf.mu.Unlock()

		ok := rf.sendAppendEntries(server, args, reply) // blocking

		rf.mu.Lock()
		if !ok || rf.checkTerm(reply.Term) { //|| heartbeat
			break
		}
		if reply.Success {
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			if rf.update() {
				rf.cond.Signal()
			}
			break
		} else {
			if reply.CurrLen != -1 {
				rf.nextIndex[server] = reply.CurrLen
			} else {
				rf.nextIndex[server] = reply.CurrIndex
				for i := len(rf.log) - 1; i > 0; i-- {
					if rf.log[i].Term == reply.CurrTerm {
						rf.nextIndex[server] = i
						break
					} else if rf.log[i].Term < reply.CurrTerm {
						break
					}
				}
				// rf.nextIndex[server]--
			}
			if rf.nextIndex[server] < 1 {
				rf.nextIndex[server] = 1
			}
		}
	}
}

//while not commited - try again

func (rf *Raft) forEachAppendEntries(heartbeat bool) {
	for k, _ := range rf.peers {
		if rf.me != k {
			go rf.forOneAppendEntries(k, heartbeat)
		}
	}
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
	rf.mu.Lock()
	//DPrintf("id: %d  - Start()", rf.me)
	isLeader = rf.state == Leader
	if !isLeader {
		rf.mu.Unlock()
		return index, term, isLeader
	}
	temp := LogEntry{Command: command, Term: rf.currentTerm}
	rf.log = append(rf.log, temp)
	rf.setUnpersisted()
	term = rf.currentTerm
	index = len(rf.log) - 1
	rf.forEachAppendEntries(false)
	rf.mu.Unlock()

	return index, term, isLeader
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
	//2A{//

	rf.applyCh = applyCh

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = []LogEntry{{Term: 0, Command: nil}}
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers), len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers), len(rf.peers))

	rf.cond = sync.NewCond(&rf.mu)

	rf.becomeFollower()

	rf.mu.Lock()
	go rf.monitor()
	go rf.monitorElection()

	//}2A//
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()

	return rf
}

func (rf *Raft) monitor() {
	for {
		if rf.killed() {
			//DPrintf("%d killed", rf.me)
			return
		}
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied {
			rf.cond.Wait()
		}
		rf.mu.Unlock()
		rf.apply()

		// time.Sleep(10 * time.Millisecond) // blocking
	}
}

func (rf *Raft) update() bool {
	if rf.state == Leader {
		maxIndex := -1
		N := rf.commitIndex + 1
		for N < len(rf.log) {
			commitedFollowers := 1
			for follower, matchIndex := range rf.matchIndex {
				if follower != rf.me {
					if matchIndex >= N {
						commitedFollowers += 1
					}
				}
			}
			if commitedFollowers > len(rf.peers)/2 && N < len(rf.log) && rf.log[N].Term == rf.currentTerm {
				maxIndex = N
			}
			N += 1
		}
		if maxIndex > rf.commitIndex {
			rf.commitIndex = maxIndex
			return true
		}
	}
	return false
}

func (rf *Raft) apply() {
	rf.mu.Lock()
	for rf.commitIndex > rf.lastApplied {
		temp := ApplyMsg{true, rf.log[rf.lastApplied+1].Command, rf.lastApplied + 1}
		rf.mu.Unlock()
		rf.applyCh <- temp
		rf.mu.Lock()
		rf.lastApplied += 1
	}
	rf.mu.Unlock()
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	for k := range rf.matchIndex {
		if k != rf.me {
			rf.nextIndex[k] = len(rf.log)
			rf.matchIndex[k] = 0
		}
	}
	go rf.lead()
}

func (rf *Raft) becomeFollower() {
	rf.state = Follower
	rf.rpcTriggered = false
	rf.votedFor = -1
	// rf.setUnpersisted()
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.setUnpersisted()
	rf.votesReceived = 1

	//DPrintf("%d became a candidate term: %d", rf.me, rf.currentTerm)
	for k, _ := range rf.peers {
		if rf.me != k {
			go func(term int, me int, server int, LLIndex int, LLTerm int) {
				args := &RequestVoteArgs{}
				reply := &RequestVoteReply{}
				args.Term = term
				args.CandidateId = me
				args.LastLogIndex = LLIndex
				args.LastLogTerm = LLTerm

				ok := rf.sendRequestVote(server, args, reply) // blocking
				if !ok {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.checkTerm(reply.Term) {
					return
				}
				if reply.VoteGranted {
					//DPrintf("%d voted for %d",server,  rf.me)

					rf.votesReceived += 1

					//DPrintf("%d received %d/%d votes term: %d", rf.me, rf.votesReceived, len(rf.peers), rf.currentTerm)
				}

				if rf.state != Leader && rf.votesReceived > (len(rf.peers)/2) {

					//DPrintf("%d became a leader term: %d", rf.me, rf.currentTerm)
					rf.becomeLeader()
				}
			}(rf.currentTerm, rf.me, k, len(rf.log)-1, rf.log[len(rf.log)-1].Term)
		}
	}
}

func (rf *Raft) lead() {
	for {
		if rf.killed() {
			return
		}
		rf.mu.Lock()

		if rf.state != Leader {
			rf.mu.Unlock()
			break
		}
		rf.forEachAppendEntries(true)
		rf.mu.Unlock()
		time.Sleep(150 * time.Millisecond) // blocking
	}
}

func (rf *Raft) monitorElection() {
	//send requestVote when leader hasn't responded in a while
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	for {
		if rf.killed() {
			//DPrintf("%d killed", rf.me)
			return
		}
		secs := ((r1.Int63() % 300) + 300)
		time.Sleep(time.Duration(secs) * time.Millisecond) // blocking
		rf.mu.Lock()
		if rf.state == Follower && rf.rpcTriggered { //rf.leaderAppended && (rf.votedFor == nil(or -1))){
			//if we're follower and leader have sent append rpc and never voted for candidate - reset timer
			rf.rpcTriggered = false
			//DPrintf("%d could not become a candidate due to triggered rpc  - term: %d", rf.me, rf.currentTerm)
		} else if rf.state != Leader {
			//host an election
			rf.becomeCandidate()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) checkTerm(term int) bool {
	if rf.currentTerm < term {
		rf.currentTerm = term
		rf.becomeFollower()
		rf.setUnpersisted()
		return true
	}
	return false
}

func (rf *Raft) setUnpersisted() {
	// rf.persist()
	rf.persisted = false
}

func (rf *Raft) checkPersisted(isLocked bool) {
	if !isLocked {
		rf.mu.Lock()
		defer rf.mu.Unlock()
	}
	if rf.persisted == false {
		// if !isLocked {
		// 	rf.mu.Lock()
		// 	defer rf.mu.Unlock()
		// }
		rf.mu.Unlock()
		rf.persist()
		rf.mu.Lock()
		rf.persisted = true
	}
}
