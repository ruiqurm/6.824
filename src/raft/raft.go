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
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
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

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2
)
const (
	HEARTBEAT_INTERVAL     = 100
	ELECTION_MIN_TIMEOUT   = 5 * HEARTBEAT_INTERVAL
	ELECTION_TIMEOUT_RANGE = 3 * HEARTBEAT_INTERVAL
	WAKE_UP_INTERVAL       = 50
	MAX_LOG_PER_REQUEST    = 512
)

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
	currentTerm  int
	votedFor     int
	state        int8
	log          *Log
	electionTime time.Time

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state;for leader
	nextIndex  []int
	matchIndex []int
	applyCh    chan ApplyMsg
	applyCond  *sync.Cond

	// snapshot
	snapshot []byte
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}
func (rf *Raft) checkThenPersistL(should_persist *bool) {
	if *should_persist {
		rf.persistL()
	}
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persistL() {
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
	e.Encode(rf.log.GetLog())
	state := w.Bytes()
	rf.persister.SaveStateAndSnapshot(state, rf.snapshot)
	rf.Log_infofL("persist")
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		panic("readPersist error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = NewLog(0, 0, logs)
	}

	rf.Log_infofL("read Persist\n")

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
	// XTerm   int // collision term
	XIndex int // first log index which term equals to collision term
	// XLen    int // distance of real next index and now index
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		reply.Term = 0
		reply.Success = false
		return
	}

	rf.setElectionTimeL() // refresh timer
	reply.Term = rf.currentTerm
	should_persist := false
	defer rf.checkThenPersistL(&should_persist)
	if args.Term >= rf.currentTerm {
		if rf.state != FOLLOWER {
			rf.asFollowerL(args.Term)
			should_persist = true
		}
		if rf.votedFor != args.LeaderId {
			rf.votedFor = args.LeaderId
			should_persist = true
		}
		// defer rf.persistL()
		// check log
		prev_index := args.PrevLogIndex // index of the first log entry not yet applied
		if rf.log.LatestIndex() < prev_index {
			reply.Success = false
			// reply.XTerm = -1
			reply.XIndex = rf.log.LatestIndex() + 1
			rf.Log_debugfL("AE %v-> %v,failed,len(rf.log) < prev_index,xindex=%v", args.LeaderId, rf.me, reply.XIndex)
			return
		}

		if prev_index > 0 && rf.log.GetTerm(prev_index) != args.PrevLogTerm {
			// should discard the log entries before prevLogIndex
			// reply.XTerm = rf.log.GetTerm(prev_index)
			idx := prev_index
			for ; idx > 0; idx-- {
				if rf.log.GetTerm(idx) != rf.log.GetTerm(prev_index) {
					break
				}
			}
			reply.XIndex = idx + 1
			reply.Success = false
			rf.Log_debugfL("AE: %v -> %v,failed,prev_index(index=%v,term=%v) not match(%v),log index is %v;xindex=%v", args.LeaderId, rf.me, prev_index, rf.log.GetTerm(prev_index), args.PrevLogTerm, rf.log.LatestIndex(), reply.XIndex)
			// if rf.log.Cut(reply.XIndex) {
			if rf.log.Cut(prev_index) {
				should_persist = true
			}
		} else {
			// have been synchronized with leader
			reply.Success = true
			if rf.log.Cut(prev_index + 1) {
				should_persist = true
			}
			if len(args.Entries) > 0 && prev_index == rf.log.LatestIndex() {
				rf.log.Append(args.Entries...)
				should_persist = true
			}
			rf.Log_debugfL("AE: %v -> %v,succ", args.LeaderId, rf.me)
		}
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(rf.log.LatestIndex(), args.LeaderCommit)
		}
		if reply.Success && rf.commitIndex > rf.lastApplied {
			rf.applyCond.Broadcast()
		}
	} else {
		reply.Success = false
		rf.Log_debugfL("AE: %v -> %v,failed  args.Term<self.term", args.LeaderId, rf.me)
	}
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if rf.killed() {
		reply.Term = 0
		reply.VoteGranted = false
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// defer rf.persistL()
	should_persist := false
	defer rf.checkThenPersistL(&should_persist)
	n := rf.log.LatestIndex()
	lastLogTerm := rf.log.LatestTerm()

	reply.Term = rf.currentTerm
	rf.setElectionTimeL()
	if args.Term > rf.currentTerm {
		rf.asFollowerL(args.Term)
		rf.votedFor = -1
		should_persist = true
		// reply.VoteGranted = false
		// return
	}

	if args.Term < rf.currentTerm {
		rf.Log_debugfL("RV: %v-> %v,reject;votedFor=%v;args.term=%v; currentTerm=%v\n", args.CandidateId, rf.me, rf.votedFor, args.Term, rf.currentTerm)
		reply.VoteGranted = false
		return
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && // not voted for anyone or voted for the candidate
		((args.LastLogIndex >= n && lastLogTerm == args.LastLogTerm) || (args.LastLogTerm > lastLogTerm)) {
		rf.Log_debugfL("RV: %v-> %v,grant vote,rf.votedFor=%v,last_term=%v\n", args.CandidateId, rf.me, rf.votedFor, lastLogTerm)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		should_persist = true
	} else {
		var term int
		if n == 0 {
			term = 0
		} else {
			term = rf.log.Get(n).Term
		}
		rf.Log_debugfL("RV: %v-> %v,reject;votedFor=%v;args.term=%v,args.lastindex=%v; currentTerm=%v,logindex=%v,term=%v\n", args.CandidateId, rf.me, rf.votedFor, args.Term, args.LastLogIndex, rf.currentTerm, n, term)
		reply.VoteGranted = false
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
	if rf.state == LEADER {
		rf.log.Append(LogEntry{rf.currentTerm, command})
		rf.persistL()
		Log_infof("[%v] log append,index=%v,term%v\n", rf.me, rf.log.LatestIndex(), rf.currentTerm)
	}
	return rf.log.LatestIndex(), rf.currentTerm, rf.state == LEADER
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Log_importfL("be killed")
	rf.applyCond.Broadcast()
	// rf.currentTerm = 0
	// rf.votedFor = -1
	// rf.state = FOLLOWER
	// rf.setElectionTimeL()
	// rf.commitIndex = 0
	// rf.lastApplied = 0
	// rf.log = NewLog()
	// time.Sleep(time.Duration(1000) * time.Millisecond)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		if time.Now().After(rf.electionTime) {
			rf.setElectionTimeL()
			rf.electionL()
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(WAKE_UP_INTERVAL) * time.Millisecond)
	}
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
	rand.Seed(time.Now().UnixNano())
	rf.setElectionTimeL()
	rf.state = FOLLOWER
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCond = sync.NewCond(&sync.Mutex{})
	data := persister.ReadRaftState()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.votedFor = -1
		rf.currentTerm = 0
		rf.log = NewLog(0, 0, []LogEntry{})
	} else {
		rf.readPersist(data)
	}
	// initialize from state persisted before a crash
	rf.applyCh = applyCh
	// SetLevel(ImportantLevel)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applier()
	return rf
}
