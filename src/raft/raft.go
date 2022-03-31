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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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
	HEARTBEAT_INTERVAL   = 100
	ELECTION_MIN_TIMEOUT = 5 * HEARTBEAT_INTERVAL
	ELECTION_MAX_TIMEOUT = 10 * HEARTBEAT_INTERVAL
	RETRY_TIMEOUT        = 100
	MAX_RETRY            = 1
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
	currentTerm int
	votedFor    int
	state       int8
	// log		 []LogEntry
	electionFlag bool
}
type LogEntry struct {
	Term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
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
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = true
	rf.electionFlag = false
	// if args.Term < rf.currentTerm {
	// 	reply.Success = false
	// 	return
	// }
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		rf.votedFor = -1
	}
	//log.Printf("[%v] receive AppendEntries from %d", rf.me, args.LeaderId)
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		// if there is a higher term, follow him.
		if rf.state == LEADER || rf.state == CANDIDATE {
			rf.state = FOLLOWER
		}
		rf.currentTerm = args.Term // update term
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.electionFlag = false // refresh election timer
		//log.Printf("[%v] receive from [%v],grant vote\n", rf.me, args.CandidateId)
	} else {
		// otherwise, deny vote
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		//log.Printf("[%v] receive from [%v],reject\n", rf.me, args.CandidateId)
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
func (rf *Raft) sendRequestVote(server int, vote *int32, done *int32, currentTerm int, me int, cond *sync.Cond) {
	args := RequestVoteArgs{}
	args.Term = currentTerm
	args.CandidateId = me
	reply := RequestVoteReply{}
	times := 0
	for times != MAX_RETRY {
		ok := rf.peers[server].Call("Raft.RequestVote", &args, &reply)
		rf.mu.Lock()
		if rf.state != CANDIDATE {
			rf.mu.Unlock()
			atomic.AddInt32(done, 1)
			cond.Broadcast()
			return
		}
		if ok {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = FOLLOWER
			}
			if reply.VoteGranted {
				atomic.AddInt32(vote, 1)
			}
			rf.mu.Unlock()
			atomic.AddInt32(done, 1)
			cond.Broadcast()
			return
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(RETRY_TIMEOUT * time.Millisecond))
		times++
	}
}

func (rf *Raft) sendAppendEntries(server int, wg *sync.WaitGroup) {
	args := AppendEntriesArgs{}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	reply := AppendEntriesReply{}
	//log.Printf("[%v] send heartbeat to %v\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if ok {
		if reply.Term > rf.currentTerm {
			rf.state = FOLLOWER
			rf.currentTerm = reply.Term
		}
	}
	wg.Done()
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
	rf.mu.Lock()
	rf.currentTerm = 0
	rf.votedFor = rf.me
	rf.state = FOLLOWER
	rf.electionFlag = true
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	const ELECTION_TIMEOUT_RANGE = ELECTION_MAX_TIMEOUT - ELECTION_MIN_TIMEOUT
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		time.Sleep(time.Duration(rand.Int63()%(ELECTION_TIMEOUT_RANGE)+ELECTION_MIN_TIMEOUT) * time.Millisecond)
		// after sleep, start a new election

		var currentTerm int
		var me int
		rf.mu.Lock()
		if rf.state == LEADER {
			rf.mu.Unlock()
			continue
		}
		if !rf.electionFlag {
			//log.Printf("[%v] heartbeat verified\n", rf.me)
			rf.electionFlag = true
			rf.mu.Unlock()
			continue
		}
		//log.Printf("[%v] election timer up\n", rf.me)
		rf.state = CANDIDATE
		rf.currentTerm += 1
		rf.votedFor = rf.me
		currentTerm = rf.currentTerm
		me = rf.me
		n := len(rf.peers)
		rf.mu.Unlock()
		go func(total int) {
			// start a new election
			var done int32 = 0
			var vote int32 = 0
			cond := sync.NewCond(&sync.Mutex{})
			//log.Printf("[%v] start a new election. total=%v\n", rf.me, total)
			for server := 0; server < n; server++ {
				if server != me {
					go rf.sendRequestVote(server, &vote, &done, currentTerm, me, cond)
				}
			}
			for {
				cond.L.Lock()
				cond.Wait()
				cond.L.Unlock()
				if int(atomic.LoadInt32(&vote)) >= total/2 {
					// "=" beacause candidate will vote themselves
					//log.Printf("[%v] win the election\n", rf.me)
					rf.mu.Lock()
					rf.state = LEADER
					// note: here does not unlock
					break
				}
				if int(atomic.LoadInt32(&done)) == total {
					//log.Printf("[%v] loss the election\n", rf.me)
					return
				}
			}

			// if it wins the election, it begins to send heartbeat
			// here it have required the mutex
			currentTerm = rf.currentTerm
			for rf.state == LEADER && !rf.killed() {
				rf.mu.Unlock()
				group := sync.WaitGroup{}
				group.Add(total - 1)
				//log.Printf("[%v] leader send heartbeat\n", rf.me)
				for server := 0; server < total; server++ {
					if server != me {
						go rf.sendAppendEntries(server, &group)
					}
				}
				group.Wait()
				time.Sleep(HEARTBEAT_INTERVAL * time.Millisecond)
				// re-acquire the mutex to check if it is still leader
				rf.mu.Lock()
			}
			//log.Printf("[%v] is no longer leader\n", rf.me)
			rf.mu.Unlock()
		}(n)
	}
	//log.Printf("[%v] self suicide\n", rf.me)
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
	rf.votedFor = me
	rf.state = FOLLOWER
	rf.electionFlag = true
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
