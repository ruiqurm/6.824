package raft

import "sync/atomic"

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	// offset int
	Data []byte
	// done bool
	// no need to implement offset and done in this lab
}
type InstallSnapshotReply struct {
	Term int
}

// type Snapshot struct {
// 	Snapshot  []byte
// 	LastIndex []int
// 	LastTerm  []int
// 	pos
// }

// func NewSnapshot(snapshot []byte, lastIndex []int, lastTerm []int) *Snapshot {
// 	return &Snapshot{snapshot, lastIndex, lastTerm}
// }
// func (s *Snapshot) Append(data []byte, lastIndex int, lastTerm int) {
// 	s.Snapshot = append(s.Snapshot, data...)
// 	s.LastIndex = append(s.LastIndex, lastIndex)
// 	s.LastTerm = append(s.LastTerm, lastTerm)
// }
// func (s *Snapshot) GetLeastMatch(index int) ([]byte, int, int) {
// 	lastindex := bineary_search(s.LastIndex, index)
// 	if lastindex == len(s.LastIndex) {
// 		return nil, -1, -1
// 	}

// 	return s.Snapshot[]
// }

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return atomic.LoadInt32(&rf.is_appling) == 0
	// Your code here (2D).
	// return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.log.GetLastIncludedIndex() >= index {
			rf.Debug(dSnapshot, "Snapshot %d ignore\n", index)
			return
		}
		rf.Debug(dSnapshot, "Snapshot %d\n", index)
		lastTerm := rf.log.Get(index).Term
		rf.log.Reindex(index, lastTerm)
		rf.snapshot = snapshot
		rf.persistL()
		// for i := 0; i < len(rf.peers); i++ {
		// 	if i != rf.me {
		// 		go rf.SendInstallSnapshot(i)
		// 	}
		// }
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      rf.snapshot,
			SnapshotTerm:  rf.log.GetLastIncludedTerm(),
			SnapshotIndex: rf.log.GetLastIncludedIndex(),
		}
		rf.lastApplied = rf.log.GetLastIncludedIndex()

	}()

}
func (rf *Raft) SendInstallSnapshot(server int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{}
	args.Term = rf.currentTerm
	args.Data = rf.snapshot
	args.LastIncludedIndex = rf.log.GetLastIncludedIndex()
	args.LastIncludedTerm = rf.log.GetLastIncludedTerm()
	args.LeaderId = rf.me
	args.Term = rf.currentTerm
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	reply := InstallSnapshotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.asFollowerL(reply.Term)
			rf.Debug(dSnapshot, "%v -> %v; Snapshot Done.Because it's no longer leader\n", rf.me, server)
			rf.persistL()
			return
		} else if reply.Term > args.Term {
			// failed; but it's still leader
			// can add retry here
			rf.Debug(dSnapshot, "%v -> %v; Snapshot retry again", rf.me, server)
			go rf.SendInstallSnapshot(server)
		} else {
			// success
			rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
			rf.nextIndex[server] = max(rf.nextIndex[server], args.LastIncludedIndex+1)
			rf.Debug(dSnapshot, "%v -> %v; Snapshot Done\n", rf.me, server)
			return
		}
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.Debug(dSnapshot, "rejct InstallSnapshot(%v->%v)", args.LeaderId, rf.me)
		return
	}
	rf.Debug(dSnapshot, "accept InstallSnapshot(%v->%v)", args.LeaderId, rf.me)
	// is send by current leader
	rf.setElectionTimeL() // refresh timer
	rf.asFollowerL(args.Term)
	rf.votedFor = args.LeaderId
	rf.snapshot = args.Data
	rf.log.Reindex(args.LastIncludedIndex, args.LastIncludedTerm)
	rf.persistL()
	rf.Debug(dSnapshot, "apply snapshot:index=%v,term:%v\n", args.LastIncludedIndex, args.LastIncludedTerm)
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = args.LastIncludedIndex
}
