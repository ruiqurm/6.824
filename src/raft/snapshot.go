package raft

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

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex == rf.log.GetLastIncludedIndex() && lastIncludedTerm == rf.log.GetLastIncludedTerm() {
		return true
	}
	return false
	// Your code here (2D).
	// return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.log.GetLastIncludedIndex() >= index {
		rf.Debug(dSnapshot, "Snapshot %d ignore\n", index)
		return
	}
	// atomic.StoreInt32(&rf.is_appling, 1)
	rf.Debug(dSnapshot, "Snapshot %d\n", index)
	lastTerm := rf.log.Get(index).Term
	rf.log.Reindex(index, lastTerm)
	rf.snapshot = snapshot
	rf.persistL()
	rf.commitIndex = max(rf.commitIndex, index)
	rf.lastApplied = max(rf.lastApplied, index)
	UnblockWrite(rf.applyCond, true)
	// atomic.StoreInt32(&rf.is_appling, 0)
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
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.Debug(dSnapshot, "rejct InstallSnapshot(%v->%v)", args.LeaderId, rf.me)
		rf.mu.Unlock()
		return
	}
	rf.Debug(dSnapshot, "accept InstallSnapshot(%v->%v)", args.LeaderId, rf.me)
	// is send by current leader
	rf.setElectionTimeL() // refresh timer
	if rf.state != FOLLOWER {
		rf.asFollowerL(args.Term)
		rf.votedFor = args.LeaderId
	}
	if rf.log.GetLastIncludedIndex() >= args.LastIncludedIndex {
		rf.Debug(dSnapshot, "Snapshot %d ignore\n", args.LastIncludedIndex)
		// atomic.StoreInt32(&rf.waitSnapshot, 0)
		rf.mu.Unlock()
		return
	}
	rf.snapshot = args.Data
	rf.log.Reindex(args.LastIncludedIndex, args.LastIncludedTerm)
	rf.persistL()
	rf.Debug(dSnapshot, "apply snapshot:index=%v,term:%v\n", args.LastIncludedIndex, args.LastIncludedTerm)
	if rf.lastApplied < args.LastIncludedIndex {
		msg := ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Data,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
		rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
		rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)
		// atomic.StoreInt32(&rf.waitSnapshot, 1)
		rf.waitSnapshot.Lock()
		rf.mu.Unlock()
		rf.applyCh <- msg
		rf.waitSnapshot.Unlock()
		UnblockWrite(rf.applyCond, true)
	} else {
		rf.mu.Unlock()
	}
}
