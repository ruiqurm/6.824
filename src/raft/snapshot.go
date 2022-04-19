package raft

import "fmt"

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

	// Your code here (2D).
	return true
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
		lastTerm := rf.log.Get(index).Term
		rf.log.Reindex(index, lastTerm)
		args := InstallSnapshotArgs{}
		args.Term = rf.currentTerm
		args.Data = snapshot
		args.LastIncludedIndex = index
		args.LastIncludedTerm = rf.log.GetLastIncludedTerm()
		args.LeaderId = rf.me
		rf.snapshot = append(rf.snapshot, snapshot...)
		rf.persistL()
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go rf.SendInstallSnapshot(&args, i)
			}
		}
		// rf.applyCh <- ApplyMsg{
		// 	SnapshotValid: true,
		// 	Snapshot:      args.Data,
		// 	SnapshotTerm:  args.LastIncludedTerm,
		// 	SnapshotIndex: args.LastIncludedIndex,
		// }
		fmt.Printf("Snapshot %d DONE\n", index)

	}()

}
func (rf *Raft) SendInstallSnapshot(args *InstallSnapshotArgs, server int) {
	rf.mu.Lock()
	if rf.state != LEADER {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	for {
		reply := InstallSnapshotReply{}
		ok := rf.peers[server].Call("Raft.InstallSnapshot", args, &reply)
		if ok {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.asFollowerL(reply.Term)
				rf.persistL()
				rf.mu.Unlock()
				return
			}
			rf.matchIndex[server] = max(rf.matchIndex[server], args.LastIncludedIndex)
			rf.nextIndex[server] = max(rf.nextIndex[server], args.LastIncludedIndex+1)
			rf.mu.Unlock()
			return
		}
	}

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.Log_infofL("recv;InstallSnapshot")
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	// is send by current leader
	rf.setElectionTimeL() // refresh timer
	rf.asFollowerL(args.Term)
	rf.votedFor = args.LeaderId
	rf.snapshot = append(rf.snapshot, args.Data...)
	rf.log.Reindex(args.LastIncludedIndex, args.LastIncludedTerm)
	rf.persistL()
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)
}
