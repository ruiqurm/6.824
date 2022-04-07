package raft

import (
	"fmt"
	"math/rand"
	"time"
)

func (rf *Raft) setElectionTime() {
	// should acquire lock
	rf.electionTime = time.Now().Add(time.Duration(rand.Int63()%(ELECTION_TIMEOUT_RANGE)+ELECTION_MIN_TIMEOUT) * time.Millisecond)
}

func (rf *Raft) asCandidate() {
	// should call with lock!
	rf.state = CANDIDATE
	rf.currentTerm += 1
	rf.votedFor = rf.me
}

func (rf *Raft) asLeader() {
	// should call with lock!
	rf.state = LEADER
	rf.votedFor = rf.me
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log) + 1 // leader last log index + 1
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) asFollower(term int) {
	// should call with lock!
	rf.state = FOLLOWER
	rf.currentTerm = term
}

// if timeout, start an election
func (rf *Raft) election() {
	// should call with lock!
	rf.asCandidate()
	me := rf.me
	n := len(rf.peers)
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = me
	if len(rf.log) == 0 {
		args.LastLogIndex = 0
		args.LastLogTerm = 0
	} else {
		args.LastLogIndex = len(rf.log) - 1
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
	}
	Log_debugf("[%v] start a new election. total=%v,curTerm=%v\n", rf.me, n, rf.currentTerm)
	// start a new election
	var vote int = 0
	for server := 0; server < n; server++ {
		if server != me {
			go rf.sendRequestVote(server, &args, &vote)
		}
	}
	// go rf.heartBeat()
	// go rf.log_replication()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, vote *int) {
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != CANDIDATE {
		return
	}
	if ok {
		if reply.Term > rf.currentTerm {
			rf.asFollower(reply.Term)
		} else if reply.VoteGranted {
			*vote += 1
			if *vote >= len(rf.peers)/2 {
				fmt.Println("[", rf.me, "]", "become leader vote=", *vote)
				rf.asLeader()
			}
		}
	}
}

func (rf *Raft) sendAppendEntries(server int) {
	rf.mu.Lock()
	var index_to_append int
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex

	prev_index := rf.nextIndex[server] - 1
	lastest_log_index := len(rf.log)
	if prev_index != 0 {
		// special judge if next_index is 0
		args.PrevLogTerm = rf.log[prev_index-1].Term
		args.PrevLogIndex = prev_index
		index_to_append = prev_index // prev_index + 1 -1
	} else {
		args.PrevLogTerm = 0
		args.PrevLogIndex = 0
		index_to_append = 0
	}
	if lastest_log_index > prev_index {
		// If follower does not have latest entries, send to them
		// For simplicity, we just send one log per time
		// Note here is "next index",so "equal" should include
		args.Entries = append(args.Entries, rf.log[index_to_append])
	}
	Log_debugf("[%v] send AE to %v,PrevLogIndex=%v,PrevLogTerm=%v,withdata=%v\n", rf.me, server, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries) != 0)
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.asFollower(reply.Term)
			return
		}
		if reply.Success {
			// If successful: update nextIndex and matchIndex for follower
			if len(args.Entries) > 0 {
				rf.matchIndex[server] = prev_index + 1
				rf.nextIndex[server]++
			} else {
				rf.matchIndex[server] = prev_index
			}
		} else {
			rf.nextIndex[server]--
		}

		// scan matchIndex and update commitIndex
		for i := rf.commitIndex + 1; i <= len(rf.log); i++ {
			count := 0
			for j := 0; j < len(rf.peers); j++ {
				if rf.matchIndex[j] >= i && j != rf.me {
					count++
				}
				if count >= len(rf.peers)/2 {
					rf.commitIndex = i // commited successfully
					msg := ApplyMsg{
						CommandValid: true,
						Command:      rf.log[i-1].Command,
						CommandIndex: i,
					}
					rf.applyCh <- msg
					rf.lastApplied = i
					break
				}
			}
			if rf.commitIndex != i {
				break
			}
		}
		// Log_debugf("[%v] commit=%v,last_applied=%v\n", rf.me, rf.commitIndex, rf.lastApplied)
	}
	// wg.Done()
}
