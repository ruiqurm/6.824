package raft

import (
	"fmt"
	"math/rand"
	"sync"
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
	rf.backoff = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.log.LatestIndex() + 1 // leader last log index + 1
		rf.matchIndex[i] = 0
		rf.backoff[i] = 1
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
	args.LastLogIndex = rf.log.LatestIndex()
	args.LastLogTerm = rf.log.LatestTerm()
	Log_debugf("[%v] start a new election. total=%v,curTerm=%v,lastLogIndex=%v,lastLogTerm=%v,\n", rf.me, n, rf.currentTerm, args.LastLogIndex, args.LastLogTerm)
	// start a new election
	var vote int = 0
	var done int = 1
	cond := sync.Cond{L: &sync.Mutex{}}
	for server := 0; server < n; server++ {
		if server != me {
			go rf.sendRequestVote(server, &args, &vote, &done, &cond)
		}
	}
	go rf.leaderLoop(&cond)
	// go rf.log_replication()
}

func (rf *Raft) leaderLoop(cond *sync.Cond) {
	cond.L.Lock()
	cond.Wait()
	cond.L.Unlock()
	rf.mu.Lock()
	for rf.state == LEADER && !rf.killed() {
		rf.setElectionTime()
		rf.heartBeat()
		rf.mu.Unlock()
		time.Sleep(HEARTBEAT_INTERVAL * time.Millisecond)
		rf.mu.Lock()
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, vote *int, done *int, cond *sync.Cond) {
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
				cond.Broadcast()
			}
		}
	}
	*done += 1
	if *done == len(rf.peers) {
		cond.Broadcast()
	}
}

func (rf *Raft) sendAppendEntries(server int) {
	rf.mu.Lock()
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me
	args.LeaderCommit = rf.commitIndex

	prev_index := rf.nextIndex[server] - 1
	args.PrevLogTerm = rf.log.GetTerm(prev_index)
	args.PrevLogIndex = prev_index
	if rf.log.LatestIndex() > prev_index {
		// If follower does not have latest entries, send to them
		// For simplicity, we just send one log per time
		// Note here is "next index",so "equal" should include
		args.Entries = append(args.Entries, rf.log.GetMany(rf.nextIndex[server])...)
	}
	Log_debugf("[%v] send AE to %v,PrevLogIndex=%v,PrevLogTerm=%v,commitIndex=%v,withdata=%v\n", rf.me, server, args.PrevLogIndex, args.PrevLogTerm, rf.commitIndex, len(args.Entries) != 0)
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
				rf.matchIndex[server] = prev_index + len(args.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
			} else {
				rf.matchIndex[server] = prev_index
			}
			rf.backoff[server] = 1
		} else {
			// linear backoff
			// rf.nextIndex[server]--
			// exponential backoff
			if rf.backoff[server] < 1024 {
				rf.backoff[server] <<= 1
			}
			next_backoff := rf.nextIndex[server] - rf.backoff[server]
			if next_backoff < 1 {
				rf.nextIndex[server] = 1
			} else {
				rf.nextIndex[server] = next_backoff
			}
		}
		// scan matchIndex and update commitIndex
		for i := rf.commitIndex + 1; i <= rf.log.LatestIndex(); i++ {
			count := 0
			for j := 0; j < len(rf.peers); j++ {
				if rf.matchIndex[j] >= i && j != rf.me {
					count++
				}
				if count >= len(rf.peers)/2 {
					rf.commitIndex = i // commited successfully
					msg := ApplyMsg{
						CommandValid: true,
						Command:      rf.log.Get(i).Command,
						CommandIndex: i,
					}
					Log_infof("[%v] apply msg(index=%v,term=%v,command=%v)", rf.me, i, rf.log.Get(i).Term, rf.log.Get(i).Command)
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
