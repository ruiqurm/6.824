package raft

type Log struct {
	lastIndex int
	lastTerm  int
	log       []LogEntry
}

type LogEntry struct {
	Term    int
	Command interface{}
}

func (l *Log) GetLog() []LogEntry {
	return l.log
}
func NewLog(lastIndex int, lastTerm int, logs []LogEntry) *Log {
	return &Log{lastIndex, lastTerm, logs}
}

// func RecoverLog(logs []LogEntry) *Log {
// 	return &Log{log: logs}
// }
func (l *Log) Len() int {
	return len(l.log)
}

func (l *Log) Get(index int) (e *LogEntry) {
	if index <= l.lastIndex {
		panic("attempt to get from empty log")
	}
	return &l.log[index-1-l.lastIndex]
}
func (l *Log) GetMany(index int) (e []LogEntry) {
	if index-l.lastIndex > len(l.log) {
		// may be no longer leader
		return nil
	}
	right := min(len(l.log), index-l.lastIndex-1+MAX_LOG_PER_REQUEST)
	return l.log[index-1-l.lastIndex : right]
}
func (l *Log) LatestTerm() (term int) {
	if len(l.log) == 0 {
		return l.lastTerm
	} else {
		return l.log[l.LatestIndex()-1-l.lastIndex].Term
	}
}

func (l *Log) LatestIndex() int {
	return len(l.log) + l.lastIndex
}

func (l *Log) Cut(index int) bool {
	if index-l.lastIndex <= len(l.log) {
		l.log = l.log[:index-1-l.lastIndex]
		return true
	}
	return false
}
func (l *Log) Reindex(lastIndex int, lastTerm int) {
	if lastIndex >= l.lastIndex && lastIndex <= l.LatestIndex() {
		l.log = l.log[lastIndex-l.lastIndex:] // lastIndex -l.lastIndex - 1 + 1
	} else {
		l.log = []LogEntry{}
	}
	// Log_infof("reindex;Index = %v,Term = %v,len=%v", lastIndex, lastTerm, len(l.log))
	l.lastIndex = lastIndex
	l.lastTerm = lastTerm
}
func (l *Log) Append(e ...LogEntry) {
	l.log = append(l.log, e...)
}

func (l *Log) GetTerm(index int) int {
	if index < 1 {
		return 0
	} else if index == l.lastIndex {
		return l.lastTerm
	} else if index-l.lastIndex > len(l.log) {
		// may be no longer leader
		return -1
	} else {
		return l.log[index-1-l.lastIndex].Term
	}
}

func (l *Log) GetLastIncludedIndex() int {
	return l.lastIndex
}

func (l *Log) GetLastIncludedTerm() int {
	return l.lastTerm
}

func (rf *Raft) updateL() {
	// scan matchIndex and update commitIndex
	for i := rf.commitIndex + 1; i <= rf.log.LatestIndex(); i++ {
		count := 0
		if i != 1 && rf.log.GetTerm(i) != rf.currentTerm {
			continue
		}
		for j := 0; j < len(rf.peers); j++ {
			if rf.matchIndex[j] >= i && j != rf.me {
				count++
			}
			if count >= len(rf.peers)/2 {

				rf.commitIndex = i // commited successfully
				rf.applyCond.Broadcast()

				break
			}
		}
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.applyCond.L.Lock()
		rf.applyCond.Wait()
		rf.applyCond.L.Unlock()
		if rf.killed() {
			break
		}
		rf.mu.Lock()
		rf.applyMsg()
		rf.mu.Unlock()
	}

}

func (rf *Raft) applyMsg() {
	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			rf.Debug(dApply, "apply msg(index=%v,term=%v)", i, rf.log.Get(i).Term)
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.log.Get(i).Command,
				CommandIndex: i,
			}
			rf.applyCh <- msg
			rf.lastApplied = i
		}

	}
}

func (rf *Raft) heartBeat() {
	// should call with lock
	me := rf.me
	n := len(rf.peers)
	// Log_debugf("[%v] leader send heartbeat\n", rf.me)
	for server := 0; server < n; server++ {
		if server != me {
			go rf.sendAppendEntries(server)
		}
	}
}
