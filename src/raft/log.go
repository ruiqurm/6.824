package raft

type Log struct {
	log []LogEntry
}

type LogEntry struct {
	Term    int
	Command interface{}
}

func (l *Log) GetLog() []LogEntry {
	return l.log
}
func NewLog() *Log {
	return &Log{[]LogEntry{}}
}
func RecoverLog(logs []LogEntry) *Log {
	return &Log{log: logs}
}
func (l *Log) Len() int {
	return len(l.log)
}

func (l *Log) Get(index int) (e *LogEntry) {
	if len(l.log) == 0 {
		panic("attempt to get from empty log")
	}
	return &l.log[index-1]
}
func (l *Log) GetMany(index int) (e []LogEntry) {
	if index > len(l.log) {
		panic("index out of range")
	}
	right := min(len(l.log), index-1+MAX_LOG_PER_REQUEST)
	return l.log[index-1 : right]
}
func (l *Log) LatestTerm() (term int) {
	if len(l.log) == 0 {
		return 0
	} else {
		return l.log[len(l.log)-1].Term
	}
}

func (l *Log) LatestIndex() int {
	return len(l.log)
}

func (l *Log) Cut(index int) {
	if index <= len(l.log) {
		l.log = l.log[:index-1]
	}
}

func (l *Log) Append(e ...LogEntry) {
	l.log = append(l.log, e...)
}

func (l *Log) GetTerm(index int) int {
	if index <= 1 {
		return 0
	} else {
		return l.log[index-1].Term
	}
}

func (rf *Raft) updateL() {
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
				rf.Log_importfL("apply msg(index=%v,term=%v)", i, rf.log.Get(i).Term)
				rf.applyCh <- msg
				rf.lastApplied = i
				break
			}
		}
		if rf.commitIndex != i {
			break
		}
	}
}
