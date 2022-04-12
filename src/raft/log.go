package raft

type Log struct {
	log []LogEntry
}

type LogEntry struct {
	Term    int
	Command interface{}
}

func NewLog() *Log {
	return &Log{[]LogEntry{}}
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
	return l.log[index-1:]
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
	l.log = l.log[:index-1]
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
