package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type logTopic string

const (
	dNextIndex   logTopic = "NEXT"
	dSnapshot    logTopic = "SNAP"
	dApply       logTopic = "APPL"
	dPersist     logTopic = "PERS"
	dAppendEntry logTopic = "APPE"
	dRequestVote logTopic = "REQV"
	dLog         logTopic = "LOGS"
	dOther       logTopic = "OTHE"
)

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func DebugPrint(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

func (rf *Raft) Debug(topic logTopic, format string, a ...interface{}) {
	var state string
	if rf.state == FOLLOWER {
		state = "F"
	} else if rf.state == CANDIDATE {
		state = "C"
	} else if rf.state == LEADER {
		state = "L"
	}
	append_str := fmt.Sprintf(format, a...)
	DebugPrint(topic, "[%v] t=%v,log=%v,len=%v,%v %v", rf.me, rf.currentTerm, rf.log.LatestIndex(), rf.log.Len(), state, append_str)
}

func (rf *Raft) report_indexL() {
	if rf.state != LEADER {
		return
		// panic("report_indexL called by non-leader")
	}
	var sb strings.Builder
	sb.WriteString("index\t")
	for i := 0; i < len(rf.peers); i++ {
		sb.WriteString(fmt.Sprintf("\t%v", i))
	}
	sb.WriteString("\n")
	sb.WriteString("nextIndex:")
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			sb.WriteString(fmt.Sprintf("\t%v ", rf.nextIndex[i]))
		} else {
			sb.WriteString(fmt.Sprintf("\tNaN "))
		}
	}
	sb.WriteString("\n")
	DebugPrint(dNextIndex, "%v\n%v", rf.me, sb.String())
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func bineary_search(array []int, value int) int {
	l := 0
	r := len(array)
	for l < r {
		mid := ((r - l) / 2) + l
		if array[mid] < value {
			l = mid + 1
		} else {
			r = mid
		}
	}
	return l
}
