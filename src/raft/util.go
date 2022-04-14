package raft

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"sync"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	DebugLevel = iota
	InfoLevel
	ImportantLevel
)

var (
	debugLog     = log.New(os.Stdout, "\033[36m[debug]\033[0m ", log.Ltime|log.Lmicroseconds)
	infoLog      = log.New(os.Stdout, "\033[34m[info ]\033[0m ", log.Ltime|log.Lmicroseconds)
	importantLog = log.New(os.Stdout, "\033[31m[important ]\033[0m ", log.Ltime|log.Lmicroseconds)
	loggers      = []*log.Logger{debugLog, infoLog, importantLog}
	mu           sync.Mutex
)

var (
	Log_debug   = debugLog.Println
	Log_debugf  = debugLog.Printf
	Log_info    = infoLog.Println
	Log_infof   = infoLog.Printf
	Log_import  = importantLog.Println
	Log_importf = importantLog.Printf
)

func SetLevel(level int) {
	mu.Lock()
	defer mu.Unlock()

	for _, logger := range loggers {
		logger.SetOutput(os.Stdout)
	}
	if level > ImportantLevel {
		importantLog.SetOutput(ioutil.Discard)
		infoLog.SetOutput(ioutil.Discard)
		debugLog.SetOutput(ioutil.Discard)
	} else if level > InfoLevel {
		infoLog.SetOutput(ioutil.Discard)
		debugLog.SetOutput(ioutil.Discard)
	} else if level > DebugLevel {
		debugLog.SetOutput(ioutil.Discard)
	}

}
func (rf *Raft) Log_infofL(format string, a ...interface{}) {
	var state string
	if rf.state == FOLLOWER {
		state = "F"
	} else if rf.state == CANDIDATE {
		state = "C"
	} else if rf.state == LEADER {
		state = "L"
	}
	append_str := fmt.Sprintf(format, a...)
	Log_infof("[%v,term=%v,log=%v,%v]%v", rf.me, rf.currentTerm, rf.log.Len(), state, append_str)
}
func (rf *Raft) Log_debugfL(format string, a ...interface{}) {
	var state string
	if rf.state == FOLLOWER {
		state = "F"
	} else if rf.state == CANDIDATE {
		state = "C"
	} else if rf.state == LEADER {
		state = "L"
	}
	append_str := fmt.Sprintf(format, a...)
	Log_debugf("[%v,term=%v,log=%v,%v]%v", rf.me, rf.currentTerm, rf.log.Len(), state, append_str)
}

func (rf *Raft) Log_importfL(format string, a ...interface{}) {
	var state string
	if rf.state == FOLLOWER {
		state = "F"
	} else if rf.state == CANDIDATE {
		state = "C"
	} else if rf.state == LEADER {
		state = "L"
	}
	append_str := fmt.Sprintf(format, a...)
	Log_importf("[%v,term=%v,log=%v,%v]%v", rf.me, rf.currentTerm, rf.log.Len(), state, append_str)
}
