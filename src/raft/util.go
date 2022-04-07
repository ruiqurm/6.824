package raft

import (
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
	WarnLevel
)

var (
	debugLog = log.New(os.Stdout, "\033[36m[debug]\033[0m ", log.LstdFlags|log.Lmicroseconds|log.Lshortfile)
	infoLog  = log.New(os.Stdout, "\033[34m[info ]\033[0m ", log.LstdFlags|log.Lshortfile)
	warnLog  = log.New(os.Stdout, "\033[31m[warn ]\033[0m ", log.LstdFlags|log.Lshortfile)
	loggers  = []*log.Logger{debugLog, infoLog, warnLog}
	mu       sync.Mutex
)

var (
	Log_debug  = debugLog.Println
	Log_debugf = debugLog.Printf
	Log_info   = infoLog.Println
	Log_infof  = infoLog.Printf
	Log_warn   = warnLog.Println
	Log_warnf  = warnLog.Printf
)

func SetLevel(level int) {
	mu.Lock()
	defer mu.Unlock()

	for _, logger := range loggers {
		logger.SetOutput(os.Stdout)
	}
	if level > WarnLevel {
		warnLog.SetOutput(ioutil.Discard)
		infoLog.SetOutput(ioutil.Discard)
		debugLog.SetOutput(ioutil.Discard)
	} else if level > InfoLevel {
		infoLog.SetOutput(ioutil.Discard)
		debugLog.SetOutput(ioutil.Discard)
	} else if level > DebugLevel {
		debugLog.SetOutput(ioutil.Discard)
	}

}
