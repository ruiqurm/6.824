package kvraft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

func concat(a string, b string) string {
	new_str := make([]byte, len(a)+len(b))
	pos := copy(new_str, a)
	copy(new_str[pos:], b)
	return string(new_str)
}

type logTopic string

const (
	CACT logTopic = "CACT" // client action
	SSET logTopic = "SSET" // server set
	SGET logTopic = "SGET" // server get
	SAPL logTopic = "SAPL" // server append
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
	if debugVerbosity >= 0 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

func (kv *KVServer) Debug(topic logTopic, format string, a ...interface{}) {
	append_str := fmt.Sprintf(format, a...)
	DebugPrint(topic, "[%v] %s", kv.me, append_str)
}
