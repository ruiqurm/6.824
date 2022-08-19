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
	CFIN logTopic = "CFIN" // Client find server
	CSET logTopic = "CSET" // client set
	CGET logTopic = "CGET" // client get
	SSET logTopic = "SSET" // server set
	SGET logTopic = "SGET" // server get
	SAPL logTopic = "SAPL" // server append
	SSNA logTopic = "SSNA" // server snapshot
	DEBG logTopic = "DEBUG"
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
	fmt.Printf("debugVerbosity=%v\n", debugVerbosity)
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func DebugPrint(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 3 ||
		(debugVerbosity == 1 && (topic == CFIN || topic == CSET || topic == CGET)) ||
		(debugVerbosity == 2 && (topic == SSET || topic == SGET || topic == SAPL)) {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

func (kv *KVServer) Debug(topic logTopic, format string, a ...interface{}) {
	append_str := fmt.Sprintf(format, a...)
	DebugPrint(topic, "kvs[%v] %s", kv.me, append_str)
}
func (ck *Clerk) Debug(topic logTopic, format string, a ...interface{}) {
	append_str := fmt.Sprintf(format, a...)
	DebugPrint(topic, "client[%v] %s", ck.id, append_str)
}

// func abbreviate_string(data string) string {
// 	/// if string length is longer than 16, then return the last 16 characters
// 	m := len(data)
// 	if m >= 16 {
// 		return data[m-16 : m]
// 	} else {
// 		return data
// 	}
// }

// func make_snow_id(client_id int16) int64{
// 	/// snowflake algorithm
// 	return 0 | (time.Now().UnixNano() << 23 >> 1) | (int64(client_id) << 12) |  ()
// }
