package kvraft

import (
	"time"
)

type Snowflake = int64

// Timestamp int64 // 42 bit,millsecond
// Id        int16 // 11 bit

// configuration
const SNOWFLAKE_TS_BITS int = 42
const SNOWFLAKE_SERIAL_ID_BITS int = 21

// const SNOWFLAKE_CLIENT_BITS int = 25

// induction
const SNOWFLAKE_MAX_SERIAL_ID = (1 << SNOWFLAKE_SERIAL_ID_BITS) - 1

const SNOWFLAKE_TS_SHIFT = SNOWFLAKE_SERIAL_ID_BITS
const SNOWFLAKE_TS_MASK int64 = ((^0) << SNOWFLAKE_SERIAL_ID_BITS) & (0x7fffffffffffffff)
const SNOWFLAKE_SERIAL_ID_MASK int64 = ((^0) << SNOWFLAKE_SERIAL_ID_BITS) ^ (^0)

var snowflake_serial_id int32

// var snowflake_time int64

func init_snowflake() {
	snowflake_serial_id = 0
	// snowflake_time = 0

}
func make_snowflake() Snowflake {
	now := time.Now().UnixMilli()
	// fmt.Println(now)
	// if now == snowflake_time {
	// 	atomic.AddInt32(&snowflake_serial_id, 1)
	// 	// if snowflake_serial_id > SNOWFLAKE_MAX_SERIAL_ID {
	// 	// panic("serial id out of range")
	// 	// }
	// } else {
	// 	snowflake_time = now
	// 	atomic.StoreInt32(&snowflake_serial_id, 0)
	// }
	return (now << SNOWFLAKE_SERIAL_ID_BITS)
}
func get_timestamp_from_snowflake(sf Snowflake) int64 {
	return (sf & (SNOWFLAKE_TS_MASK)) >> SNOWFLAKE_SERIAL_ID_BITS
}
func get_id_from_snowflake(sf Snowflake) int {
	return int(sf & SNOWFLAKE_SERIAL_ID_MASK)
}
