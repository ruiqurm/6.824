package kvraft

import (
	"time"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrStale       = "ErrStale"
	ErrTimeout     = "ErrTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Sf Snowflake
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Sf Snowflake
}

type GetReply struct {
	Err   Err
	Value string
}

type StateArgs struct {
}

type StateReply struct {
	Leader int
	Me     int
}

type Snowflake = int64

// Timestamp int64 // 41 bit
// Client    int16 // 10 bit
// Id        int16 // 12 bit

func make_snowflake(client_id int16) Snowflake {
	// will not use id
	return (time.Now().UnixNano() << 23 >> 1) |
		(int64(client_id) << 12)
	// |id // now without id
}
func get_timestamp_from_snowflake(sf Snowflake) int64 {
	return sf & (0x1fffffffffc00000)
}
func get_client_from_snowflake(sf Snowflake) int16 {
	return int16((sf & (0x3ff000)) >> 12)
}
