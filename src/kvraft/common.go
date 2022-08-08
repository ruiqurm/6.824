package kvraft

import (
	"math/rand"
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

type Snowflake struct {
	Timestamp int64 // 41 bit
	Client    int16 // 10 bit
	Id        int16 // 12 bit
}

func make_snowflake(client_id int16) Snowflake {
	return Snowflake{
		Timestamp: time.Now().UnixNano() << 23 >> 23,
		Client:    client_id,
		Id:        int16(rand.Intn(2048)),
	}
}
