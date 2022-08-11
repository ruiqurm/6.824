package kvraft

import (
	"math/rand"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu           sync.Mutex
	leader       int
	id           int
	index2server []int
}

// func nrand() int64 {
// 	max := big.NewInt(int64(1) << 62)
// 	bigx, _ := rand.Int(rand.Reader, max)
// 	x := bigx.Int64()
// 	return x
// }

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	rand.Seed(time.Now().UnixNano())
	ck.leader = -1
	ck.index2server = make([]int, len(servers))
	ck.id = int(rand.Intn(1024))
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	serverIndex := ck.findLeaderL()
	server := ck.index2server[serverIndex]
	ck.mu.Unlock()
	ck.Debug(CGET, "GET %v", key)
	reply := GetReply{}
	for {
		ok := ck.servers[server].Call("KVServer.Get", &GetArgs{Key: key, Sf: make_snowflake(int16(ck.id))}, &reply)
		if ok {
			switch reply.Err {
			case OK:
				ck.Debug(CGET, "GET [%v]=%v", key, reply.Value)
				return reply.Value
			case ErrWrongLeader:
				ck.mu.Lock()
				ck.resetLeaderL()
				serverIndex = ck.findLeaderL()
				server = ck.index2server[serverIndex]
				ck.mu.Unlock()
			}
		}
		ck.Debug(CGET, "failed to get key=%v because of rpc error", key)
		time.Sleep(time.Microsecond * 100)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	serverIndex := ck.findLeaderL()
	server := ck.index2server[serverIndex]
	ck.mu.Unlock()
	args := PutAppendArgs{key, value, op, make_snowflake(int16(ck.id))}
	for {
		reply := PutAppendReply{}
		ck.Debug(CSET, "put append to %v,[%v]=%v", serverIndex, key, value)
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			switch reply.Err {
			case OK:
				ck.Debug(CSET, "successfully put append key %v to server %v", key, serverIndex)
				return
			case ErrStale:
				ck.Debug(CSET, "redundantly put key (%v) to server %v", key, serverIndex)
				return
			case ErrWrongLeader, ErrTimeout:
				ck.Debug(CSET, "failed to put key (%v) to %v server(wrong leader)", key, serverIndex)
				ck.mu.Lock()
				ck.resetLeaderL()
				serverIndex = ck.findLeaderL()
				server = ck.index2server[serverIndex]
				ck.mu.Unlock()
			default:
				ck.Debug(CSET, "panic on 'put append to %v,[%v]=%v'", serverIndex, key, value)
				panic("Unknown Error type")
			}
		} else {
			// failed because of network reason
			ck.Debug(CSET, "failed to put key=%v to server %v (rpc error)", key, serverIndex)
			time.Sleep(time.Microsecond * 100)
		}
	}
}
func (ck *Clerk) resetLeaderL() {
	ck.leader = -1
}
func (ck *Clerk) findLeaderL() int {
	if ck.leader != -1 {
		return ck.leader
	}
	vote_count := make([]int, len(ck.servers))
	mu := sync.Mutex{}
	args := StateArgs{}
	wg := sync.WaitGroup{}
	for {
		mu.Lock()
		for server := range ck.servers {
			vote_count[server] = 0
			wg.Add(1)
			go func(server int, mu *sync.Mutex) {
				reply := StateReply{}
				ok := ck.servers[server].Call("KVServer.State", &args, &reply)
				if ok {
					if reply.Leader != -1 {
						mu.Lock()
						ck.index2server[reply.Me] = server
						vote_count[reply.Leader]++
						mu.Unlock()
					}
				}
				wg.Done()
			}(server, &mu)
		}
		mu.Unlock()
		wg.Wait()
		max_one := -1
		max_result := 0
		for server := range ck.servers {
			if vote_count[server] > 0 && vote_count[server] > max_result {
				max_one = server
				max_result = vote_count[server]
			}
		}
		if max_one != -1 {
			// found a leader
			ck.Debug(CFIN, "found a leader: %v", max_one)
			ck.leader = max_one
			return max_one
		}
		time.Sleep(100 * time.Millisecond)
	}
}
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
