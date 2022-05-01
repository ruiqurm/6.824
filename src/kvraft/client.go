package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu          sync.Mutex
	isLeader    []bool
	leaderCount int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.isLeader = make([]bool, len(ck.servers))
	for i := range ck.isLeader {
		ck.isLeader[i] = false
	}
	ck.leaderCount = 0
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
	chosen_server := nrand() % int64(len(ck.servers))
	DebugPrint(CACT, "GET")
	reply := GetReply{}
	time.Sleep(100 * time.Millisecond) // force to wait; will be replace later
	ok := ck.servers[chosen_server].Call("KVServer.Get", &GetArgs{Key: key}, &reply)
	if !ok || reply.Err != "" {
		return ""
	}
	return reply.Value
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
	args := PutAppendArgs{key, value, op}
	for {
		wg := sync.WaitGroup{}
		ck.mu.Lock()
		succ := false
		for server := range ck.servers {
			if ck.leaderCount > 0 && !ck.isLeader[server] {
				continue
			}
			wg.Add(1)
			go func(server int, succ *bool) {
				reply := PutAppendReply{}
				DebugPrint(CACT, "put append to %v,[%v]=%v", server, key, value)
				ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)

				ck.mu.Lock()
				defer wg.Done()
				defer ck.mu.Unlock()
				if ok {
					if reply.Err == OK {
						ck.leaderCount++
						ck.isLeader[server] = true
						*succ = true
						return
					}
				}
				if ck.isLeader[server] {
					ck.isLeader[server] = false
					ck.leaderCount--
				}
			}(server, &succ)
		}
		ck.mu.Unlock()
		wg.Wait()
		if succ {
			DebugPrint(CACT, "put append done")
			return
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
