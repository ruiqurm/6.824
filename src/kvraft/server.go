package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	ACTION_PUT = iota
	ACTION_APPEND
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Action int
	Key    string
	Value  string
	Time   int64
}
type ValueStruct struct {
	Value      string
	ChangeTime int64
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data        map[string]*ValueStruct
	commitIndex int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	res, ok := kv.data[args.Key]
	if ok {
		reply.Value = res.Value
		kv.Debug(SGET, "GET [%v]= %v,time=%v", args.Key, res.Value, res.ChangeTime)
	} else {
		reply.Err = ErrNoKey
		kv.Debug(SGET, "GET [%v] NOT FOUND", args.Key)
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	var action int
	if args.Op == "Put" {
		action = ACTION_PUT
	} else if args.Op == "Append" {
		action = ACTION_APPEND
	} else {
		panic("unknown op")
	}
	now := time.Now().UnixMilli()
	index, _, isLeader := kv.rf.Start(Op{action, args.Key, args.Value, now})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if action == ACTION_PUT {
		kv.Debug(SSET, "index=%v,PUT [%v]= %v,time=%v", index, args.Key, args.Value, now)
	} else {
		kv.Debug(SSET, "index=%v,APPEND [%v] = %v,time=%v", index, args.Key, args.Value, now)
	}

	// wait for servers to commit the message
	for {
		time.Sleep(100 * time.Millisecond)
		if atomic.LoadInt64(&kv.commitIndex) >= int64(index) {
			// have been commited
			break
		}
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if s, ok := kv.data[args.Key]; ok {
		if s.ChangeTime == now {
			reply.Err = OK
			return
		}
	}
	kv.Debug(SSET, "failed")
	reply.Err = ErrWrongLeader
}

func (kv *KVServer) applyLoop() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				if int64(msg.CommandIndex) > atomic.LoadInt64(&kv.commitIndex) {
					atomic.StoreInt64(&kv.commitIndex, int64(msg.CommandIndex))
					kv.mu.Lock()
					op := msg.Command.(Op)
					kv.Debug(SAPL, "ack msg [%v]= %v,timestamp=%v", op.Key, op.Value, op.Time)
					if s, ok := kv.data[op.Key]; !ok {
						kv.data[op.Key] = &ValueStruct{op.Value, op.Time}
					} else {
						if op.Action == ACTION_PUT {
							s.Value = op.Value
						} else if op.Action == ACTION_APPEND {
							s.Value = concat(s.Value, op.Value)
						}
						s.ChangeTime = op.Time
					}
					kv.mu.Unlock()
				}
			} else if msg.SnapshotValid {

			}
		case <-time.After(time.Millisecond * 1000):
			// timeout
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	// You may need initialization code here.
	kv.data = make(map[string]*ValueStruct)
	go kv.applyLoop()
	return kv
}
