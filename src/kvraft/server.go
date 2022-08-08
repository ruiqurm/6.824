package kvraft

import (
	"fmt"
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
	ACTION_GET
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Action int
	Key    string
	Value  string
	// Time   int64
}

// type ValueStruct struct {
// 	Value      string
// 	ChangeTime int64
// }
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data map[string]string
	// commitIndex int32
	waiting   map[int32]chan string
	check_map map[int16]int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	index, _, isLeader := kv.rf.Start(Op{ACTION_GET, args.Key, ""})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.Debug(SGET, "client get %v", args.Key)
	kv.mu.Lock()
	ch := kv.getWaitingChanL(int32(index))
	kv.mu.Unlock()
	select {
	case v := <-ch:
		kv.Debug(SGET, "get %v done", args.Key)
		reply.Err = OK
		reply.Value = v
	case <-time.After(time.Millisecond * 2000):
		reply.Err = ErrWrongLeader
		kv.Debug(SGET, "timeout")
	}
	go func(i int) {
		// delete the old key
		kv.mu.Lock()
		defer kv.mu.Unlock()
		delete(kv.waiting, int32(i))
	}(index)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	// check and avoid client send same request twice
	kv.mu.Lock()
	if saved_ts, ok := kv.check_map[args.Sf.Client]; ok && saved_ts >= args.Sf.Timestamp {
		// if it is an old client and the request timestamp larger than the saved timestamp
		// set error type as ErrStale to terminate client loop.
		kv.mu.Unlock()
		reply.Err = ErrStale
		return
	}
	// if it is a new client, no need to detect.
	// we will add the time stamp to the map later on.
	kv.mu.Unlock()

	var action int
	if args.Op == "Put" {
		action = ACTION_PUT
	} else if args.Op == "Append" {
		action = ACTION_APPEND
	} else {
		panic("unknown op")
	}

	// call `Start` to commit a new Operation.
	index, _, isLeader := kv.rf.Start(Op{action, args.Key, args.Value})

	// if it is not a leader, set error type as ErrWrongLeader to ask client to find real leader.
	if !isLeader {
		kv.Debug(SSET, "Start failed; not a leader")
		reply.Err = ErrWrongLeader
		return
	}

	// set check_map and get a waiting channel, which will notify the server when it has handled by applier
	kv.mu.Lock()
	kv.check_map[args.Sf.Client] = args.Sf.Timestamp
	ch := kv.getWaitingChanL(int32(index))
	kv.mu.Unlock()
	if action == ACTION_PUT {
		kv.Debug(SSET, "index=%v,PUT [%v]= %v", index, args.Key, args.Value)
	} else {
		kv.Debug(SSET, "index=%v,APPEND [%v] = %v", index, args.Key, args.Value)
	}

	// wait for servers to commit the message
	select {
	case <-ch:
		reply.Err = OK
	case <-time.After(time.Millisecond * 2000):
		reply.Err = ErrTimeout
		kv.Debug(SSET, "set timeout; failed")
	}

	// delete the old key
	go func(i int) {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		delete(kv.waiting, int32(i))
	}(index)
}

func (kv *KVServer) State(args *StateArgs, reply *StateReply) {
	reply.Leader, reply.Me = kv.rf.GetLeader()
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				// if int32(msg.CommandIndex) > atomic.LoadInt32(&kv.commitIndex) {
				// 	atomic.StoreInt32(&kv.commitIndex, int32(msg.CommandIndex))
				kv.mu.Lock()
				op := msg.Command.(Op)

				ret := ""
				switch op.Action {
				case ACTION_PUT:
					kv.data[op.Key] = op.Value
					kv.Debug(SAPL, "ack msg  set [%v]= %v", op.Key, op.Value)
				case ACTION_APPEND:
					if v, ok := kv.data[op.Key]; ok {
						kv.data[op.Key] = concat(v, op.Value)
					} else {
						kv.data[op.Key] = op.Value
					}
					kv.Debug(SAPL, "ack msg  append [%v]= %v", op.Key, op.Value)
				case ACTION_GET:
					// do nothing
					if v, ok := kv.data[op.Key]; ok {
						ret = v
					}
					// else ret is empty string
					kv.Debug(SAPL, "ack msg  get[%v]=%v", op.Key, ret)
				default:
					panic("unknow action")
				}

				_, isLeader := kv.rf.GetState()
				if isLeader {
					ch := kv.getWaitingChanL(int32(msg.CommandIndex))
					kv.mu.Unlock()
					ch <- ret
				} else {
					kv.mu.Unlock()
				}
				// }
			} else if msg.SnapshotValid {
			}
		case <-time.After(time.Millisecond * 2000): // avoid blocking
			// timeout
			// fmt.Printf("%v timeout!!!\n", kv.me)
		}
	}
	fmt.Printf("%v killed!!!\n", kv.me)
}

func (kv *KVServer) getWaitingChanL(index int32) chan string {
	ch, ok := kv.waiting[index]
	if !ok {
		ch = make(chan string)
		kv.waiting[index] = ch
	}
	return ch

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
	kv.data = make(map[string]string)
	// kv.clients = make(map[int16]int64)
	kv.waiting = make(map[int32]chan string)
	kv.check_map = make(map[int16]int64)
	go kv.applier()
	return kv
}
