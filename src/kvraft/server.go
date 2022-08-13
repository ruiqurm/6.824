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
	ACTION_GET
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Action        int
	Key           string
	Value         string
	Verified_code int64
	Client        int64
}

type WaitingStruct struct {
	Committed     chan bool
	Value         string
	Verified_code int64
	Client        int64
}
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
	waiting   map[int32]*WaitingStruct
	check_map map[int64]int64
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(Op{ACTION_GET, args.Key, "", args.Sf, args.Client})
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.Debug(SGET, "%v send getting [%v]", args.Client, args.Key)
	ws := &WaitingStruct{
		Committed: make(chan bool),
	}
	kv.waiting[int32(index)] = ws
	kv.mu.Unlock()
	select {
	case <-ws.Committed:
		if ws.Client != args.Client || ws.Verified_code != args.Sf {
			// commit failed(beacuse it's no longer leader)
			reply.Err = ErrWrongLeader
			kv.Debug(SGET, "get %v stale", args.Key)
		} else {
			reply.Err = OK
			reply.Value = ws.Value
			kv.Debug(SGET, "get %v done", args.Key)
		}

	case <-time.After(time.Millisecond * 2000):
		reply.Err = ErrTimeout
		kv.Debug(SGET, "timeout")
	}
	go func(i int) {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if item, ok := kv.waiting[int32(index)]; ok {
			if item == ws {
				delete(kv.waiting, int32(index))
			}
		}
		delete(kv.waiting, int32(i))
		kv.Debug(DEBG, "delete channel %v", i)
	}(index)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	// check and avoid client send same request twice
	kv.mu.Lock()
	client := args.Client
	timestamp := args.Sf
	if saved_ts, ok := kv.check_map[client]; ok && saved_ts >= timestamp {
		// if it is an old client and the request timestamp larger than the saved timestamp
		// set error type as ErrStale to terminate client loop.
		kv.Debug(SSET, "STALE; client=%v,key=%v,ts=%v, while old_ts=%v", client, args.Key, timestamp, saved_ts)
		kv.mu.Unlock()
		reply.Err = ErrStale
		return
	}
	// if it is a new client, no need to detect.
	// we will add the time stamp to the map later on.

	var action int
	if args.Op == "Put" {
		action = ACTION_PUT
	} else if args.Op == "Append" {
		action = ACTION_APPEND
	} else {
		panic("unknown op")
	}

	// call `Start` to commit a new Operation.
	index, _, isLeader := kv.rf.Start(Op{action, args.Key, args.Value, args.Sf, args.Client})

	// if it is not a leader, set error type as ErrWrongLeader to ask client to find real leader.
	if !isLeader {
		kv.mu.Unlock()
		kv.Debug(SSET, "Start failed; not a leader")
		reply.Err = ErrWrongLeader
		return
	}

	// set check_map and get a waiting channel, which will notify the server when it has handled by applier
	ws := &WaitingStruct{
		Committed: make(chan bool),
	}
	kv.waiting[int32(index)] = ws
	kv.mu.Unlock()
	kv.Debug(SSET, "index=%v,client %v %v [%v]= %v", index, client, args.Op, args.Key, args.Value)

	// wait for servers to commit the message
	select {
	case <-ws.Committed:
		if ws.Client != args.Client || ws.Verified_code != args.Sf {
			// commit failed(beacuse it's no longer leader)
			reply.Err = ErrWrongLeader
			kv.Debug(DEBG, "commit failed; client=%v,key=%v", client, args.Key)
		} else {
			reply.Err = OK
			kv.Debug(DEBG, "commit succ; client=%v,key=%v", client, args.Key)
		}
	case <-time.After(time.Millisecond * 2000):
		reply.Err = ErrTimeout
		kv.Debug(SSET, "client=%v,index=%v,time=%v,key=%v timeout; failed", args.Client, index, args.Sf, args.Key)
	}

	// delete the old key
	go func(i int) {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if item, ok := kv.waiting[int32(index)]; ok {
			if item == ws {
				delete(kv.waiting, int32(index))
			}
		}
		delete(kv.waiting, int32(i))
		kv.Debug(DEBG, "delete channel %v", i)
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
				client := op.Client
				timestamp := op.Verified_code
				stale := false
				ret := ""

				if op.Action == ACTION_APPEND || op.Action == ACTION_PUT {
					// must exist
					vc, ok := kv.check_map[client]
					if ok && vc >= timestamp {
						// stale
						stale = true
						kv.Debug(DEBG, "redundant commit;index=%v,client=%v,key=%v;check_map[%v]=%v,origin_ts=%v", msg.CommandIndex, client, op.Key, client, timestamp, vc)
					} else {
						kv.Debug(DEBG, "update check_map[%v]=%v", client, timestamp)
						kv.check_map[client] = timestamp
					}
				}
				if !stale {
					switch op.Action {
					case ACTION_PUT:
						kv.data[op.Key] = op.Value
						kv.Debug(SAPL, "ack msg set [%v]= %v", op.Key, op.Value)
					case ACTION_APPEND:
						if v, ok := kv.data[op.Key]; ok {
							kv.data[op.Key] = concat(v, op.Value)
						} else {
							kv.data[op.Key] = op.Value
						}
						kv.Debug(SAPL, "ack msg append [%v]= %v", op.Key, op.Value)
					case ACTION_GET:
						// do nothing
						if v, ok := kv.data[op.Key]; ok {
							ret = v
						}
						// else ret is empty string
						kv.Debug(SAPL, "ack msg get [%v]=%v", op.Key, ret)
					default:
						panic("unknow action")
					}
				}
				ws, ok := kv.waiting[int32(msg.CommandIndex)]
				if ok {
					ws.Client = op.Client
					ws.Value = ret
					ws.Verified_code = op.Verified_code
				} else {
					kv.Debug(SAPL, "not found index=%v DONE", msg.CommandIndex)
				}
				kv.mu.Unlock()
				if ok {
					close(ws.Committed)
					kv.Debug(SAPL, "index=%v DONE", msg.CommandIndex)
				}

			} else if msg.SnapshotValid {
			}
		case <-time.After(time.Millisecond * 2000): // avoid blocking
			// timeout
			// fmt.Printf("%v timeout!!!\n", kv.me)
		}
	}
	kv.Debug(SAPL, "%v be killed", kv.me)
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
	kv.waiting = make(map[int32]*WaitingStruct)
	kv.check_map = make(map[int64]int64)
	go kv.applier()
	return kv
}
