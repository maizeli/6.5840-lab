package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/util"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ArgsType int32

var (
	ArgsType_PutAppend ArgsType = 1
	ArgsType_Get       ArgsType = 2
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ArgsType      ArgsType
	PutAppendArgs *PutAppendArgs
	GetArgs       *GetArgs

	GetValue string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	Data          map[string]string
	dataMutex     sync.Mutex
	Callback      map[int][]chan *Op // key=index
	callbackMutex sync.Mutex
	cache         map[string]struct{}
	cacheMutex    sync.Mutex
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	util.Logger.Printf("kv[%v] recv Get args=%v", kv.me, util.JSONMarshal(args))
	defer util.Logger.Printf("kv[%v] Get reply=%v", kv.me, util.JSONMarshal(reply))
	// Your code here.
	op := &Op{
		ArgsType: ArgsType_Get,
		GetArgs:  args,
	}
	idx, _, isLeader := kv.rf.Start(*op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	ch := make(chan *Op)
	kv.callbackMutex.Lock()
	kv.Callback[idx] = append(kv.Callback[idx], ch)
	kv.callbackMutex.Unlock()
	for op := range ch {
		switch op.ArgsType {
		case ArgsType_Get:
			if args.TimeStamp != op.GetArgs.TimeStamp {
				reply.Err = ErrCommandNotCommit
				return
			}
			reply.Value = op.GetValue
			return
		default:
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	st := time.Now()
	// util.TestLogger.Printf("kv[%v] recv PutAppend args=%v", kv.me, util.JSONMarshal(args))
	// Your code here.
	op := &Op{
		ArgsType:      ArgsType_PutAppend,
		PutAppendArgs: args,
	}
	if kv.hitCache(op) {
		util.Logger.Printf("kv[%v] PutAppend hit cache", kv.me)
		return
	}
	idx, _, isLeader := kv.rf.Start(*op)
	if !isLeader {
		util.Logger.Printf("kv[%v] PutAppend not leader", kv.me)
		reply.Err = ErrWrongLeader
		return
	}
	util.Logger.Printf("kv[%v] PutAppend is leader", kv.me)
	ch := make(chan *Op)
	kv.callbackMutex.Lock()
	kv.Callback[idx] = append(kv.Callback[idx], ch)
	kv.callbackMutex.Unlock()
	for op := range ch {
		switch op.ArgsType {
		case ArgsType_PutAppend:
			if args.TimeStamp != op.PutAppendArgs.TimeStamp {
				util.Logger.Printf("kv[%v] PutAppend args.TimeStamp!=op.PutAppendArgs.TimeStamp, args=%v, op.PutAppendArgs=%v", kv.me, util.JSONMarshal(args), util.JSONMarshal(op.PutAppendArgs))
				reply.Err = ErrCommandNotCommit
				return
			}
			// et := time.Now()
			// util.TestLogger.Printf("PutAppend[%v] cost %v ms", kv.me, et.Sub(st).Milliseconds())
			util.Logger.Printf("kv[%v] PutAppend success, reply=%v", kv.me, util.JSONMarshal(reply))
			return
		default:
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.Data = make(map[string]string)
	kv.Callback = make(map[int][]chan *Op)
	kv.cache = make(map[string]struct{})

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.apply()
	// Start一个空的，初始化下快照
	// kv.rf.Start(&Op{
	// 	ArgsType: ArgsType(0),
	// })
	// TODO 应该要针对Snapshot中的Log初始化下kv.Data

	return kv
}

func (kv *KVServer) apply() {
	for msg := range kv.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)
			switch op.ArgsType {
			case ArgsType_Get:
				kv.dataMutex.Lock()
				op.GetValue = kv.Data[op.GetArgs.Key]
				kv.dataMutex.Unlock()
				kv.callback(msg.CommandIndex, &op)
			case ArgsType_PutAppend:
				kv.dataMutex.Lock()
				if op.PutAppendArgs.Op == "Put" {
					kv.Data[op.PutAppendArgs.Key] = op.PutAppendArgs.Value
				} else if op.PutAppendArgs.Op == "Append" {
					kv.Data[op.PutAppendArgs.Key] += op.PutAppendArgs.Value
				}
				kv.dataMutex.Unlock()
				// 缓存住
				kv.writeCache(&op)
				kv.callback(msg.CommandIndex, &op)
			default:
				continue
			}
		} else if msg.SnapshotValid {

		} else {

		}
	}
}

// writeCache 仅cache PutAppend
func (kv *KVServer) writeCache(op *Op) {
	if op == nil || op.ArgsType != ArgsType_PutAppend {
		return
	}
	kv.cacheMutex.Lock()
	defer kv.cacheMutex.Unlock()
	key := fmt.Sprintf("%v#%v#%v", op.PutAppendArgs.Op, op.PutAppendArgs.ClientIdx, op.PutAppendArgs.TimeStamp)
	kv.cache[key] = struct{}{}
}

func (kv *KVServer) hitCache(op *Op) bool {
	if op == nil || op.ArgsType != ArgsType_PutAppend {
		return false
	}
	kv.cacheMutex.Lock()
	defer kv.cacheMutex.Unlock()
	key := fmt.Sprintf("%v#%v#%v", op.PutAppendArgs.Op, op.PutAppendArgs.ClientIdx, op.PutAppendArgs.TimeStamp)
	_, ok := kv.cache[key]
	if !ok {
		return false
	}

	return true
}

// callback 回调函数
func (kv *KVServer) callback(idx int, op *Op) {
	kv.callbackMutex.Lock()
	chList := kv.Callback[idx]
	kv.callbackMutex.Unlock()

	for _, ch := range chList {
		ch := ch
		go func() {
			ch <- op
		}()
	}
}
