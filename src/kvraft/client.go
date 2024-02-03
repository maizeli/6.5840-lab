package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
	"6.5840/util"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	DB      map[string]string
	DBMutex sync.Mutex

	lastServer int32
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
	return ck
}

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
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := &GetArgs{
		Key:       key,
		TimeStamp: time.Now().UnixNano(),
	}

	server := ck.lastServer
	for {
		reply := &GetReply{}
		util.Logger.Printf("ck begin call kv[%v] Get args=%v", server, util.JSONMarshal(args))
		ck.servers[server].Call("KVServer.Get", args, reply)
		if len(reply.Err) != 0 {
			server = (server + 1) % int32(len(ck.servers))
			time.Sleep(time.Millisecond * 10)
		} else {
			atomic.StoreInt32(&ck.lastServer, server)
			return reply.Value
		}
	}

	return "v_v"
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := &PutAppendArgs{
		Key:       key,
		Op:        op,
		Value:     value,
		TimeStamp: time.Now().UnixNano(),
	}

	server := atomic.LoadInt32(&ck.lastServer)
	for {
		reply := &PutAppendReply{}
		util.Logger.Printf("ck begin call kv[%v] PutAppend args=%v", server, util.JSONMarshal(args))
		ck.servers[server].Call("KVServer.PutAppend", args, reply)
		util.Logger.Printf("ck begin call kv[%v] PutAppend reply=%v", server, util.JSONMarshal(reply))
		if len(reply.Err) != 0 {
			util.Logger.Printf("ck reply err[%v]", reply.Err)
			server = (server + 1) % int32(len(ck.servers))
			// time.Sleep(time.Millisecond * 10)
		} else {
			atomic.StoreInt32(&ck.lastServer, server)
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
