package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	me      int64
	// You will have to modify this struct.
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
	ck.me = time.Now().UnixNano()
	// You'll have to add code here.
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
	DPrintf("client get")
	time := time.Now().UnixNano()
	for {
		for i, _ := range ck.servers {
			args := &GetArgs{}
			reply := &GetReply{}
			args.Key = key
			args.Footprint = time
			args.Clerk = ck.me
			ok := ck.servers[i].Call("KVServer.Get", args, reply)
			if ok {
				if reply.Err == ErrNoKey {
					return ""
				} else if reply.Err == OK {
					return reply.Value
				}
			}
		}
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
	time := time.Now().UnixNano()
	for {
		for i, _ := range ck.servers {
			args := &PutAppendArgs{}
			reply := &PutAppendReply{}
			args.Key = key
			args.Value = value
			args.Op = op
			args.Clerk = ck.me
			args.Footprint = time
			ok := ck.servers[i].Call("KVServer.PutAppend", args, reply)
			if ok {
				if reply.Err == OK {
					return
				}
			}
		}
	}
	// You will have to modify this function.
}

func (ck *Clerk) Put(key string, value string) {
	DPrintf("client put")
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	DPrintf("client append")
	ck.PutAppend(key, value, "Append")
}
