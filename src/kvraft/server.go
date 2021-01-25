package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const (
	GET    = "Get"
	PUT    = "Put"
	APPEND = "Append"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Res GetReply

// type Res struct {
// 	Err   Err
// 	Value string
// }

type Op struct {
	Op        string
	Key       string
	Value     string
	Footprint int64
	Clerk     int64

	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	database map[string]string

	opChannels map[int]chan Res

	footprint map[int64]int64

	// Your definitions here.
}

func (kv *KVServer) deleteChannel(ind int) {
	kv.mu.Lock()
	delete(kv.opChannels, ind)
	kv.mu.Unlock()
}

func (kv *KVServer) getChannel(ind int) (chan Res, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch, ok := kv.opChannels[ind]
	return ch, ok
}

func (kv *KVServer) newChannel(ind int) chan Res {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.opChannels[ind] = make(chan Res)
	return kv.opChannels[ind]
}

func (kv *KVServer) timeout(index int) {
	time.Sleep(1000 * time.Millisecond)
	res := Res{ErrWrongLeader, ""}
	ch, ok := kv.getChannel(index)
	if ok {
		ch <- res
	}
	DPrintf("timeout")
}

func (kv *KVServer) runOp(op Op) (string, Err) {
	kv.mu.Lock()
	_, okay := kv.footprint[op.Clerk]
	if !okay {
		kv.footprint[op.Clerk] = -1
	}
	kv.mu.Unlock()
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		return "", ErrWrongLeader
	}

	ch := kv.newChannel(index)
	go kv.timeout(index)
	res, ok := <-ch
	kv.deleteChannel(index)
	if !ok {
		DPrintf("reserr : %s", res.Err)
		return "", ErrWrongLeader
	}
	return res.Value, res.Err
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{GET, args.Key, "", args.Footprint, args.Clerk}
	value, err := kv.runOp(op)
	reply.Err = err
	reply.Value = value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{args.Op, args.Key, args.Value, args.Footprint, args.Clerk}
	_, err := kv.runOp(op)
	reply.Err = err
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

func (kv *KVServer) run(op Op, res *Res) {
	DPrintf("run")
	kv.mu.Lock()
	defer kv.mu.Unlock()

	fp, ok := kv.footprint[op.Clerk]
	if !ok || fp < op.Footprint {
		if op.Op == GET {
			// kv.footprint[op.Clerk] = op.Footprint
			val, okay := kv.database[op.Key]
			if !okay {
				res.Err = ErrNoKey
				return
			}
			res.Value = val
			// DPrintf("run get %s", val)
		} else if op.Op == PUT {
			kv.footprint[op.Clerk] = op.Footprint
			kv.database[op.Key] = op.Value
			// DPrintf("run put %s %s", op.Key, op.Value)
		} else if op.Op == APPEND {
			// DPrintf("run append %s %s", op.Key, op.Value)
			kv.footprint[op.Clerk] = op.Footprint
			if v, ok := kv.database[op.Key]; ok {
				kv.database[op.Key] = v + op.Value
				return
			}
			kv.database[op.Key] = op.Value
		}
	}
}

func (kv *KVServer) listen() {
	DPrintf("listening")
	for {
		msg := <-kv.applyCh
		// DPrintf("received op %s %s", msg.Command.(Op).Key, msg.Command.(Op).Value)

		res := Res{OK, ""}
		kv.run(msg.Command.(Op), &res)

		_, isLeader := kv.rf.GetState()

		ch, ok := kv.getChannel(msg.CommandIndex)
		kv.deleteChannel(msg.CommandIndex)
		if ok {
			if !isLeader {
				res = Res{ErrWrongLeader, ""}
			}
			ch <- res
		}
	}
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

	kv.database = make(map[string]string)
	kv.opChannels = make(map[int]chan Res)
	kv.footprint = make(map[int64]int64)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.listen()
	// You may need initialization code here.
	return kv
}
