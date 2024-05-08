package shardkv

import (
	"6.5840/labrpc"
	"6.5840/shardctrler"
	"sync/atomic"
)
import "6.5840/raft"
import "sync"
import "6.5840/labgob"

type OpType int

const (
	GetType OpType = iota
	AppendType
	PutType
	DeleteType
)

type Op struct {
	ID    int64
	Type  OpType
	Key   string
	Value string
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead             int32
	data             map[string]string
	executed         map[int64]bool
	versionData      map[int64]string
	lastAppliedIndex int
	cond             *sync.Cond
	persiter         *raft.Persister
	mck              *shardctrler.Clerk
	ShardConfig      shardctrler.Config
	shards           map[int]bool
}

func (kv *ShardKV) lock() {
	kv.mu.Lock()
}

func (kv *ShardKV) unlock() {
	kv.mu.Unlock()
}

func (kv *ShardKV) isLeader() bool {
	_, ret := kv.rf.GetState()
	return ret
}

func (kv *ShardKV) checkExecuted(id int64) bool {
	if _, ok := kv.executed[id]; ok {
		return true
	}
	return false
}

func (kv *ShardKV) Get(req *GetArgs, resp *GetReply) {
	if kv.killed() {
		return
	}

	m := GetMethod
	kv.lock()
	defer kv.unlock()
	shard := key2shard(req.Key)
	if _, ok := kv.shards[shard]; !ok {
		resp.Err = ErrWrongGroup
		debugf(m, kv.me, "id: %v, Wrong Group", req.ID)
		return
	}
	if !kv.isLeader() {
		debugf(m, kv.me, "not leader, req: %v", toJson(req))
		resp.Err = ErrWrongLeader
		return
	}
	resp.Err = OK
	if kv.checkExecuted(req.ID) {
		if _, ok := kv.versionData[req.ID]; !ok {
			panic(req.ID)
		}
		resp.Value = kv.versionData[req.ID]
		debugf(m, kv.me, "Executed, id: %v", req.ID)
		return
	}

	op := Op{
		ID:   req.ID,
		Key:  req.Key,
		Type: GetType,
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		debugf(m, kv.me, "not leader, req: %v", toJson(req))
		resp.Err = ErrWrongLeader
		return
	}
	debugf(m, kv.me, "req: %v, index: %v, term:%v", toJson(req), index, term)

	timeoutChan := make(chan bool, 1)
	go startTimeout(kv.cond, timeoutChan)
	timeout := false
	for !(index <= kv.lastAppliedIndex) && !timeout {
		select {
		case <-timeoutChan:
			timeout = true
		default:
			kv.cond.Wait() // wait, must hold mutex, after blocked, release lock
		}
	}
	if !kv.isLeader() {
		debugf(m, kv.me, "not leader, req: %v", toJson(req))
		resp.Err = ErrWrongLeader
		return
	}
	if timeout {
		resp.Err = ErrTimeout
		debugf(m, kv.me, "timeout!, req: %v", toJson(req))
		return
	}
	if _, ok := kv.versionData[op.ID]; !ok {
		panic("empty")
	}
	res := kv.versionData[op.ID]
	if res == "" {
		debugf(m, kv.me, "warning, req:%v", toJson(req))
	}
	debugf(m, kv.me, "success, req: %v, value: %v", toJson(req), res)
	resp.Value = res
}

func (kv *ShardKV) PutAppend(req *PutAppendArgs, resp *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		return
	}
	m := PutMethod
	if req.Op == "Append" {
		m = AppendMethod
	}

	kv.lock()
	defer kv.unlock()
	shard := key2shard(req.Key)
	if _, ok := kv.shards[shard]; !ok {
		resp.Err = ErrWrongGroup
		debugf(m, kv.me, "id: %v, Wrong Group", req.ID)
		return
	}
	if !kv.isLeader() {
		debugf(m, kv.me, "id: %v, not leader", req.ID)
		resp.Err = ErrWrongLeader
		return
	}

	resp.Err = OK
	if kv.checkExecuted(req.ID) {
		debugf(m, kv.me, "Executed, id: %v", req.ID)
		return
	}
	op := Op{
		ID:    req.ID,
		Type:  PutType,
		Key:   req.Key,
		Value: req.Value,
	}
	if m == AppendMethod {
		op.Type = AppendType
	}
	debugf(m, kv.me, "start, id: %v", req.ID)
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		debugf(m, kv.me, "not leader, req: %v", toJson(req))
		resp.Err = ErrWrongLeader
		return
	}
	debugf(m, kv.me, "req: %v, index: %v, term:%v", toJson(req), index, term)
	timeoutChan := make(chan bool, 1)
	go startTimeout(kv.cond, timeoutChan)
	timeout := false
	for !(index <= kv.lastAppliedIndex) && !timeout {
		select {
		case <-timeoutChan:
			timeout = true // timeout notify, raft can not do a agreement
		default:
			kv.cond.Wait() // wait, must hold mutex, after blocked, release lock
		}
	}
	if !kv.isLeader() {
		debugf(m, kv.me, "not leader, req: %v", toJson(req))
		resp.Err = ErrWrongLeader
		return
	}
	if timeout {
		resp.Err = ErrTimeout
		debugf(m, kv.me, "timeout!, req: %v", toJson(req))
		return
	}
	debugf(m, kv.me, "success, req:%v", toJson(req))
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	atomic.StoreInt32(&kv.dead, 1)
	debugf(KILL, kv.me, "")
}
func (kv *ShardKV) killed() bool {
	ret := atomic.LoadInt32(&kv.dead)
	return ret == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.dead = 0
	kv.data = map[string]string{}
	kv.executed = map[int64]bool{}
	kv.versionData = map[int64]string{}
	kv.lastAppliedIndex = 0
	kv.cond = sync.NewCond(&kv.mu)
	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.persiter = persister
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applyMsgForStateMachine()
	go kv.dectionMaxSize()
	go kv.UpdateConfig()
	return kv
}
