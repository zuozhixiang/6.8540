package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"sync"
	"sync/atomic"
	"time"
)

type OpType int

const (
	GetType OpType = iota
	AppendType
	PutType
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type  OpType
	Key   string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data     map[string]string
	executed map[string]bool
}

func (kv *KVServer) lock() {
	kv.mu.Lock()
}

func (kv *KVServer) unlock() {
	kv.mu.Unlock()
}

func (kv *KVServer) isLeader() bool {
	_, ret := kv.rf.GetState()
	return ret
}

func (kv *KVServer) checkExecuted(id string) bool {
	if _, ok := kv.executed[id]; ok {
		return true
	}
	kv.executed[id] = true
	return false
}

func (kv *KVServer) Get(req *GetArgs, resp *GetReply) {
	if kv.killed() {
		return
	}
	// Your code here.
	kv.lock()
	defer kv.unlock()

	m := GetMethod
	if !kv.isLeader() {
		debugf(m, kv.me, "not leader, req: %v", toJson(req))
		resp.Err = ErrWrongLeader
		return
	}

	resp.Err = OK
	op := Op{
		Type: GetType,
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		debugf(m, kv.me, "not leader, req: %v", toJson(req))
		resp.Err = ErrWrongLeader
		return
	}
	debugf(m, kv.me, "req: %v", toJson(req))
	select {
	case applyMsg := <-kv.applyCh:
		if applyMsg.CommandValid {
			if index == applyMsg.CommandIndex {
				debugf(m, kv.me, "success, id:%v index: %v, term: %v", req.ID, index, term)
			} else {
				// panic
				logger.Panicf("1")
			}
		}
	case <-time.After(1 * time.Second):
		logger.Errorf("timeout")
		resp.Err = ErrTimeout
		return
	}

	if value, ok := kv.data[req.Key]; ok {
		resp.Value = value
		return
	}

	resp.Value = ""
}

func (kv *KVServer) Put(req *PutAppendArgs, resp *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		return
	}
	kv.lock()
	defer kv.unlock()
	m := PutMethod
	if !kv.isLeader() {
		debugf(m, kv.me, "not leader, req: %v", toJson(req))
		resp.Err = ErrWrongLeader
		return
	}

	resp.Err = OK
	if kv.checkExecuted(req.ID) {
		debugf(m, kv.me, "Executed, id: %v", toJson(req))
		return
	}

	op := Op{
		Type:  PutType,
		Key:   req.Key,
		Value: req.Value,
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		debugf(m, kv.me, "not leader, req: %v", toJson(req))
		resp.Err = ErrWrongLeader
		return
	}
	debugf(PutMethod, kv.me, "%v", term)
	select {
	case applyMsg := <-kv.applyCh:
		if applyMsg.CommandValid {
			if index == applyMsg.CommandIndex {
				debugf(m, kv.me, "success, id:%v index: %v, term: %v", req.ID, index, term)
			} else {
				// panic
				logger.Panicf("1")
			}
		}
	case <-time.After(1 * time.Second):
		logger.Errorf("timeout")
		resp.Err = ErrTimeout
		return
	}
	kv.data[req.Key] = req.Value
}

func (kv *KVServer) Append(req *PutAppendArgs, resp *PutAppendReply) {
	if kv.killed() {
		return
	}
	kv.lock()
	defer kv.unlock()
	meth := AppendMethod
	if !kv.isLeader() {
		debugf(meth, kv.me, "not leader, req: %v", toJson(req))
		resp.Err = ErrWrongLeader
		return
	}
	resp.Err = OK
	if kv.checkExecuted(req.ID) {
		debugf(meth, kv.me, "Executed, id: %v", req.ID)
		return
	}

	debugf(meth, kv.me, "arrive req: %v", toJson(req))
	op := Op{
		Type:  AppendType,
		Key:   req.Key,
		Value: req.Value,
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		debugf(meth, kv.me, "not leader, req: %v", toJson(req))
		resp.Err = ErrWrongLeader
		return
	}
	debugf(meth, kv.me, "submit to raft, id: %v, index: %v, term: %v", req.ID, index, term)
	select {
	case applyMsg := <-kv.applyCh:
		if applyMsg.CommandValid {
			if index == applyMsg.CommandIndex {
				debugf(meth, kv.me, "success, id:%v index: %v, term: %v", req.ID, index, term)
			} else {
				// panic
				logger.Panicf("1")
			}
		}
	case <-time.After(1 * time.Second):
		logger.Errorf("%v [S%v] timeout id: %v", meth, kv.me, req.ID)
		resp.Err = ErrTimeout
		return
	}

	if _, ok := kv.data[req.Key]; !ok {
		//  logger.Errorf("append illegal req: %v", toJson(req))
		kv.data[req.Key] = req.Value
		return
	}
	oldv := kv.data[req.Key]
	newv := oldv + req.Value
	kv.data[req.Key] = newv
	return
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
	debugf(KILL, kv.me, "be killed")
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
	kv.data = map[string]string{}
	kv.executed = map[string]bool{}
	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	return kv
}
