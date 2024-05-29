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
	DeleteType
)

type Op struct {
	ID   int64
	Type OpType
	Data any
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	data             map[string]string // state machine data
	executed         map[int64]bool    // executed requestId
	versionData      map[int64]string  // requestID to data
	lastAppliedIndex int
	persiter         *raft.Persister // persist util
	done             map[int]*chan Result
}

func (kv *KVServer) lock() {
	//_, file, line, _ := runtime.Caller(1)
	//pos := fmt.Sprintf("%v:%v", file[Len:], line)

	//debugf(Method("lock"), kv.me, "lock: pos: %v", pos)
	kv.mu.Lock()
}

func (kv *KVServer) unlock() {
	//_, file, line, _ := runtime.Caller(1)
	//pos := fmt.Sprintf("%v:%v", file[Len:], line)
	//debugf(Method("unlock"), kv.me, "lock: pos: %v", pos)
	kv.mu.Unlock()
}

func (kv *KVServer) isLeader() bool {
	_, ret := kv.rf.GetState()
	return ret
}

func (kv *KVServer) checkExecuted(id int64) bool {
	if _, ok := kv.executed[id]; ok {
		return true
	}
	return false
}

func (kv *KVServer) StartAndWaitRes(op Op) (res Result) {
	ch := make(chan Result, 1)
	kv.lock()
	index, _, _ := kv.rf.Start(op)
	kv.done[index] = &ch
	kv.unlock()
	res.Err = OK
	select {
	case r := <-ch:
		{
			res = r
		}
	case <-time.After(700 * time.Millisecond):
		res.Err = ErrTimeout
	}
	kv.lock()
	delete(kv.done, index)
	kv.unlock()
	return
}

func (kv *KVServer) Get(req *GetArgs, resp *GetReply) {
	m := GetMethod
	if !kv.isLeader() {
		debugf(m, kv.me, "not leader, req: %v", toJson(req))
		resp.Err = ErrWrongLeader
		return
	}
	resp.Err = OK

	op := Op{
		ID:   req.ID,
		Data: req,
		Type: GetType,
	}
	res := kv.StartAndWaitRes(op)
	resp.Err = res.Err
	resp.Value = res.Value
	if resp.Err == OK {
		debugf(m, kv.me, "success req: %v, value: %v", toJson(req), resp.Value)
	} else {
		debugf(m, kv.me, "fail req: %v, resp: %v ", toJson(req), toJson(resp))
	}
}

func (kv *KVServer) Put(req *PutAppendArgs, resp *PutAppendReply) {
	m := PutMethod
	if !kv.isLeader() {
		debugf(m, kv.me, "not leader, req: %v", toJson(req))
		resp.Err = ErrWrongLeader
		return
	}
	resp.Err = OK
	kv.lock()
	if kv.executed[req.ID] {
		kv.unlock()
		return
	}
	kv.unlock()
	op := Op{
		ID:   req.ID,
		Data: req,
		Type: PutType,
	}
	res := kv.StartAndWaitRes(op)
	resp.Err = res.Err
	if resp.Err == OK {
		debugf(m, kv.me, "success req: %v", toJson(req))
	} else {
		debugf(m, kv.me, "fail req: %v, resp: %v ", toJson(req), toJson(resp))
	}
}

func (kv *KVServer) Append(req *PutAppendArgs, resp *PutAppendReply) {
	m := AppendMethod
	if !kv.isLeader() {
		debugf(m, kv.me, "not leader, req: %v", toJson(req))
		resp.Err = ErrWrongLeader
		return
	}
	resp.Err = OK
	kv.lock()
	if kv.executed[req.ID] {
		kv.unlock()
		return
	}
	kv.unlock()
	op := Op{
		ID:   req.ID,
		Data: req,
		Type: AppendType,
	}
	res := kv.StartAndWaitRes(op)
	resp.Err = res.Err
	if resp.Err == OK {
		debugf(m, kv.me, "success req: %v", toJson(req))
	} else {
		debugf(m, kv.me, "fail req: %v, resp: %v ", toJson(req), toJson(resp))
	}
}

func (kv *KVServer) Notify(req *NotifyFinishedRequest, resp *NotifyFinishedResponse) {
	if !kv.isLeader() {
		resp.Err = ErrWrongLeader
		return
	}
	op := Op{
		ID:   req.ID,
		Data: req,
		Type: DeleteType,
	}
	res := kv.StartAndWaitRes(op)
	resp.Err = res.Err
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
	labgob.Register(&PutAppendArgs{})
	labgob.Register(&GetArgs{})
	labgob.Register(&NotifyFinishedRequest{})
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.data = map[string]string{}
	kv.versionData = map[int64]string{}
	kv.executed = map[int64]bool{}
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.persiter = persister
	kv.lastAppliedIndex = 0
	kv.done = make(map[int]*chan Result, 100)
	kv.applySnapshot(persister.ReadSnapshot())
	debugf(Start, kv.me, "data: %v, executed: %v, versionData:%v", toJson(kv.data), toJson(kv.executed), toJson(kv.versionData))
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applyMsgForStateMachine()
	return kv
}
