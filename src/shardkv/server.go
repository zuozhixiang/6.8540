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
	SyncConfigType
	MoveShardType
	MoveDoneType
)

type ShardState int

const (
	NoSelf ShardState = iota
	Serving
	Pulling
	Pushing
)

type ShardData struct {
	Shard       int
	Data        map[string]string
	VersionData map[int64]string
	Executed    map[int64]bool
}

type Op struct {
	ID   int64
	Type OpType
	Data any
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
	dead int32

	ShardData [shardctrler.NShards]Shard

	lastAppliedIndex int
	persiter         *raft.Persister
	mck              *shardctrler.Clerk
	ShardConfig      shardctrler.Config
	HoldNormalShards map[int]bool // now hold on serving shards
	AllShardState    [shardctrler.NShards]ShardState
	done             map[int]*chan Result
}

func (kv *ShardKV) lock() {
	//if kv.isLeader() {
	//	_, file, line, _ := runtime.Caller(1)
	//	pos := fmt.Sprintf("%v:%v", file[Len:], line)
	//	debugf(Method("lock"), kv.me, kv.gid, "lock: pos: %v", pos)
	//}
	kv.mu.Lock()
}

func (kv *ShardKV) unlock() {
	//if kv.isLeader() {
	//	_, file, line, _ := runtime.Caller(1)
	//	pos := fmt.Sprintf("%v:%v", file[Len:], line)
	//	debugf(Method("unlock"), kv.me, kv.gid, "unlock: pos: %v", pos)
	//}
	kv.mu.Unlock()
}

func (kv *ShardKV) isLeader() bool {
	_, ret := kv.rf.GetState()
	return ret
}
func (kv *ShardKV) checkShardState(shardID int) string {
	kv.lock()
	defer kv.unlock()
	if kv.AllShardState[shardID] == Serving {
		return OK
	}
	if kv.AllShardState[shardID] == Pulling {
		return ErrShardNoReady
	}
	return ErrWrongGroup
}
func (kv *ShardKV) checkLeaderAndShard(shardID int) string {
	if !kv.isLeader() {
		return ErrWrongLeader
	}
	return kv.checkShardState(shardID)
}

func (kv *ShardKV) Get(req *GetArgs, resp *GetReply) {
	m := GetMethod
	shardID := key2shard(req.Key)
	resp.Err = kv.checkShardState(shardID)
	if resp.Err != OK {
		return
	}
	op := Op{
		ID:   req.ID,
		Data: req,
		Type: GetType,
	}
	res := kv.StartAndWaitRes(op)
	resp.Err = res.Err
	resp.Value = res.Value
	if resp.Err == OK {
		debugf(m, kv.me, kv.gid, "success req: %v, value: %v", toJson(req), resp.Value)
	} else {
		debugf(m, kv.me, kv.gid, "fail req: %v, resp: %v ", toJson(req), toJson(resp))
	}
}

func (kv *ShardKV) PutAppend(req *PutAppendArgs, resp *PutAppendReply) {
	m := PutMethod
	opType := PutType
	if req.Op == "Append" {
		m = AppendMethod
		opType = AppendType
	}
	shardID := key2shard(req.Key)
	resp.Err = kv.checkShardState(shardID)
	if resp.Err != OK {
		return
	}

	op := Op{
		ID:   req.ID,
		Data: req,
		Type: opType,
	}
	res := kv.StartAndWaitRes(op)
	resp.Err = res.Err
	if resp.Err == OK {
		debugf(m, kv.me, kv.gid, "success req: %v", toJson(req))
	} else {
		debugf(m, kv.me, kv.gid, "fail req: %v, resp: %v ", toJson(req), toJson(resp))
	}
}

func (kv *ShardKV) MoveShard(req *MoveShardArgs, resp *MoveShardReply) {
	m := GetShard
	if !kv.isLeader() {
		resp.Err = ErrWrongLeader
		debugf(m, kv.me, kv.gid, "not leader, id: %v", req.ID)
		return
	}
	kv.lock()
	newConfig := &req.ShardConfig
	if newConfig.Num < kv.ShardConfig.Num {
		resp.Err = OK
		debugf(m, kv.me, kv.gid, "err old verion, req: %v", req.ID)
		kv.unlock()
		return
	}
	if newConfig.Num > kv.ShardConfig.Num {
		resp.Err = ErrWaiting
		debugf(m, kv.me, kv.gid, "need wait req: %v", req.ID)
		kv.unlock()
		return
	}
	kv.unlock()
	resp.Err = OK
	op := Op{
		ID:   req.ID,
		Type: MoveShardType,
		Data: req,
	}
	res := kv.StartAndWaitRes(op)
	resp.Err = res.Err
	if resp.Err == OK {
		debugf(m, kv.me, kv.gid, "success req: %v", toJson(req))
	} else {
		debugf(m, kv.me, kv.gid, "fail req: %v, resp: %v ", toJson(req), toJson(resp))
	}
	return
}

func (kv *ShardKV) MoveDone(req *MoveDoneArgs, resp *MoveDoneReply) {
	m := MoveDoneM
	if !kv.isLeader() {
		debugf(m, kv.me, kv.gid, "no leader, id: %v", req.ID)
		resp.Err = ErrWrongLeader
		return
	}
	op := Op{
		ID:   req.ID,
		Data: req,
		Type: MoveDoneType,
	}
	res := kv.StartAndWaitRes(op)
	resp.Err = res.Err
	if resp.Err == OK {
		debugf(m, kv.me, kv.gid, "success req: %v", toJson(req))
	} else {
		debugf(m, kv.me, kv.gid, "fail req: %v, resp: %v ", toJson(req), toJson(resp))
	}
}

func (kv *ShardKV) CallDone(req *CallDoneArgs, resp *CallDoneReply) {
	op := Op{
		ID:   req.ID,
		Type: DeleteType,
		Data: req,
	}
	res := kv.StartAndWaitRes(op)
	resp.Err = res.Err
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	atomic.StoreInt32(&kv.dead, 1)
	// kv.applySnapshot(kv.dumpData())
	debugf(KILL, kv.me, kv.gid, "")
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
	labgob.Register(&PutAppendArgs{})
	labgob.Register(&GetArgs{})
	labgob.Register(&MoveDoneArgs{})
	labgob.Register(&MoveShardArgs{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(Shard{})
	labgob.Register(&CallDoneArgs{})

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

	kv.lastAppliedIndex = 0
	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.persiter = persister
	kv.HoldNormalShards = make(map[int]bool)
	kv.applySnapshot(persister.ReadSnapshot())
	kv.done = make(map[int]*chan Result, 100)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applyMsgForStateMachine()
	go kv.UpdateConfig()
	go kv.checkAndSendShard()
	// logger.Infof("start [G%v][S%v], state: %v", gid, kv.me, toJson(kv))
	return kv
}

func (kv *ShardKV) ClearShardState() {
	for i := 0; i < len(kv.AllShardState); i++ {
		kv.AllShardState[i] = NoSelf
	}
}
