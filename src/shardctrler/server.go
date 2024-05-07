package shardctrler

import (
	"6.5840/raft"
	"fmt"
	"sync/atomic"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	dead    int32
	applyCh chan raft.ApplyMsg

	// Your data here.
	executed         map[int64]bool
	versionData      map[int64]Config
	configs          []Config // indexed by config num
	lastAppliedIndex int
	maxraftstate     int

	persiter *raft.Persister
	cond     *sync.Cond
}
type OpType int

const (
	JoinOp OpType = iota
	LeaveOp
	MoveOp
	QueryOp
)

type Op struct {
	// Your data here.
	ID   int64
	Type OpType
	Args interface{} // different type, different args
}

func (sc *ShardCtrler) isLeader() bool {
	_, ret := sc.rf.GetState()
	return ret
}
func (sc *ShardCtrler) lock() {
	sc.mu.Lock()
}
func (sc *ShardCtrler) unlock() {
	sc.mu.Unlock()
}

func (sc *ShardCtrler) checkExecuted(ID int64) bool {
	_, ok := sc.executed[ID]
	return ok
}

func (sc *ShardCtrler) Join(req *JoinArgs, resp *JoinReply) {
	// Your code here.
	if sc.killed() {
		return
	}
	m := JoinMethod
	sc.lock()
	defer sc.unlock()
	if !sc.isLeader() {
		resp.Err = ErrWrongLeader
		debugf(m, sc.me, "not leader")
		return
	}
	resp.Err = OK
	if sc.checkExecuted(req.ID) {
		debugf(m, sc.me, "id: %v, executed", req.ID)
		return
	}
	debugf(m, sc.me, "req: %v", toJson(req))
	op := Op{
		ID:   req.ID,
		Type: JoinOp,
		Args: req,
	}
	index, term, isleader := sc.rf.Start(op)
	if !isleader {
		resp.Err = ErrWrongLeader
		debugf(m, sc.me, "not leader")
		return
	}
	fmt.Println(term)
	timeoutChan := make(chan bool, 1)
	go startTimeout(sc.cond, timeoutChan)
	timeout := false
	for !(index <= sc.lastAppliedIndex) && !timeout {
		select {
		case <-timeoutChan:
			timeout = true // timeout notify, raft can not do a agreement
		default:
			sc.cond.Wait() // wait, must hold mutex, after blocked, release lock
		}
	}
	if !sc.isLeader() {
		resp.Err = ErrWrongLeader
		debugf(m, sc.me, "not leader")
		return
	}
	if timeout {
		resp.Err = ErrTimeout
		debugf(m, sc.me, "timeout, id: %v", req.ID)
		return
	}
	debugf(m, sc.me, "success, req: %v", toJson(req))
}

func (sc *ShardCtrler) Leave(req *LeaveArgs, resp *LeaveReply) {
	// Your code here.
	if sc.killed() {
		return
	}
	m := LeaveMethod
	sc.lock()
	defer sc.unlock()
	if !sc.isLeader() {
		resp.Err = ErrWrongLeader
		debugf(m, sc.me, "not leader")
		return
	}
	resp.Err = OK
	if sc.checkExecuted(req.ID) {
		debugf(m, sc.me, "id: %v, executed", req.ID)
		return
	}
	debugf(m, sc.me, "req: %v", toJson(req))
	op := Op{
		ID:   req.ID,
		Type: LeaveOp,
		Args: req,
	}
	index, term, isleader := sc.rf.Start(op)
	if !isleader {
		resp.Err = ErrWrongLeader
		debugf(m, sc.me, "not leader")
		return
	}
	fmt.Println(term)
	timeoutChan := make(chan bool, 1)
	go startTimeout(sc.cond, timeoutChan)
	timeout := false
	for !(index <= sc.lastAppliedIndex) && !timeout {
		select {
		case <-timeoutChan:
			timeout = true // timeout notify, raft can not do a agreement
		default:
			sc.cond.Wait() // wait, must hold mutex, after blocked, release lock
		}
	}
	if !sc.isLeader() {
		resp.Err = ErrWrongLeader
		debugf(m, sc.me, "not leader")
		return
	}
	if timeout {
		resp.Err = ErrTimeout
		debugf(m, sc.me, "timeout, id: %v", req.ID)
		return
	}
	debugf(m, sc.me, "success, req: %v", toJson(req))
}

func (sc *ShardCtrler) Move(req *MoveArgs, resp *MoveReply) {
	// Your code here.
	if sc.killed() {
		return
	}
	m := MoveMethod
	sc.lock()
	defer sc.unlock()
	if !sc.isLeader() {
		resp.Err = ErrWrongLeader
		debugf(m, sc.me, "not leader")
		return
	}
	resp.Err = OK
	if sc.checkExecuted(req.ID) {
		debugf(m, sc.me, "id: %v, executed", req.ID)
		return
	}
	debugf(m, sc.me, "req: %v", toJson(req))

	op := Op{
		ID:   req.ID,
		Type: MoveOp,
		Args: req,
	}
	index, term, isleader := sc.rf.Start(op)
	if !isleader {
		resp.Err = ErrWrongLeader
		debugf(m, sc.me, "not leader")
		return
	}
	fmt.Println(term)
	timeoutChan := make(chan bool, 1)
	go startTimeout(sc.cond, timeoutChan)
	timeout := false
	for !(index <= sc.lastAppliedIndex) && !timeout {
		select {
		case <-timeoutChan:
			timeout = true // timeout notify, raft can not do a agreement
		default:
			sc.cond.Wait() // wait, must hold mutex, after blocked, release lock
		}
	}
	if !sc.isLeader() {
		resp.Err = ErrWrongLeader
		debugf(m, sc.me, "not leader")
		return
	}
	if timeout {
		resp.Err = ErrTimeout
		debugf(m, sc.me, "timeout, id: %v", req.ID)
		return
	}
	debugf(m, sc.me, "success, req: %v", toJson(req))

}

func (sc *ShardCtrler) Query(req *QueryArgs, resp *QueryReply) {
	// Your code here.
	if sc.killed() {
		return
	}
	m := QueryMethod
	sc.lock()
	defer sc.unlock()
	if !sc.isLeader() {
		resp.Err = ErrWrongLeader
		debugf(m, sc.me, "not leader")
		return
	}
	resp.Err = OK
	if sc.checkExecuted(req.ID) {
		debugf(m, sc.me, "id: %v, executed", req.ID)
		return
	}
	debugf(m, sc.me, "req: %v", toJson(req))
	op := Op{
		ID:   req.ID,
		Type: QueryOp,
		Args: req,
	}
	index, term, isleader := sc.rf.Start(op)
	if !isleader {
		resp.Err = ErrWrongLeader
		debugf(m, sc.me, "not leader")
		return
	}
	fmt.Println(term)
	timeoutChan := make(chan bool, 1)
	go startTimeout(sc.cond, timeoutChan)
	timeout := false
	for !(index <= sc.lastAppliedIndex) && !timeout {
		select {
		case <-timeoutChan:
			timeout = true // timeout notify, raft can not do a agreement
		default:
			sc.cond.Wait() // wait, must hold mutex, after blocked, release lock
		}
	}
	if !sc.isLeader() {
		resp.Err = ErrWrongLeader
		debugf(m, sc.me, "not leader")
		return
	}
	if timeout {
		resp.Err = ErrTimeout
		debugf(m, sc.me, "timeout, id: %v", req.ID)
		return
	}
	debugf(m, sc.me, "success, req: %v", toJson(req))
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	atomic.StoreInt32(&sc.dead, 1)
	debugf(KillMethod, sc.me, "be killed")
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	dead := atomic.LoadInt32(&sc.dead)
	return dead == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.dead = 0

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	sc.executed = map[int64]bool{}
	sc.cond = sync.NewCond(&sc.mu)
	sc.versionData = map[int64]Config{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.persiter = persister
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	return sc
}
