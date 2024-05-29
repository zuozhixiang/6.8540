package kvraft

import (
	"6.5840/raft"
	"fmt"
)

func (kv *KVServer) executeOp(op *Op) (res Result) {
	res.Err = OK
	switch op.Type {
	case PutType:
		{
			if !kv.executed[op.ID] {
				req := op.Data.(*PutAppendArgs)
				kv.executed[op.ID] = true
				kv.data[req.Key] = req.Value
			} else {
			}
		}
	case AppendType:
		{
			if !kv.executed[op.ID] {
				req := op.Data.(*PutAppendArgs)
				kv.executed[op.ID] = true
				kv.data[req.Key] = kv.data[req.Key] + req.Value
			} else {
			}
		}
	case GetType:
		{
			req := op.Data.(*GetArgs)
			kv.executed[op.ID] = true
			kv.versionData[op.ID] = kv.data[req.Key]
			res.Value = kv.data[req.Key]
		}
	case DeleteType:
		{
			//delete(kv.executed, op.Key)
			//delete(kv.versionData, op.Key)
		}
	default:
		panic("illegal type")
	}
	return
}

var opmap = map[OpType]Method{
	GetType:    (GetMethod),
	PutType:    (PutMethod),
	AppendType: (AppendMethod),
	DeleteType: "Delete",
}

func (kv *KVServer) applyMsgForStateMachine() {
	//msg := <-kv.applyCh
	//needApplyMsg := []raft.ApplyMsg{msg}
	//size := len(kv.applyCh)
	//for i := 0; i < size; i++ {
	//	needApplyMsg = append(needApplyMsg, <-kv.applyCh)
	//}

	for msg := range kv.applyCh {
		kv.lock()
		lastApplied := kv.lastAppliedIndex
		if msg.CommandValid {
			if msg.CommandIndex <= lastApplied {
				errmsg := fmt.Sprintf("[S%v], msg index: %v, lastApplied: %v, msg: %v", kv.me, msg.CommandIndex, lastApplied, msg)
				panic(errmsg)
			}
			lastApplied = msg.CommandIndex
			op := msg.Command.(Op)
			res := kv.executeOp(&op)
			debugf("Aly"+opmap[op.Type], kv.me, "idx: %v, id: \"%v\", res: %v", lastApplied, op.ID, toJson(res))
			kv.lastAppliedIndex = lastApplied
			if kv.isLeader() {
				ch, ok := kv.done[lastApplied]
				kv.unlock()
				if ok {
					*ch <- res
				}
			} else {
				kv.unlock()
			}

			kv.lock()
			size := kv.persiter.RaftStateSize()
			if kv.maxraftstate != -1 && size >= kv.maxraftstate {
				dumps := kv.dumpData()
				kv.rf.Snapshot(kv.lastAppliedIndex, dumps)
				debugf(MakeSnap, kv.me, "%v > %v, lastApplied: %v, newsize: %v", size, kv.maxraftstate, kv.lastAppliedIndex, kv.persiter.RaftStateSize())
			}
			kv.unlock()
		} else {
			if msg.SnapshotIndex <= lastApplied {
				errmsg := fmt.Sprintf("[S%v], snapshot index: %v, lastApplied: %v, msg: %v", kv.me, msg.SnapshotIndex, lastApplied, raft.GetPrintMsg([]raft.ApplyMsg{msg}))
				panic(errmsg)
			}
			lastApplied = msg.SnapshotIndex
			kv.applySnapshot(msg.Snapshot)
			kv.lastAppliedIndex = lastApplied
			debugf(AppSnap, kv.me, "snapIndex: %v, snapTerm: %v", msg.SnapshotIndex, msg.SnapshotTerm)
			kv.unlock()
		}

	}
	//if len(kv.applyCh) == 0 {
	//	time.Sleep(10 * time.Millisecond)
	//}

}
