package shardkv

import (
	"6.5840/raft"
	"fmt"
)

func (kv *ShardKV) executeOp(op Op) string {

	switch op.Type {
	case PutType:
		{
			kv.executed[op.ID] = true
			shard := key2shard(op.Key)
			kv.executedList[shard] = append(kv.executedList[shard], op.ID)
			kv.data[shard][op.Key] = op.Value
			return op.Value
		}
	case AppendType:
		{
			kv.executed[op.ID] = true
			shard := key2shard(op.Key)
			kv.executedList[shard] = append(kv.executedList[shard], op.ID)
			kv.data[shard][op.Key] = kv.data[shard][op.Key] + op.Value
			return kv.data[shard][op.Key]
		}
	case GetType:
		{
			kv.executed[op.ID] = true
			shard := key2shard(op.Key)
			kv.versionData[shard][op.ID] = kv.data[shard][op.Key]
			return kv.data[shard][op.Key]
		}
	case DeleteType:
		{
			//delete(kv.executed, op.Key)
			//delete(kv.versionData, op.Key)
			// delete(kv.executedlist, op.ID)
		}
	default:
		panic("illegal op type")
	}
	return ""
}

var opmap = map[OpType]string{
	GetType:    string(GetMethod),
	PutType:    string(PutMethod),
	AppendType: string(AppendMethod),
	DeleteType: "Delete",
}

func formatCmd(op Op) string {
	return fmt.Sprintf("[%v][%v][Key: %v][Value: %v]", op.ID, opmap[op.Type], op.Key, op.Value)
}

func (kv *ShardKV) applyMsgForStateMachine() {

	for {
		if kv.killed() {
			return
		}
		msg := <-kv.applyCh
		needApplyMsg := []raft.ApplyMsg{msg}
		size := len(kv.applyCh)
		for i := 0; i < size; i++ {
			needApplyMsg = append(needApplyMsg, <-kv.applyCh)
		}
		kv.lock()
		lastApplied := kv.lastAppliedIndex
		for _, msg := range needApplyMsg {
			if msg.CommandValid {
				if msg.CommandIndex <= lastApplied {
					errmsg := fmt.Sprintf("[S%v][G%v], msg index: %v, lastApplied: %v, msg: %v", kv.me, kv.gid, msg.CommandIndex, lastApplied, msg)
					panic(errmsg)
				}
				lastApplied = msg.CommandIndex
				op := msg.Command.(Op)
				if kv.checkExecuted(op.ID) {
					continue
				}
				res := kv.executeOp(op)
				op.Value = res
				debugf(Apply, kv.me, kv.gid, "idx: %v, %v", msg.CommandIndex, formatCmd(op))
			} else {
				if msg.SnapshotIndex <= lastApplied {
					errmsg := fmt.Sprintf("[S%v][G%v], snapshot index: %v, lastApplied: %v, msg: %v", kv.me, kv.gid, msg.SnapshotIndex, lastApplied, raft.GetPrintMsg([]raft.ApplyMsg{msg}))
					panic(errmsg)
				}
				lastApplied = msg.SnapshotIndex
				kv.applySnapshot(msg.Snapshot)
				debugf(AppSnap, kv.me, kv.gid, "snapIndex: %v, snapTerm: %v", msg.SnapshotIndex, msg.SnapshotTerm)
			}
		}
		kv.lastAppliedIndex = lastApplied
		kv.cond.Broadcast()
		kv.unlock()
	}
}
