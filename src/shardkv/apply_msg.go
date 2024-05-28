package shardkv

import (
	"6.5840/raft"
	"fmt"
)

func (kv *ShardKV) executeOp(op Op, shard int) string {
	switch op.Type {
	case PutType:
		{
			shard := key2shard(op.Key)
			kv.Executed[shard][op.ID] = true
			kv.Data[shard][op.Key] = op.Value
			return op.Value
		}
	case AppendType:
		{
			shard := key2shard(op.Key)
			kv.Executed[shard][op.ID] = true
			kv.Data[shard][op.Key] = kv.Data[shard][op.Key] + op.Value
			return kv.Data[shard][op.Key]
		}
	case GetType:
		{
			shard := key2shard(op.Key)
			kv.Executed[shard][op.ID] = true
			kv.VersionData[shard][op.ID] = kv.Data[shard][op.Key]
			return kv.Data[shard][op.Key]
		}
	case SyncConfigType:
		{
			// need to do
			newConfig := &op.Config
			if newConfig.Num == kv.ShardConfig.Num {
				debugf(Method("SyncConfigType"), kv.me, kv.gid, "get old config: %v, self: %v", toJson(newConfig), toJson(kv.ShardConfig))
				return ""
			}
			// calaculate change config
			oldConfig := &kv.ShardConfig
			add, remove := getAddAndRemove(oldConfig.Shards, newConfig.Shards, kv.gid)
			for _, shard := range add {
				kv.AllShardState[shard] = Pulling
				delete(kv.HoldNormalShards, shard)
			}
			for _, shard := range remove {
				kv.AllShardState[shard] = Pushing
				delete(kv.HoldNormalShards, shard)
			}
		}
	case GetShardType:
		{

		}
	case DeleteType:
		{
			//delete(kv.Executed, op.Key)
			//delete(kv.VersionData, op.Key)
			// delete(kv.Executedlist, op.ID)
		}
	default:
		panic("illegal op type")
	}
	return ""
}

var opmap = map[OpType]string{
	GetType:        string(GetMethod),
	PutType:        string(PutMethod),
	AppendType:     string(AppendMethod),
	SyncConfigType: "SyncConfig",
	DeleteType:     "Delete",
	GetShardType:   "GetShard",
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
				shard := key2shard(op.Key)

				if op.Type == GetShardType {

				} else if op.Type != SyncConfigType && (!kv.checkShard(shard) || kv.checkExecuted(op.ID, shard)) {
					continue
				}
				res := kv.executeOp(op, shard)
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
