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
			kv.executed[shard][op.ID] = true
			kv.data[shard][op.Key] = op.Value
			return op.Value
		}
	case AppendType:
		{
			shard := key2shard(op.Key)
			kv.executed[shard][op.ID] = true
			kv.data[shard][op.Key] = kv.data[shard][op.Key] + op.Value
			return kv.data[shard][op.Key]
		}
	case GetType:
		{
			shard := key2shard(op.Key)
			kv.executed[shard][op.ID] = true
			kv.versionData[shard][op.ID] = kv.data[shard][op.Key]
			return kv.data[shard][op.Key]
		}
	case SyncConfigType:
		{
			oldConfig := kv.ShardConfig
			kv.ShardConfig = op.Config
			kv.shards = getSelfShards(op.Config.Shards, kv.gid) // all shards need
			add := op.NeedAddShards                             // need to add
			remove := op.NeedRemoveShards                       // need to remove

			// update hold shards
			for _, shard := range add {
				if oldConfig.Shards[shard] == 0 {
					kv.HoldShards[shard] = true
				}
			}
			for _, shard := range remove {
				kv.HoldShards[shard] = false
			}

			for _, shard := range add {
				//  need to wait new add shards come
				if !kv.HoldShards[shard] {
					kv.NoReadyShardSet[shard] = true
				}
			}
			// need to send shard to other group
			if kv.isLeader() {
				for _, shard := range remove {
					kv.SendShard(shard, op.Config.Shards[shard])
				}
			}

			// remove, shard data
			for _, shard := range remove {
				kv.data[shard] = map[string]string{}
				kv.versionData[shard] = map[int64]string{}
				kv.executed[shard] = map[int64]bool{}
			}

			debugf(Apply, kv.me, kv.gid, "sync Config: %v,hold: %v, noready: %v,state: %v, ", toJson(kv.ShardConfig), toJson(kv.HoldShards), toJson(kv.NoReadyShardSet), toJson(kv.data))
			return ""
		}
	case GetShardType:
		{
			kv.moveExecuted[op.ID] = true
			shardData := op.ShardData
			kv.data[shard] = shardData.Data
			kv.executed[shard] = shardData.Executed
			kv.versionData[shard] = shardData.VersionData
			delete(kv.NoReadyShardSet, shard)
			kv.HoldShards[shard] = true
			debugf(Apply, kv.me, kv.gid, "get shard: %v, id:%v, NoReady:%v, holdShard:%v, data: %v", shard, op.ID, toJson(kv.NoReadyShardSet), toJson(kv.HoldShards), toJson(kv.data))
			return ""
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
					if kv.moveExecuted[op.ID] {
						continue
					}
					shard = op.ShardData.Shard
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
