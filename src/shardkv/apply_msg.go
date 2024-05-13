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
			debugf(Method("SyncConfig"), kv.me, kv.gid, "Op: %v state: %v", toJson(op), toJson(kv))
			oldConfig := kv.ShardConfig
			kv.ShardConfig = op.Config
			kv.Shards = getSelfShards(op.Config.Shards, kv.gid) // all shards need
			add := op.NeedAddShards                             // need to add
			remove := op.NeedRemoveShards                       // need to remove

			// update hold shards
			for _, shard := range add {
				if oldConfig.Shards[shard] == 0 {
					kv.HoldShards[shard] = true
				}
			}

			// need to send shard to other group
			needSend := []int{}
			for _, shard := range remove {
				if kv.HoldShards[shard] {
					needSend = append(needSend, shard)
					kv.HoldShards[shard] = false
				}
			}

			if kv.isLeader() && len(needSend) > 0 {
				for _, shard := range needSend {
					kv.SendShard(shard, op.Config.Shards[shard])
				}
			}

			for _, shard := range add {
				//  need to wait new add shards come
				if !kv.HoldShards[shard] {
					kv.NoReadyShardSet[shard] = true
				}
			}
			// remove, shard Data
			for _, shard := range remove {
				kv.Data[shard] = map[string]string{}
				kv.VersionData[shard] = map[int64]string{}
				kv.Executed[shard] = map[int64]bool{}
			}
			debugf(Method("SyncConfig"), kv.me, kv.gid, "sync Config success, %v,hold: %v, noready: %v,state: %v, ", toJson(kv.ShardConfig), toJson(kv.HoldShards), toJson(kv.NoReadyShardSet), toJson(kv.Data))
			return ""
		}
	case GetShardType:
		{
			debugf(GetShard, kv.me, kv.gid, "Op: %v", toJson(op))
			if kv.MoveExecuted[op.ID] {
				panic(op.ID)
			}
			if kv.ShardConfig.Num > op.Config.Num {
				//debugf(GetShard, kv.me, kv.gid, "id: %v, update config", op.ID)
				//kv.ShardConfig = op.Config
				panic("123")
			} else if kv.ShardConfig.Num < op.Config.Num {
				debugf(GetShard, kv.me, kv.gid, "id: %v, old config", op.ID)
				return ""
			}
			kv.MoveExecuted[op.ID] = true
			//if kv.HoldShards[shard] {
			//	errMsg := fmt.Sprintf("[G%v][S%v] ID:%v aleady hold shard: %v", kv.gid, kv.me, op.ID, shard)
			//	debugf(GetShard, kv.me, kv.gid, "ID:%v, state: %v", op.ID, toJson(kv))
			//	panic(errMsg)
			//}

			shardData := op.ShardData
			kv.Data[shard] = shardData.Data
			kv.Executed[shard] = shardData.Executed
			kv.VersionData[shard] = shardData.VersionData
			delete(kv.NoReadyShardSet, shard)
			kv.HoldShards[shard] = true
			debugf(GetShard, kv.me, kv.gid, "get shard: %v, id:%v, NoReady:%v, holdShard:%v, state: %v", shard, op.ID, toJson(kv.NoReadyShardSet), toJson(kv.HoldShards), toJson(kv.Data))
			return ""
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
					if kv.MoveExecuted[op.ID] {
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
