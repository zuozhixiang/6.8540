package shardkv

import (
	"6.5840/raft"
	"6.5840/shardctrler"
	"fmt"
	"time"
)

// submit request to raft and wait raft return
func (kv *ShardKV) StartAndWaitRes(op Op) (res Result) {
	ch := make(chan Result, 1)
	index, _, isleader := kv.rf.Start(op)
	if !isleader {
		res.Err = ErrWrongLeader
		return
	}
	kv.lock()
	kv.done[index] = &ch
	kv.unlock()
	res.Err = OK
	select {
	case r := <-ch:
		{
			res = r
		}
	case <-time.After(600 * time.Millisecond):
		{
			res.Err = ErrTimeout
		}
	}
	kv.lock()
	delete(kv.done, index)
	kv.unlock()
	return
}

// execute command
func (kv *ShardKV) executeOp(op *Op) (res Result) {
	res.Err = OK
	switch op.Type {
	case PutType:
		{
			req := op.Data.(*PutAppendArgs)
			shardID := key2shard(req.Key)
			if kv.AllShardState[shardID] == Serving {
				shard := kv.ShardData[shardID]
				if _, ok := shard.Executed[op.ID]; !ok {
					shard.Executed[op.ID] = struct{}{}
					shard.Data[req.Key] = req.Value
				}
				return
			}
			if kv.AllShardState[shardID] == Pulling {
				res.Err = ErrShardNoReady
				return
			}
			res.Err = ErrWrongGroup
		}
	case AppendType:
		{
			req := op.Data.(*PutAppendArgs)
			shardID := key2shard(req.Key)
			shardState := kv.AllShardState[shardID]
			if shardState == Serving {
				shard := kv.ShardData[shardID]
				if _, ok := shard.Executed[op.ID]; !ok {
					shard.Executed[op.ID] = struct{}{}
					shard.Data[req.Key] += req.Value
				}
				return
			}
			if kv.AllShardState[shardState] == Pulling {
				res.Err = ErrShardNoReady
				return
			}
			res.Err = ErrWrongGroup
		}
	case GetType:
		{
			req := op.Data.(*GetArgs)
			shardID := key2shard(req.Key)
			shardState := kv.AllShardState[shardID]
			if shardState == Serving {
				shard := kv.ShardData[shardID]
				if _, ok := shard.Executed[req.ID]; ok {
					res.Value = shard.VersionData[req.ID]
				} else {
					shard.Executed[req.ID] = struct{}{}
					shard.VersionData[req.ID] = shard.Data[req.Key]
					res.Value = shard.Data[req.Key]
				}
				return
			}
			if shardState == Pulling {
				res.Err = ErrShardNoReady
				return
			}
			res.Err = ErrWrongGroup
		}
	case SyncConfigType:
		{
			newConfig := op.Data.(shardctrler.Config)
			if newConfig.Num < kv.ShardConfig.Num {
				debugf(Config, kv.me, kv.gid, "warn old version, old: %v, self: %v", newConfig.Num, kv.ShardConfig.Num)
				return
			}
			if newConfig.Num == kv.ShardConfig.Num {
				return
			}
			oldConfig := kv.ShardConfig
			add, remove := getAddAndRemove(oldConfig.Shards, newConfig.Shards, kv.gid)
			kv.ShardConfig = newConfig
			if newConfig.Num == 1 {
				if len(remove) > 0 {
					logger.Panicf("[S%v][G%v] old: %v, new: %v", toJson(kv.ShardConfig), toJson(newConfig))
					return
				}
				for _, shardID := range add {
					kv.AllShardState[shardID] = Serving
					kv.ShardData[shardID] = NewShard()
					kv.HoldNormalShards[shardID] = true
				}
				debugf(Config, kv.me, kv.gid, "old Config: %v, new Config: %v, add: %v, remove: %v", toJson(oldConfig), toJson(newConfig), toJson(add), toJson(remove))
				return
			}
			for _, shardID := range add {
				kv.AllShardState[shardID] = Pulling
			}
			for _, shardID := range remove {
				kv.AllShardState[shardID] = Pushing
				delete(kv.HoldNormalShards, shardID)
			}
			debugf(Config, kv.me, kv.gid, "old Config: %v, new Config: %v, add: %v, remove: %v", toJson(oldConfig), toJson(newConfig), toJson(add), toJson(remove))
		}
	case MoveShardType:
		{
			req := op.Data.(*MoveShardArgs)
			if req.ShardConfig.Num < kv.ShardConfig.Num {
				debugf(GetShard, kv.me, kv.gid, "warn old version, old: %v, self: %v", req.ShardConfig.Num, kv.ShardConfig.Num)
				res.Err = OK
				return
			}
			if req.ShardConfig.Num > kv.ShardConfig.Num {
				debugf(GetShard, kv.me, kv.gid, "warn new version, old: %v, self: %v", req.ShardConfig.Num, kv.ShardConfig.Num)
				res.Err = ErrWaiting
				return
			}
			if kv.AllShardState[req.ShardID] == Serving {
				debugf(GetShard, kv.me, kv.gid, "warn already get, shard: %v, id: %v", req.ShardID, req.ID)
				return
			}
			kv.ShardData[req.ShardID] = req.Data
			kv.HoldNormalShards[req.ShardID] = true
			kv.AllShardState[req.ShardID] = Serving
			// go kv.sendMoveDone(req.ID, kv.ShardConfig.Groups[req.FromGID], kv.me, req.ShardID)
		}
	case MoveDoneType:
		{
			req := op.Data.(*MoveDoneArgs)
			shardID := req.ShardID
			state := kv.AllShardState[shardID]
			if state == Serving || state == Pulling {
				logger.Panicf("illegal! id: %v", req.ID)
				return
			}
			if state == NoSelf {
				debugf(MoveDoneM, kv.me, kv.gid, "warn already noself, id: %v", req.ID)
				return
			}
			kv.AllShardState[shardID] = NoSelf
			kv.ShardData[shardID] = Shard{}
		}
	case DeleteType:
		{
			req := op.Data.(*CallDoneArgs)
			sharsState := kv.AllShardState[req.ShardID]
			if sharsState == Serving {
				delete(kv.ShardData[req.ShardID].Executed, req.ID)
				delete(kv.ShardData[req.ShardID].VersionData, req.ID)
				return
			}
			if sharsState == Pulling {
				res.Err = ErrShardNoReady
				return
			}
			debugf(Method("test"), kv.me, kv.gid, "state: %v", toJson(kv.AllShardState))
			res.Err = ErrWrongGroup
		}
	default:
		panic("illegal type")
	}
	return
}

var opmap = map[OpType]Method{
	GetType:        (GetMethod),
	PutType:        (PutMethod),
	AppendType:     (AppendMethod),
	DeleteType:     "Delete",
	SyncConfigType: Config,
	MoveShardType:  GetShard,
	MoveDoneType:   MoveDoneM,
}

func (kv *ShardKV) applyMsgForStateMachine() {
	for msg := range kv.applyCh {
		if kv.killed() {
			return
		}
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
			debugf("Aly"+opmap[op.Type], kv.me, kv.gid, "idx: %v, id: \"%v\", res: %v", lastApplied, op.ID, toJson(res))
			kv.lastAppliedIndex = lastApplied
			if kv.isLeader() {
				ch, ok := kv.done[lastApplied]
				if ok {
					*ch <- res
				}
			}
			size := kv.persiter.RaftStateSize()
			if kv.maxraftstate != -1 && size >= kv.maxraftstate {
				dumps := kv.dumpData()
				kv.rf.Snapshot(kv.lastAppliedIndex, dumps)
				debugf(MakeSnap, kv.me, kv.gid, "%v > %v, lastApplied: %v, newsize: %v", size, kv.maxraftstate, kv.lastAppliedIndex, kv.persiter.RaftStateSize())
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
			debugf(AppSnap, kv.me, kv.gid, "snapIndex: %v, snapTerm: %v", msg.SnapshotIndex, msg.SnapshotTerm)
			kv.unlock()
		}
	}
}
