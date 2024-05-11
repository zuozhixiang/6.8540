package shardkv

import (
	"6.5840/shardctrler"
	"time"
)

func (kv *ShardKV) GetLastConfig() *shardctrler.Config {
	if !kv.isLeader() {
		return nil
	}
	lastConfig := kv.mck.Query(-1)
	kv.lock()
	defer kv.unlock()
	if lastConfig.Num <= kv.ShardConfig.Num {
		return nil
	}
	return &lastConfig

}

func getSelfShards(shards [shardctrler.NShards]int, gid int) map[int]bool {
	res := map[int]bool{}
	for shard, gi := range shards {
		if gi == gid {
			res[shard] = true
		}
	}
	return res
}

func getAddAndRemove(oldShards [shardctrler.NShards]int, newShards [shardctrler.NShards]int, gid int) ([]int, []int) {
	add := []int{}
	remove := []int{}
	for i := 0; i < shardctrler.NShards; i++ {
		if oldShards[i] != newShards[i] {
			if newShards[i] == gid {
				// new add
				add = append(add, i)
			} else if oldShards[i] == gid {
				// remove others
				remove = append(remove, i)
			}
		}
	}
	return add, remove
}

func (kv *ShardKV) sendShard(me int, fromgid int, togid int, req *MoveShardArgs, groups []string) {
	resp := &MoveShardReply{}
	defer debugf(SendShard, me, fromgid, "->[g%v] success", togid)
	for kv.isLeader() && !kv.killed() {
		for _, srvname := range groups {
			srv := kv.make_end(srvname)
			debugf(SendShard, me, fromgid, "->[g%v]%v, req: %v", togid, srvname, toJson(req))
			ok := srv.Call("ShardKV.MoveShard", req, resp)
			if ok && resp.Err == OK {
				debugf(SendShard, me, fromgid, "success ->[g%v]%v, id:%v shard: %v", togid, srvname, req.ID, req.ShardData)
				return
			} else if ok && resp.Err == ErrWrongGroup {
				debugf(SendShard, me, fromgid, "fatal fail wrong group  ->[g%v]%v, id:%v, shard: %v", togid, srvname, req.ID, req.Shard)
				return
			} else if ok && resp.Err == ErrWrongLeader {
				debugf(SendShard, me, fromgid, "fail wrong leader  ->[g%v]%v, id:%v, shard: %v", togid, srvname, req.ID, req.Shard)
			} else if ok && resp.Err == ErrOldVersion {
				debugf(SendShard, me, fromgid, "fatal fail old config  ->[g%v]%v, id:%v, shard: %v", togid, srvname, req.ID, req.Shard)
				return
			}
		}
	}
}

func (kv *ShardKV) SendShard(shard int, gid int) {
	// get shard data,
	data := Copy(kv.data[shard]).(map[string]string)
	versionData := Copy(kv.versionData[shard]).(map[int64]string)
	executed := Copy(kv.executed[shard]).(map[int64]bool)
	shardConfig := Copy(kv.ShardConfig).(shardctrler.Config)
	req := &MoveShardArgs{
		ID:          nrand(),
		Shard:       shard,
		ShardData:   data,
		VersionData: versionData,
		Executed:    executed,
		ShardConfig: shardConfig,
	}
	go kv.sendShard(kv.me, kv.gid, gid, req, kv.ShardConfig.Groups[gid])
}

func (kv *ShardKV) updateConfigHelper(lastConfig shardctrler.Config) {
	if lastConfig.Num > kv.ShardConfig.Num {
		oldConf := kv.ShardConfig
		add, remove := getAddAndRemove(oldConf.Shards, lastConfig.Shards, kv.gid)
		// need to sync other servers in the group
		op := Op{
			Type:             SyncConfigType,
			NeedRemoveShards: remove,
			NeedAddShards:    add,
			Config:           lastConfig,
		}
		my := getSelfShards(lastConfig.Shards, kv.gid)
		index, _, _ := kv.rf.Start(op)
		debugf(Config, kv.me, kv.gid, "index: %v,my:%v, allocated: %v, newConfig: %v,", index, my, toJson(shardctrler.GetGIDShards(lastConfig.Shards)), toJson(lastConfig))
	}
}

func (kv *ShardKV) UpdateConfig() {
	for !kv.killed() {
		if kv.isLeader() {
			lastConfig := kv.mck.Query(-1)
			debugf(Method("GetConfig"), kv.me, kv.gid, "config: %v", toJson(lastConfig))
			kv.lock()
			kv.updateConfigHelper(lastConfig)
			kv.unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func formateMoveShardArgs(req *MoveShardArgs) string {
	x := map[string]interface{}{}
	x["ID"] = req.ID
	x["shard"] = req.Shard
	return toJson(x)
}

func (kv *ShardKV) MoveShard(req *MoveShardArgs, resp *MoveShardReply) {
	m := GetShard
	if !kv.isLeader() {
		resp.Err = ErrWrongLeader
		debugf(m, kv.me, kv.gid, "not leader, id: %v", req.ID)
		return
	}
	kv.lock()
	defer kv.unlock()
	resp.Err = OK
	if _, ok := kv.moveExecuted[req.ID]; ok {
		debugf(m, kv.me, kv.gid, "id:%v, executed", req.ID)
		return
	}
	if req.ShardConfig.Num > kv.ShardConfig.Num {
		// request config is newer
		debugf(m, kv.me, kv.gid, "id:%v, update config", req.ID)
		kv.updateConfigHelper(req.ShardConfig)
	} else if req.ShardConfig.Num < kv.ShardConfig.Num {
		resp.Err = ErrOldVersion
		debugf(m, kv.me, kv.gid, "id:%v, old config, self: %v, other: %v", req.ID, toJson(kv.ShardConfig), toJson(req.ShardConfig))
		return
	}
	shard := req.Shard
	if kv.ShardConfig.Shards[shard] != kv.gid {
		resp.Err = ErrWrongGroup
		return
	}
	op := Op{
		ID:   req.ID,
		Type: GetShardType,
		ShardData: &ShardData{
			Shard:       req.Shard,
			Data:        Copy(req.ShardData).(map[string]string),
			VersionData: Copy(req.VersionData).(map[int64]string),
			Executed:    Copy(req.Executed).(map[int64]bool),
		},
	}
	index, _, isleader := kv.rf.Start(op)
	if !isleader {
		resp.Err = ErrWrongLeader
		return
	}
	debugf(m, kv.me, kv.gid, "req: %v", formateMoveShardArgs(req))
	timeoutChan := make(chan bool, 1)
	go startTimeout(kv.cond, timeoutChan)
	timeout := false
	for !(index <= kv.lastAppliedIndex) && !timeout {
		select {
		case <-timeoutChan:
			timeout = true
		default:
			kv.cond.Wait() // wait, must hold mutex, after blocked, release lock
		}
	}
	if !kv.isLeader() {
		debugf(m, kv.me, kv.gid, "not leader, req: %v", toJson(req))
		resp.Err = ErrWrongLeader
		return
	}
	if timeout {
		resp.Err = ErrTimeout
		debugf(m, kv.me, kv.gid, "timeout!, req: %v", toJson(req))
		return
	}
}
