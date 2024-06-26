package shardkv

import (
	"6.5840/shardctrler"
	"fmt"
	"runtime"
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
	defer debugf(SendShard, me, fromgid, "->[g%v] success, req:%v", togid, toJson(req))
	for kv.isLeader() && !kv.killed() {
		for i := 0; i < len(groups); i++ {
			srvname := groups[i]
			srv := kv.make_end(srvname)
			debugf(SendShard, me, fromgid, "->[g%v]%v, req: %v", togid, srvname, toJson(req))
			ok := srv.Call("ShardKV.MoveShard", req, resp)
			if ok && resp.Err == OK {
				debugf(SendShard, me, fromgid, "success ->[g%v]%v, id:%v req: %v", togid, srvname, req.ID, toJson(req))
				return
			} else if ok && resp.Err == ErrWrongGroup {
				debugf(SendShard, me, fromgid, "fatal fail wrong group  ->[g%v]%v, id:%v, shard: %v", togid, srvname, req.ID, req.Shard)
				panic(resp.Err)
			} else if ok && resp.Err == ErrWrongLeader {
				debugf(SendShard, me, fromgid, "fail wrong leader  ->[g%v]%v, id:%v, shard: %v", togid, srvname, req.ID, req.Shard)
			} else if ok && resp.Err == ErrOldVersion {
				// debugf(SendShard, me, fromgid, "fatal fail old config  ->[g%v]%v, id:%v, shard: %v", togid, srvname, req.ID, req.Shard)
				msg := "old config" + toJson(req)
				panic(msg)
			} else if ok && resp.Err == ErrWaiting {
				debugf(SendShard, me, fromgid, "id: %v, need to wait", req.ID)
				time.Sleep(10 * time.Millisecond)
				i--
			}
		}
	}
}

func (kv *ShardKV) SendShard(shard int, gid int) {
	// get shard data,
	data := Copy(kv.Data[shard]).(map[string]string)
	versionData := Copy(kv.VersionData[shard]).(map[int64]string)
	executed := Copy(kv.Executed[shard]).(map[int64]bool)
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

func (kv *ShardKV) updateConfigHelper(lastConfig shardctrler.Config, ID int64) {
	if lastConfig.Num > kv.ShardConfig.Num {
		oldConf := kv.ShardConfig
		add, remove := getAddAndRemove(oldConf.Shards, lastConfig.Shards, kv.gid)
		// need to sync other servers in the group
		if ID == 0 {
			ID = nrand()
		}
		op := Op{
			ID:               ID,
			Type:             SyncConfigType,
			NeedRemoveShards: remove,
			NeedAddShards:    add,
			Config:           lastConfig,
		}
		debugf(Config, kv.me, kv.gid, "Op: %v", toJson(op))
		my := getSelfShards(lastConfig.Shards, kv.gid)
		index, _, _ := kv.rf.Start(op)
		debugf(Config, kv.me, kv.gid, "index: %v,my:%v, allocated: %v, newConfig: %v, state:%v", index, toJson(my), toJson(shardctrler.GetGIDShards(lastConfig.Shards)), toJson(lastConfig), toJson(kv.Data))
	}
}

func getGoroutineID() int {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	idStr := string(buf[:n])

	var id int
	fmt.Sscanf(idStr, "goroutine %d", &id)

	return id
}

func (kv *ShardKV) UpdateConfig() {
	for !kv.killed() {
		if kv.isLeader() {
			kv.lock()
			num := kv.ShardConfig.Num + 1
			kv.unlock()
			if num > 1 {
				num = -1
			}
			lastConfig := kv.mck.Query(num)
			kv.lock()
			debugf(Config, kv.me, kv.gid, "config: %v, state:%v", toJson(lastConfig), toJson(kv.Data))
			kv.updateConfigHelper(lastConfig, 0)
			kv.unlock()
		}
		time.Sleep(90 * time.Millisecond)
	}
}

func (kv *ShardKV) RequestUpdate(ID int64) {
	num := kv.ShardConfig.Num + 1
	kv.unlock()
	lastConfig := kv.mck.Query(num)
	kv.lock()
	debugf(Config, kv.me, kv.gid, "self: %v, last: %v", toJson(kv.ShardConfig), toJson(lastConfig))
	kv.updateConfigHelper(lastConfig, ID)
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
	if _, ok := kv.MoveExecuted[req.ID]; ok {
		debugf(m, kv.me, kv.gid, "id:%v, executed", req.ID)
		return
	}
	//if req.ShardConfig.Num > kv.ShardConfig.Num+1 {
	//	errMsg := fmt.Sprintf("%v > %v", req.ShardConfig.Num, kv.ShardConfig.Num)
	//	debugf(m, kv.me, kv.gid, "errmsg:%v, req: %v, selfConfig: %v", errMsg, toJson(req), toJson(kv.ShardConfig))
	//}
	if req.ShardConfig.Num > kv.ShardConfig.Num {
		// request config is newer
		//debugf(m, kv.me, kv.gid, "id:%v, shard: %v, update config", req.ID, req.Shard)
		//kv.updateConfigHelper(req.ShardConfig)
		debugf(m, kv.me, kv.gid, "id:%v, shard: %v, update config, %v < %v", req.ID, req.Shard, kv.ShardConfig.Num, req.ShardConfig.Num)
		resp.Err = ErrWaiting
		kv.RequestUpdate(req.ID)
		return
	} else if req.ShardConfig.Num < kv.ShardConfig.Num {
		//resp.Err = ErrOldVersion
		//debugf(m, kv.me, kv.gid, "id:%v, old config, self: %v, other: %v", req.ID, toJson(kv.ShardConfig), toJson(req.ShardConfig))
		resp.Err = ErrOldVersion
		return
	}
	op := Op{
		ID:     req.ID,
		Type:   GetShardType,
		Config: req.ShardConfig,
		ShardData: &ShardData{
			Shard:       req.Shard,
			Data:        Copy(req.ShardData).(map[string]string),
			VersionData: Copy(req.VersionData).(map[int64]string),
			Executed:    Copy(req.Executed).(map[int64]bool),
		},
	}
	debugf(m, kv.me, kv.gid, "start Op: %v", toJson(op))
	index, _, isleader := kv.rf.Start(op)
	if !isleader {
		resp.Err = ErrWrongLeader
		return
	}
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
	debugf(m, kv.me, kv.gid, "success, req:%v, holdonsharsds:%v, state:%v", toJson(req), toJson(kv.HoldShards), toJson(kv.Data))
}
