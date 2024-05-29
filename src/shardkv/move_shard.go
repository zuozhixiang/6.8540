package shardkv

import (
	"6.5840/shardctrler"
	"sync"
	"time"
)

func (kv *ShardKV) send() {
	kv.lock()
	defer kv.unlock()
	var wg sync.WaitGroup
	for i := 0; i < len(kv.AllShardState); i++ {
		if kv.AllShardState[i] == Pushing {
			sendGID := kv.ShardConfig.Shards[i]
			shard := i
			kv.SendShard(shard, sendGID, &wg)
		}
	}
	wg.Wait()
}

func (kv *ShardKV) SendShard(shard int, gid int, wg *sync.WaitGroup) {
	// get shard data,
	wg.Add(1)
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
	go kv.sendShard(kv.me, kv.gid, gid, req, kv.ShardConfig.Groups[gid], wg)
}

func (kv *ShardKV) sendShard(me int, fromgid int, togid int, req *MoveShardArgs, groups []string, wg *sync.WaitGroup) {
	resp := &MoveShardReply{}
	defer debugf(SendShard, me, fromgid, "->[g%v] success, req:%v", togid, toJson(req))
	defer wg.Done()
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

func (kv *ShardKV) checkAndSendShard() {
	for !kv.killed() {
		if kv.isLeader() {
			kv.send()
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (kv *ShardKV) MoveShard(req *MoveShardArgs, resp *MoveShardReply) {
	if kv.killed() {
		return
	}
	if !kv.isLeader() {
		resp.Err = ErrWrongLeader
		return
	}
	m := Method("MoveShard")
	kv.lock()
	defer kv.unlock()
	newConfig := &req.ShardConfig
	if newConfig.Num < kv.ShardConfig.Num {
		resp.Err = ErrOldVersion
		debugf(m, kv.me, kv.gid, "err old verion, req: %v", req.ID)
		return
	}
	if newConfig.Num > kv.ShardConfig.Num {
		resp.Err = ErrWaiting
		debugf(m, kv.me, kv.gid, "need wait req: %v", req.ID)
		return
	}
	resp.Err = OK
	op := Op{
		ID:     req.ID,
		Type:   MoveShardType,
		Config: *newConfig,

		ShardData: &ShardData{
			Shard:       req.Shard,
			Data:        req.ShardData,
			VersionData: req.VersionData,
			Executed:    req.Executed,
		},
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		debugf(m, kv.me, kv.gid, "not leader, req: %v", toJson(req))
		resp.Err = ErrWrongLeader
		return
	}
	debugf(m, kv.me, kv.gid, "req: %v, index: %v, term:%v", toJson(req), index, term)
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
	debugf(m, kv.me, kv.gid, "success id: %v", req.ID)
	return
}
