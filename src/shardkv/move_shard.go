package shardkv

import (
	"6.5840/shardctrler"
	"sync"
	"time"
)

func (kv *ShardKV) send() {
	kv.lock()
	flag := false
	for i := 0; i < len(kv.AllShardState); i++ {
		if kv.AllShardState[i] == Pushing {
			flag = true
		}
	}
	if !flag {
		kv.unlock()
		return
	}
	shardConfig := Copy(kv.ShardConfig).(shardctrler.Config)
	shardData := Copy(kv.ShardData).([shardctrler.NShards]*Shard)
	kv.unlock()
	var wg sync.WaitGroup
	for i := 0; i < len(kv.AllShardState); i++ {
		if kv.AllShardState[i] == Pushing {
			sendGID := shardConfig.Shards[i]
			shard := i
			kv.SendShard(shardConfig, *shardData[i], shard, sendGID, &wg)
		}
	}
	// debugf(Method("test"), kv.me, kv.gid, "block")
	wg.Wait()
}

func (kv *ShardKV) SendShard(shardConfig shardctrler.Config, shard Shard, shardID int, gid int, wg *sync.WaitGroup) {
	// get shard data,
	wg.Add(1)
	shardConfig = Copy(shardConfig).(shardctrler.Config)
	shard = Copy(shard).(Shard)
	req := &MoveShardArgs{
		ID:          nrand(),
		ShardID:     shardID,
		Data:        &shard,
		ShardConfig: shardConfig,
		FromGID:     kv.gid,
		Me:          kv.me,
	}
	go kv.sendShard(kv.me, kv.gid, gid, req, kv.ShardConfig.Groups[gid], wg)
}

func (kv *ShardKV) sendShard(me int, fromgid int, togid int, req *MoveShardArgs, groups []string, wg *sync.WaitGroup) {
	resp := &MoveShardReply{}
	defer debugf(SendShard, me, fromgid, "->[g%v] success, req:%v", togid, toJson(req))
	defer wg.Done()
	for kv.isLeader() {
		for i := 0; i < len(groups); i++ {
			srvname := groups[i]
			srv := kv.make_end(srvname)
			debugf(SendShard, me, fromgid, "->[g%v]%v, req: %v", togid, srvname, toJson(req))
			ok := srv.Call("ShardKV.MoveShard", req, resp)
			if ok && resp.Err == OK {
				debugf(SendShard, me, fromgid, "success ->[g%v]%v, id:%v req: %v", togid, srvname, req.ID, toJson(req))
				return
			} else if ok && resp.Err == ErrWrongGroup {
				debugf(SendShard, me, fromgid, "fatal fail wrong group  ->[g%v]%v, id:%v, shard: %v", togid, srvname, req.ID, req.ShardID)
				panic(resp.Err)
			} else if ok && resp.Err == ErrWrongLeader {
				debugf(SendShard, me, fromgid, "fail wrong leader  ->[g%v]%v, id:%v, shard: %v", togid, srvname, req.ID, req.ShardID)
			} else if ok && resp.Err == ErrOldVersion {
				// debugf(SendShard, me, fromgid, "fatal fail old config  ->[g%v]%v, id:%v, shard: %v", togid, srvname, req.ID, req.Shard)
				msg := "old config" + toJson(req)
				panic(msg)
			} else if ok && resp.Err == ErrWaiting {
				debugf(SendShard, me, fromgid, "id: %v, need to wait", req.ID)
				time.Sleep(100 * time.Millisecond)
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

func (kv *ShardKV) sendMoveDone(ID int64, groups []string, me int, shardID int) {
	req := &MoveDoneArgs{ShardID: shardID, ID: ID}
	resp := &MoveShardReply{}
	for i := me; ; i++ {
		i = i % len(groups)
		srvname := groups[i]
		srv := kv.make_end(srvname)
		ok := srv.Call("ShardKV.MoveDone", req, resp)
		debugf(MoveDoneM, kv.me, kv.gid, "req: %v", toJson(req))
		if ok {
			if resp.Err == OK {
				break
			}
		}
	}
	kv.lock()
	kv.HoldNormalShards[shardID] = true
	kv.AllShardState[shardID] = Serving
	kv.unlock()
}
