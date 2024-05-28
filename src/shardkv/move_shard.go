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

func (kv *ShardKV) MoveShard() {
	for !kv.killed() {
		if kv.isLeader() {
			kv.send()
		}
		time.Sleep(20 * time.Millisecond)
	}
}
