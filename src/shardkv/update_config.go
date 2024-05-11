package shardkv

import (
	"6.5840/shardctrler"
	"time"
)

func (kv *ShardKV) UpdateConfig() {
	for !kv.killed() {
		time.Sleep(95 * time.Millisecond)
		if !kv.isLeader() {
			continue
		}
		lastConfig := kv.mck.Query(-1)
		kv.lock()
		kv.ShardConfig = lastConfig
		shards := map[int]bool{}
		for shard, gid := range lastConfig.Shards {
			if gid == kv.gid {
				shards[shard] = true
			}
		}
		kv.shards = shards
		debugf(Config, kv.me, kv.gid, "my:%v, allocated: %v, newConfig: %v,", toJson(kv.shards), toJson(shardctrler.GetGIDShards(lastConfig.Shards)), toJson(lastConfig))

		kv.unlock()
	}
}
