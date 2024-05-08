package shardkv

import "time"

func (kv *ShardKV) UpdateConfig() {
	for !kv.killed() {
		time.Sleep(1 * time.Second)
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
		kv.unlock()
	}
}
