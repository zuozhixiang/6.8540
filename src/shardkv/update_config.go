package shardkv

import (
	"6.5840/shardctrler"
	"fmt"
	"runtime"
	"time"
)

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

func (kv *ShardKV) checkState() bool {
	for _, state := range kv.AllShardState {
		if state == Pulling || state == Pushing {
			return false
		}
	}
	return true
}

func (kv *ShardKV) UpdateConfig() {
	for !kv.killed() {
		if kv.isLeader() {
			kv.lock()
			// all shards state is not pushing or pulling
			if kv.checkState() {
				num := kv.ShardConfig.Num + 1
				newConfig := kv.mck.Query(num)
				if newConfig.Num == num {
					op := &Op{
						ID:     nrand(),
						Type:   SyncConfigType,
						Config: newConfig,
					}
					debugf(Config, kv.me, kv.gid, "get new Config: %v", toJson(newConfig))
					kv.rf.Start(op)
				}
			}

			kv.unlock()
		}
		time.Sleep(90 * time.Millisecond)
	}
}

func formateMoveShardArgs(req *MoveShardArgs) string {
	x := map[string]interface{}{}
	x["ID"] = req.ID
	x["shard"] = req.Shard
	return toJson(x)
}
