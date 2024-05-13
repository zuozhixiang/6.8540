package shardkv

import (
	"6.5840/labgob"
	"6.5840/shardctrler"
	"bytes"
	"time"
)

func (kv *ShardKV) dectionMaxSize() {
	if kv.maxraftstate == -1 {
		return
	}
	maxsize := kv.maxraftstate
	for !kv.killed() {
		time.Sleep(20 * time.Millisecond)
		size := kv.persiter.RaftStateSize()
		kv.lock()
		if size >= maxsize {
			dumps := kv.dumpData()
			kv.rf.Snapshot(kv.lastAppliedIndex, dumps)
			debugf(MakeSnap, kv.me, kv.gid, "%v > %v, lastApplied: %v, newsize: %v", size, maxsize, kv.lastAppliedIndex, kv.persiter.RaftStateSize())
		}
		kv.unlock()
	}
}

func (kv *ShardKV) dumpData() []byte {
	w := new(bytes.Buffer)
	d := labgob.NewEncoder(w)
	var err error
	if err = d.Encode(kv.Data); err != nil {
		panic("decode fail")
	}
	if err = d.Encode(kv.Executed); err != nil {
		panic("decode fail")
	}
	if err = d.Encode(kv.VersionData); err != nil {
		panic(err)
	}
	if err = d.Encode(kv.MoveExecuted); err != nil {
		panic(err)
	}
	if err = d.Encode(kv.ShardConfig); err != nil {
		panic(err)
	}
	if err = d.Encode(kv.NoReadyShardSet); err != nil {
		panic(err)
	}
	if err = d.Encode(kv.HoldShards); err != nil {
		panic(err)
	}
	if err = d.Encode(kv.Shards); err != nil {
		panic(err)
	}
	if err = d.Encode(kv.lastAppliedIndex); err != nil {
		panic(err)
	}

	return w.Bytes()
}

func (kv *ShardKV) applySnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var newState [shardctrler.NShards]map[string]string
	var executed [shardctrler.NShards]map[int64]bool
	var versionData [shardctrler.NShards]map[int64]string
	var moveExecuted map[int64]bool
	var lastApplied int

	if err := d.Decode(&newState); err != nil {
		panic(err)
	} else {
		kv.Data = newState
	}
	if err := d.Decode(&executed); err != nil {
		panic(err)
	} else {
		kv.Executed = executed
	}
	if err := d.Decode(&versionData); err != nil {
		panic(err)
	} else {
		kv.VersionData = versionData
	}
	if err := d.Decode(&moveExecuted); err != nil {
		panic(err)
	} else {
		kv.MoveExecuted = moveExecuted
	}

	var ShardConfig shardctrler.Config
	var shards map[int]bool
	var NoReadyShardSet map[int]bool
	var HoldShards [shardctrler.NShards]bool
	if err := d.Decode(&ShardConfig); err != nil {
		panic(err)
	} else {
		kv.ShardConfig = ShardConfig
	}
	if err := d.Decode(&NoReadyShardSet); err != nil {
		panic(err)
	} else {
		kv.NoReadyShardSet = NoReadyShardSet
	}
	if err := d.Decode(&HoldShards); err != nil {
		panic(err)
	} else {
		kv.HoldShards = HoldShards
	}
	if err := d.Decode(&shards); err != nil {
		panic(err)
	} else {
		kv.Shards = shards
	}
	if err := d.Decode(&lastApplied); err != nil {
		panic(err)
	} else {
		kv.lastAppliedIndex = lastApplied
	}

}
