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
	if err = d.Encode(kv.ShardConfig); err != nil {
		panic(err)
	}
	if err = d.Encode(kv.AllShardState); err != nil {
		panic(err)
	}
	if err = d.Encode(kv.HoldNormalShards); err != nil {
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

	var ShardConfig shardctrler.Config
	var AllShardState [shardctrler.NShards]ShardState
	var HoldNormalShards map[int]bool
	if err := d.Decode(&ShardConfig); err != nil {
		panic(err)
	} else {
		kv.ShardConfig = ShardConfig
	}
	if err := d.Decode(&AllShardState); err != nil {
		panic(err)
	} else {
		kv.AllShardState = AllShardState
	}
	if err := d.Decode(&HoldNormalShards); err != nil {
		panic(err)
	} else {
		kv.HoldNormalShards = HoldNormalShards
	}

	if err := d.Decode(&lastApplied); err != nil {
		panic(err)
	} else {
		kv.lastAppliedIndex = lastApplied
	}
}
