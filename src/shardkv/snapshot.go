package shardkv

import (
	"6.5840/labgob"
	"6.5840/shardctrler"
	"bytes"
)

func (kv *ShardKV) dumpData() []byte {
	w := new(bytes.Buffer)
	d := labgob.NewEncoder(w)
	var err error
	if err = d.Encode(kv.ShardData); err != nil {
		panic("decode fail")
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
	var shardData [shardctrler.NShards]*Shard

	if err := d.Decode(&data); err != nil {
		panic(err)
	} else {
		kv.ShardData = shardData
	}

	var ShardConfig shardctrler.Config
	var AllShardState [shardctrler.NShards]ShardState
	var HoldNormalShards map[int]bool
	var lastApplied int
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
