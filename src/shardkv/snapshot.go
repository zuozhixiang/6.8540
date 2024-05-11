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
	if err = d.Encode(kv.data); err != nil {
		panic("decode fail")
	}
	if err = d.Encode(kv.executed); err != nil {
		panic("decode fail")
	}
	if err = d.Encode(kv.versionData); err != nil {
		panic(err)
	}
	if err = d.Encode(kv.moveExecuted); err != nil {
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
	if err := d.Decode(&newState); err != nil {
		panic(err)
	} else {
		kv.data = newState
	}
	if err := d.Decode(&executed); err != nil {
		panic(err)
	} else {
		kv.executed = executed
	}
	if err := d.Decode(&versionData); err != nil {
		panic(err)
	} else {
		kv.versionData = versionData
	}
	if err := d.Decode(&moveExecuted); err != nil {
		panic(err)
	} else {
		kv.moveExecuted = moveExecuted
	}
}
