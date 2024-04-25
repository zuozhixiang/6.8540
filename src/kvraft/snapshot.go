package kvraft

import (
	"6.5840/labgob"
	"bytes"
	"time"
)

func (kv *KVServer) dectionMaxSize() {
	if kv.maxraftstate == -1 {
		return
	}
	maxsize := kv.maxraftstate
	for !kv.killed() {
		time.Sleep(30 * time.Millisecond)
		kv.lock()
		size := kv.persiter.RaftStateSize()
		if size >= kv.maxraftstate {
			dumps := kv.dumpData()
			kv.rf.Snapshot(kv.lastAppliedIndex, dumps)
			debugf(MakeSnap, kv.me, "%v > %v, lastApplied: %v, newsize: %v", size, maxsize, kv.lastAppliedIndex, kv.persiter.RaftStateSize())
		}
		kv.unlock()
	}
}

func (kv *KVServer) dumpData() []byte {
	w := new(bytes.Buffer)
	d := labgob.NewEncoder(w)
	err := d.Encode(kv.data)
	if err != nil {
		return nil
	}
	err = d.Encode(kv.executed)
	if err != nil {
		return nil
	}
	return w.Bytes()
}

func (kv *KVServer) applySnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var newState map[string]string
	var executed map[string]bool
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
}
