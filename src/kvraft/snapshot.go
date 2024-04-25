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
	for !kv.killed() {
		time.Sleep(100 * time.Millisecond)
		kv.lock()
		if kv.persiter.RaftStateSize() >= kv.maxraftstate {
			dumps := kv.dumpData()
			kv.rf.Snapshot(kv.lastAppliedIndex, dumps)
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
	return w.Bytes()
}

func (kv *KVServer) applySnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var newState map[string]string
	if err := d.Decode(&newState); err != nil {
		panic(err)
	} else {
		kv.data = newState
	}
}
