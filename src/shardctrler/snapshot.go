package shardctrler

import (
	"6.5840/labgob"
	"bytes"
	"time"
)

func (sc *ShardCtrler) dectionMaxSize() {
	if sc.maxraftstate == -1 {
		return
	}
	maxsize := sc.maxraftstate
	for !sc.killed() {
		time.Sleep(20 * time.Millisecond)
		sc.lock()
		size := sc.persiter.RaftStateSize()
		if size >= sc.maxraftstate {
			dumps := sc.dumpData()
			sc.rf.Snapshot(sc.lastAppliedIndex, dumps)
			debugf(MakeSnap, sc.me, "%v > %v, lastApplied: %v, newsize: %v", size, maxsize, sc.lastAppliedIndex, sc.persiter.RaftStateSize())
		}
		sc.unlock()
	}
}

func (sc *ShardCtrler) dumpData() []byte {
	w := new(bytes.Buffer)
	d := labgob.NewEncoder(w)
	var err error
	if err = d.Encode(sc.configs); err != nil {
		panic("decode fail")
	}
	if err = d.Encode(sc.executed); err != nil {
		panic("decode fail")
	}
	if err = d.Encode(sc.versionData); err != nil {
		panic(err)
	}

	return w.Bytes()
}

func (sc *ShardCtrler) applySnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var newState []Config
	var executed map[int64]bool
	var versionData map[int64]Config
	if err := d.Decode(&newState); err != nil {
		panic(err)
	} else {
		sc.configs = newState
	}
	if err := d.Decode(&executed); err != nil {
		panic(err)
	} else {
		sc.executed = executed
	}
	if err := d.Decode(&versionData); err != nil {
		panic(err)
	} else {
		sc.versionData = versionData
	}
}
