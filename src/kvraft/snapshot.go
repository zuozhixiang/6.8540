package kvraft

import (
	"6.5840/labgob"
	"bytes"
)

func (kv *KVServer) dumpData() []byte {
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

	return w.Bytes()
}

func (kv *KVServer) applySnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var newState map[string]string
	var executed map[int64]bool
	var versionData map[int64]string
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
}
