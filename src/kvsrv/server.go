package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu   sync.Mutex
	data map[string]string

	oldData  map[int64]string
	executed map[int64]bool
	// Your definitions here.
}

func (kv *KVServer) lock() {
	kv.mu.Lock()
}

func (kv *KVServer) unlock() {
	kv.mu.Unlock()
}

func (kv *KVServer) checkExecuted(req *PutAppendArgs) bool {
	if _, ok := kv.executed[req.ID]; ok {
		return true
	}
	kv.executed[req.ID] = true
	return false
}

func (kv *KVServer) Get(req *GetArgs, resp *GetReply) {
	// Your code here.
	kv.lock()
	defer kv.unlock()
	//  logger.Debugf("ser Sucess Get:  req: %v, resp: %v", toJson(req), toJson(resp))
	if value, ok := kv.data[req.Key]; ok {
		resp.Value = value
		return
	}
	resp.Value = ""
}

func (kv *KVServer) Put(req *PutAppendArgs, resp *PutAppendReply) {
	// Your code here.
	kv.lock()
	defer kv.unlock()
	if kv.checkExecuted(req) {
		//  logger.Infof("Put ID: %v has Executed", req.ID)
		return
	}
	kv.data[req.Key] = req.Value
	//  logger.Debugf("ser Sucess Put:  req: %v, resp: %v", toJson(req), toJson(resp))
}

func (kv *KVServer) Append(req *PutAppendArgs, resp *PutAppendReply) {
	// Your code here.
	kv.lock()
	defer kv.unlock()
	if kv.checkExecuted(req) {
		//  logger.Infof("Append ID: %v has Executed, req: %v, resp: %v", req.ID, toJson(req), toJson(resp))
		resp.Value = kv.oldData[req.ID]
		return
	}
	if _, ok := kv.data[req.Key]; !ok {
		//  logger.Errorf("append illegal req: %v", toJson(req))
		kv.data[req.Key] = req.Value
		kv.oldData[req.ID] = ""
		resp.Value = ""
		return
	}
	oldv := kv.data[req.Key]
	newv := oldv + req.Value
	kv.oldData[req.ID] = oldv
	kv.data[req.Key] = newv
	resp.Value = oldv
	//  logger.Debugf("ser Sucess Append:  req: %v, resp: %v", toJson(req), toJson(resp))
}

func (kv *KVServer) Notify(req *NotifyRequest, resp *NotifyResponse) {
	kv.lock()
	defer kv.unlock()
	delete(kv.oldData, req.ID)
}

func StartKVServer() *KVServer {
	kv := &KVServer{
		mu:       sync.Mutex{},
		data:     map[string]string{},
		executed: map[int64]bool{},
		oldData:  map[int64]string{},
	}
	// You may need initialization code here.

	return kv
}
