package kvraft

import (
	"6.5840/labrpc"
	"fmt"
	"sync/atomic"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	LeaderID     int // client need to know who is leader, leaderID maybe incorrect
	ClientID     int64
	GenRequestID atomic.Int64
}

func (ck *Clerk) getNextRequestID() string {
	id := ck.GenRequestID.Load()
	ck.GenRequestID.Add(1)
	return fmt.Sprintf("%v-%v", ck.ClientID, id)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.ClientID = nrand()
	ck.GenRequestID.Store(0)

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	rpcname := "KVServer." + "Get"
	req := &GetArgs{
		ID:  ck.getNextRequestID(),
		Key: key,
	}
	resp := &GetReply{}
	for {
		resp = &GetReply{}
		debugf(SendGet, int(ck.ClientID), "->[S%v]req: %v", ck.LeaderID, toJson(req))
		ok := ck.servers[ck.LeaderID].Call(rpcname, req, resp)
		if ok && resp.Err == ErrWrongLeader {
			debugf(SendGet, int(ck.ClientID), "->[S%v] fail, id: %v, resp: %v", ck.LeaderID, req.ID, toJson(resp))
			ok = false
		}
		if !ok {
			ck.LeaderID = (ck.LeaderID + 1) % len(ck.servers)
		} else {
			break
		}
	}
	debugf(SendGet, int(ck.ClientID), "->[S%v] success, id: %v, resp: %v", ck.LeaderID, req.ID, toJson(resp))
	return resp.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	rpcname := "KVServer." + op
	req := &PutAppendArgs{
		ID:    ck.getNextRequestID(),
		Key:   key,
		Value: value,
	}
	m := SendApp
	if op == "Put" {
		m = SendPut
	}
	resp := &PutAppendReply{}
	for {
		resp = &PutAppendReply{}
		debugf(m, int(ck.ClientID), "->[S%v] req: %v", ck.LeaderID, toJson(req))
		ok := ck.servers[ck.LeaderID].Call(rpcname, req, resp)
		if ok && resp.Err == ErrWrongLeader {
			debugf(m, int(ck.ClientID), "->[S%v] fail, id: %v, resp: %v", ck.LeaderID, req.ID, toJson(resp))
			ok = false
		}
		if !ok {
			ck.LeaderID = (ck.LeaderID + 1) % len(ck.servers)
		} else {
			break
		}
	}
	debugf(m, int(ck.ClientID), "->[S%v] success, id: %v, resp: %v", ck.LeaderID, req.ID, toJson(resp))
	// notify server delete memory
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
