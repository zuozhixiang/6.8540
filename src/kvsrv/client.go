package kvsrv

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	req := &GetArgs{Key: key}
	resp := &GetReply{}
	flag := ck.server.Call("KVServer.Get", req, resp)
	for !flag {
		//  logger.Debugf("Get, req: %v, resp: %v", toJson(req), toJson(resp))
		flag = ck.server.Call("KVServer.Get", req, resp)
		if flag {
			//  logger.Debugf("Success Get, req: %v, resp: %v", toJson(req), toJson(resp))
			break
		}
	}
	return resp.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	id := nrand()
	req := &PutAppendArgs{Key: key, Value: value, ID: id}
	resp := &PutAppendReply{}
	flag := ck.server.Call("KVServer."+op, req, resp)
	for !flag {
		//  logger.Debugf("%v fail, req: %v, resp: %v", op, toJson(req), toJson(resp))
		flag = ck.server.Call("KVServer."+op, req, resp)
		if flag {
			//  logger.Debugf("Success %v, req: %v, resp: %v", op, toJson(req), toJson(resp))
			break
		}
	}
	if op == "Append" {
		ck.Notify(id)
	}
	return resp.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) Notify(id int64) {
	req := &NotifyRequest{ID: id}
	resp := &NotifyResponse{}
	for !ck.server.Call("KVServer.Notify", req, resp) {
		DPrintf("notify fail: %v", id)
	}
}
