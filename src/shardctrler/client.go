package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.5840/labrpc"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers   []*labrpc.ClientEnd
	serverNum int
	clientID  int
	leaderID  int // client need to know who is leader
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.serverNum = len(servers)
	ck.leaderID = int(nrand()) % ck.serverNum
	ck.clientID = int(nrand())
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ok := false
	req := QueryArgs{
		ID:  nrand(),
		Num: num,
	}
	cnt := 0
	var res Config
	for {
		// try each known server.
		serv := ck.servers[ck.leaderID]
		var resp QueryReply
		debugf(SendQuery, ck.clientID, "req: %v", toJson(req))
		ok = serv.Call("ShardCtrler.Query", &req, &resp)
		if !ok || ok && resp.Err != OK {
			ok = false
			ck.leaderID = (ck.leaderID + 1) % ck.serverNum
			debugf(SendQuery, ck.clientID, "fail req: %v, resp: %v", toJson(req), toJson(resp))
		} else {
			res = resp.Config
			break
		}
		cnt++
		if cnt == ck.serverNum {
			cnt = 0
			time.Sleep(100 * time.Microsecond)
		}
	}
	debugf(SendQuery, ck.clientID, "success req: %v, resp: %v", toJson(req), toJson(res))
	return res
}

func (ck *Clerk) Join(servers map[int][]string) {
	req := JoinArgs{
		ID:      nrand(),
		Servers: servers,
	}
	ok := false
	cnt := 0
	for {
		// try each known server.
		var resp JoinReply
		srv := ck.servers[ck.leaderID]
		debugf(SendJoin, ck.clientID, "req: %v", toJson(req))
		ok = srv.Call("ShardCtrler.Join", &req, &resp)
		if !ok || ok && resp.Err != OK {
			ok = false
			ck.leaderID = (ck.leaderID + 1) % ck.serverNum
			debugf(SendJoin, ck.clientID, "fail req: %v, resp: %v", toJson(req), toJson(resp))
		} else {
			break
		}
		cnt++
		if cnt == ck.serverNum {
			cnt = 0
			time.Sleep(100 * time.Microsecond)
		}
	}
	debugf(SendJoin, ck.clientID, "success req: %v", toJson(req))
}

func (ck *Clerk) Leave(gids []int) {
	req := LeaveArgs{
		ID:   nrand(),
		GIDs: gids,
	}
	ok := false
	cnt := 0
	for {
		// try each known server.
		var resp JoinReply
		srv := ck.servers[ck.leaderID]
		debugf(SendLeave, ck.clientID, "req: %v", toJson(req))
		ok = srv.Call("ShardCtrler.Leave", &req, &resp)
		if !ok || ok && resp.Err != OK {
			ok = false
			ck.leaderID = (ck.leaderID + 1) % ck.serverNum
			debugf(SendLeave, ck.clientID, "fail req: %v, resp: %v", toJson(req), toJson(resp))
		} else {
			break
		}
		cnt++
		if cnt == ck.serverNum {
			cnt = 0
			time.Sleep(100 * time.Microsecond)
		}
	}
	debugf(SendLeave, ck.clientID, "success req: %v", toJson(req))
}

func (ck *Clerk) Move(shard int, gid int) {
	req := MoveArgs{
		ID:    nrand(),
		Shard: shard,
		GID:   gid,
	}
	ok := false
	cnt := 0
	for {
		// try each known server.
		var resp JoinReply
		srv := ck.servers[ck.leaderID]
		debugf(SendMove, ck.clientID, "success req: %v", toJson(req))
		ok = srv.Call("ShardCtrler.Move", &req, &resp)
		if !ok || ok && resp.Err != OK {
			ok = false
			ck.leaderID = (ck.leaderID + 1) % ck.serverNum
			debugf(SendMove, ck.clientID, "fail req: %v, resp: %v", toJson(req), toJson(resp))
		} else {
			break
		}
		cnt++
		if cnt == ck.serverNum {
			cnt = 0
			time.Sleep(100 * time.Microsecond)
		}
	}
	debugf(SendMove, ck.clientID, "success req: %v", toJson(req))
}
