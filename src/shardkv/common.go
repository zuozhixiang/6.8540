package shardkv

import "6.5840/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongGroup   = "ErrWrongGroup"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrWaiting      = "ErrWaiting"
	ErrTimeout      = "ErrTimeout"
	ErrShardNoReady = "ErrShardNoReady"
	ErrOldVersion   = "ErrOldVersion"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	ID    int64
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ID  int64
	Key string
}

type GetReply struct {
	Err   Err
	Value string
}

type MoveShardArgs struct {
	ID          int64
	Shard       int
	ShardData   map[string]string
	VersionData map[int64]string
	Executed    map[int64]bool
	ShardConfig shardctrler.Config
}

type MoveShardReply struct {
	Err Err
}
