package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	ID    string
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	ID  string
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   string
	Value string
}

type NotifyFinishedRequest struct {
	ID string
}

type NotifyFinishedResponse struct {
	Err string
}
