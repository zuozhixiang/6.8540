package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

// Put or Append
type PutAppendArgs struct {
	ID    int64
	Key   string
	Value string
}

type PutAppendReply struct {
	Err string
}

type GetArgs struct {
	ID  int64
	Key string
}

type GetReply struct {
	Err   string
	Value string
}

type NotifyFinishedRequest struct {
	ID int64
}

type NotifyFinishedResponse struct {
	Err string
}

type Result struct {
	Err   string
	Value string
}
