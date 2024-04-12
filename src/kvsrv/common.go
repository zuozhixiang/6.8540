package kvsrv

import (
	"encoding/json"
	"go.uber.org/zap"
)

// Put or Append
type PutAppendArgs struct {
	ID    int64
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

func toJson(data interface{}) string {
	marshal, err := json.Marshal(data)
	if err != nil {
		return ""
	}
	return string(marshal)
}

type PutAppendReply struct {
	Value string
}
type GetArgs struct {
	Key string
}

type GetReply struct {
	Value string
}

type NotifyRequest struct {
	ID int64
}

type NotifyResponse struct {
}

var logger *zap.SugaredLogger

func init() {
	config := zap.NewDevelopmentConfig()
	config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	logger1, err := config.Build()
	if err != nil {
		return
	}
	logger = logger1.Sugar()
}
