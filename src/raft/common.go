package raft

import (
	"encoding/json"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/exp/constraints"
	"math/rand"
	"runtime"
	"sync/atomic"
	"time"
)

var logger *zap.SugaredLogger

func init() {
	config := zap.NewDevelopmentConfig()
	enconfig := zap.NewDevelopmentEncoderConfig()
	enconfig.EncodeCaller = nil
	enconfig.EncodeTime = zapcore.TimeEncoderOfLayout(time.StampMilli)
	config.EncoderConfig = enconfig
	config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	logger1, err := config.Build()
	if err != nil {
		return
	}
	logger = logger1.Sugar()
}

const Debug = false

var stateMap = map[int32]string{
	Follower:  "Follower",
	Candidate: "Candidate",
	Leader:    "Leader",
}

type Method string

const (
	SendVote   Method = "SendVote"
	ReciveVote Method = "ReciveVote"

	SendHeart   Method = "SendHeart"
	ReciveHeart Method = "ReciveHeart"

	SendData   Method = "SendData"
	ReciveData Method = "ReciveData"

	ApplyMess Method = "ApplyMsg"

	UpdateCommitIndex Method = "UpdateCommitIndex"
)

const path = `/Users/zuozhixiang/zzx/6.5840/`
const Len = len(path)

func (rf *Raft) debugf(meth Method, format string, a ...interface{}) {
	if Debug {
		_, file, line, _ := runtime.Caller(1)
		fmt := "%v:%v %v [S%v][%v][T%v] " + format
		state := atomic.LoadInt32(&rf.State)
		term := atomic.LoadInt32(&rf.CurrentTerm)
		me := rf.me
		x := []interface{}{file[Len:], line, meth, me, stateMap[state], term}
		x = append(x, a...)
		logger.Debugf(fmt, x...)
	}
}

func (rf *Raft) infof(format string, a ...interface{}) {
	if Debug {
		_, file, line, _ := runtime.Caller(1)
		fmt := "%v:%v [S%v][%v][T%v] " + format
		state := atomic.LoadInt32(&rf.State)
		term := atomic.LoadInt32(&rf.CurrentTerm)
		me := rf.me
		x := []interface{}{file[Len:], line, me, stateMap[state], term}
		x = append(x, a...)
		logger.Infof(fmt, x...)
	}
}

func (rf *Raft) warnf(format string, a ...interface{}) {
	if Debug {
		_, file, line, _ := runtime.Caller(1)
		fmt := "%v:%v [S%v][%v][T%v] " + format
		state := atomic.LoadInt32(&rf.State)
		term := atomic.LoadInt32(&rf.CurrentTerm)
		me := rf.me
		x := []interface{}{file[Len:], line, me, stateMap[state], term}
		x = append(x, a...)
		logger.Warnf(fmt, x...)
	}
}

func (rf *Raft) errorf(format string, a ...interface{}) {
	if Debug {
		_, file, line, _ := runtime.Caller(1)
		fmt := "%v:%v [S%v][%v][T%v] " + format
		state := atomic.LoadInt32(&rf.State)
		term := atomic.LoadInt32(&rf.CurrentTerm)
		me := rf.me
		x := []interface{}{file[Len:], line, me, stateMap[state], term}
		x = append(x, a...)
		logger.Errorf(fmt, x...)
	}
}

func toJson(data interface{}) string {
	marshal, err := json.Marshal(data)
	if err != nil {
		return ""
	}
	return string(marshal)
}

const Max = int64(1) << 61

func getID() int64 {
	return rand.Int63n(Max)
}

func GetRand(left, right int) int {
	return left + rand.Intn(right-left+1)
}

func GetNow() int64 {
	return time.Now().UnixMilli()
}

func min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	} else {
		return b
	}
}

func max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	} else {
		return b
	}
}
