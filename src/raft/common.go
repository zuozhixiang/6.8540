package raft

import (
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/exp/constraints"
	"math/rand"
	"os"
	"runtime"
	"sync/atomic"
	"time"
)

const Debug = true // print log switch

var logger *zap.SugaredLogger

// init logger object, globall single, set logger's config
func init() {
	config := zap.NewDevelopmentConfig()
	enconfig := zap.NewDevelopmentEncoderConfig()
	enconfig.EncodeCaller = nil
	enconfig.EncodeTime = zapcore.TimeEncoderOfLayout(time.StampMilli) // set format of time
	config.EncoderConfig = enconfig
	config.Level = zap.NewAtomicLevelAt(zap.DebugLevel) // set print log level
	logger1, err := config.Build()
	if err != nil {
		return
	}
	logger = logger1.Sugar()
}

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

	UpdateCommitIndex Method = "UpdateCommit"
	LeaderElection    Method = "Election"

	ArriveMsg Method = "ArriveMsg"

	Snapshot   Method = "ConSnapshot"
	SendSnap   Method = "SendSnap"
	ReciveSnap Method = "ReciveSnap"
)

var Len int

func init() {
	prefixPath, _ := os.Getwd()
	Len = len(prefixPath) + 1
}

func (rf *Raft) debugf(meth Method, format string, a ...interface{}) {
	if Debug {
		_, file, line, _ := runtime.Caller(1)
		pos := fmt.Sprintf("%v:%v", file[Len:], line) // print log code line
		state := atomic.LoadInt32(&rf.State)
		term := atomic.LoadInt32(&rf.CurrentTerm)
		me := rf.me
		info := fmt.Sprintf("[S%v][%v][T%v]", me, stateMap[state], term)
		fmt := "%-24v %-12v %-20v" + format

		x := []interface{}{pos, meth, info}
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

// get random request id
func getID() int64 {
	return rand.Int63n(Max)
}

// get current timestamp
func GetNow() int64 {
	return time.Now().UnixMilli()
}

// get min one in a, b
func min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	} else {
		return b
	}
}

// max
func max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	} else {
		return b
	}
}
