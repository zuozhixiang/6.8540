package kvraft

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"math/big"
	"os"
	"runtime"
	"sync"
	"time"
)

const Debug = false

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

func toJson(data interface{}) string {
	marshal, err := json.Marshal(data)
	if err != nil {
		return ""
	}
	return string(marshal)
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()

	return x
}

type Method string

const (
	GetMethod    Method = "Get"
	PutMethod    Method = "Put"
	AppendMethod Method = "Append"
	KILL         Method = "Kill"
	SendGet      Method = "SendGet"
	SendPut      Method = "SendPut"
	SendApp      Method = "SendApp"
	Apply        Method = "Apply"
	AppSnap      Method = "AppSnap"
	MakeSnap     Method = "MakeSnap"
	Start        Method = "Start1"
	SendNotify   Method = "SendNoti"
	Notify       Method = "Notify"
)

var Len int

func init() {
	prefixPath, _ := os.Getwd()
	Len = len(prefixPath) + 1
}

func debugf(meth Method, me int, format string, a ...interface{}) {
	if Debug {
		_, file, line, _ := runtime.Caller(1)
		pos := fmt.Sprintf("%v:%v", file[Len:], line) // print log code line
		info := fmt.Sprintf("[S%v]", me)
		fmt := "%-16v %-8v %-22v " + format

		x := []interface{}{pos, meth, info}
		x = append(x, a...)
		logger.Debugf(fmt, x...)
	}
}

func startTimeout(cond *sync.Cond, timeoutChan chan bool) {
	timeout := time.After(700 * time.Millisecond)
	select {
	case <-timeout:
		{
			timeoutChan <- true
			cond.Broadcast()
		}
	}
}
