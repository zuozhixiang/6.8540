package shardctrler

import (
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
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

var Len int

func init() {
	prefixPath, _ := os.Getwd()
	Len = len(prefixPath) + 1
}

type Method string

const (
	KillMethod  Method = "Kill"
	JoinMethod  Method = "Join"
	QueryMethod Method = "Query"
	LeaveMethod Method = "Leave"
	MoveMethod  Method = "Move"
)

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

func toJson(data interface{}) string {
	res, _ := json.Marshal(data)
	return string(res)
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
