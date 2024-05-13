package shardkv

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"math/big"
	"os"
	"reflect"
	"runtime"
	"sync"
	"time"
)

const Debug = true

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
	Config       Method = "GetConfig"
	SendShard    Method = "SendShard"
	GetShard     Method = "GetShard"
)

var Len int

func init() {
	prefixPath, _ := os.Getwd()
	Len = len(prefixPath) + 1
}

func debugf(meth Method, me int, gid int, format string, a ...interface{}) {
	if Debug {
		_, file, line, _ := runtime.Caller(1)
		pos := fmt.Sprintf("%v:%v", file[Len:], line) // print log code line
		info := fmt.Sprintf("[G%v][S%v]", gid, me)
		fmt := "%-20v %-8v %-22v " + format

		x := []interface{}{pos, meth, info}
		x = append(x, a...)
		logger.Debugf(fmt, x...)
	}
}

func startTimeout(cond *sync.Cond, timeoutChan chan bool) {
	timeout := time.After(600 * time.Millisecond)
	select {
	case <-timeout:
		{
			timeoutChan <- true
			cond.Broadcast()
		}
	}
}

// Interface for delegating copy process to type
type Interface interface {
	DeepCopy() interface{}
}

// Iface is an alias to Copy; this exists for backwards compatibility reasons.
func Iface(iface interface{}) interface{} {
	return Copy(iface)
}

// Copy creates a deep copy of whatever is passed to it and returns the copy
// in an interface{}.  The returned value will need to be asserted to the
// correct type.
func Copy(src interface{}) interface{} {
	if src == nil {
		return nil
	}

	// Make the interface a reflect.Value
	original := reflect.ValueOf(src)

	// Make a copy of the same type as the original.
	cpy := reflect.New(original.Type()).Elem()

	// Recursively copy the original.
	copyRecursive(original, cpy)

	// Return the copy as an interface.
	return cpy.Interface()
}

// copyRecursive does the actual copying of the interface. It currently has
// limited support for what it can handle. Add as needed.
func copyRecursive(original, cpy reflect.Value) {
	// check for implement deepcopy.Interface
	if original.CanInterface() {
		if copier, ok := original.Interface().(Interface); ok {
			cpy.Set(reflect.ValueOf(copier.DeepCopy()))
			return
		}
	}

	// handle according to original's Kind
	switch original.Kind() {
	case reflect.Ptr:
		// Get the actual value being pointed to.
		originalValue := original.Elem()

		// if  it isn't valid, return.
		if !originalValue.IsValid() {
			return
		}
		cpy.Set(reflect.New(originalValue.Type()))
		copyRecursive(originalValue, cpy.Elem())

	case reflect.Interface:
		// If this is a nil, don't do anything
		if original.IsNil() {
			return
		}
		// Get the value for the interface, not the pointer.
		originalValue := original.Elem()

		// Get the value by calling Elem().
		copyValue := reflect.New(originalValue.Type()).Elem()
		copyRecursive(originalValue, copyValue)
		cpy.Set(copyValue)

	case reflect.Struct:
		t, ok := original.Interface().(time.Time)
		if ok {
			cpy.Set(reflect.ValueOf(t))
			return
		}
		// Go through each field of the struct and copy it.
		for i := 0; i < original.NumField(); i++ {
			// The Type's StructField for a given field is checked to see if StructField.PkgPath
			// is set to determine if the field is exported or not because CanSet() returns false
			// for settable fields.  I'm not sure why.  -mohae
			if original.Type().Field(i).PkgPath != "" {
				continue
			}
			copyRecursive(original.Field(i), cpy.Field(i))
		}

	case reflect.Slice:
		if original.IsNil() {
			return
		}
		// Make a new slice and copy each element.
		cpy.Set(reflect.MakeSlice(original.Type(), original.Len(), original.Cap()))
		for i := 0; i < original.Len(); i++ {
			copyRecursive(original.Index(i), cpy.Index(i))
		}

	case reflect.Map:
		if original.IsNil() {
			return
		}
		cpy.Set(reflect.MakeMap(original.Type()))
		for _, key := range original.MapKeys() {
			originalValue := original.MapIndex(key)
			copyValue := reflect.New(originalValue.Type()).Elem()
			copyRecursive(originalValue, copyValue)
			copyKey := Copy(key.Interface())
			cpy.SetMapIndex(reflect.ValueOf(copyKey), copyValue)
		}

	default:
		cpy.Set(original)
	}
}
