package main

import (
	"fmt"
	"go.uber.org/zap/zapcore"
	"runtime"
	"time"
)

type Task struct {
	ID        int        `json:"id"`
	Type      int        `json:"type"`
	Files     []string   `json:"files"`
	StartTime *time.Time `json:"start_time"`
	Done      bool       `json:"done"`
}

func (t Task) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	return nil
}

func main() {
	someFunction()
}

func someFunction() {
	printStackTrace()
}

func printStackTrace() {
	pc := make([]uintptr, 10)
	n := runtime.Callers(0, pc)
	frames := runtime.CallersFrames(pc[:n])
	for {
		frame, more := frames.Next()
		fmt.Printf("%s:%d +%#x\n", frame.File, frame.Line, frame.PC)

		if !more {
			break
		}
	}
}
