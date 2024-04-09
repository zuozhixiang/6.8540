package main

import (
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
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
	task := &Task{ID: 123}

	fmt.Println(*task)
	logger, _ := zap.NewDevelopment()
	slogger := logger.Sugar()
	slogger.Infof("%+v", task)
	getwd, err := os.Getwd()
	if err != nil {
		return
	}

	temp, err := os.CreateTemp(getwd, "1.txt")
	if err != nil {
		return
	}
	os.Rename(temp.Name(), getwd+"/1.log")
	defer temp.Close()
}
