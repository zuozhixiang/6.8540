package mr

import (
	"encoding/json"
	"fmt"
	"time"
)

const (
	MapTask      = 0
	ReduceTask   = 1
	WaitTask     = 2
	FinishedTask = 3
)

type Task struct {
	ID        int        `json:"id"`
	Type      int        `json:"type"`
	Files     []string   `json:"files"`
	StartTime *time.Time `json:"start_time"`
	Done      bool       `json:"done"`
}

func toJsonString(task *Task) string {
	marshal, err := json.Marshal(task)
	if err != nil {
		return ""
	}
	return string(marshal)
}

var nextIdx = 0

func MakeMapTask(files []string) []*Task {
	res := []*Task{}
	for _, filename := range files {
		res = append(res, &Task{
			ID:    nextIdx,
			Type:  MapTask,
			Files: []string{filename},
		})
		nextIdx++
	}
	return res
}

func MakeReduceTask(x, y int) []*Task {
	res := []*Task{}
	for i := 0; i < y; i++ {
		files := []string{}
		for j := 0; j < x; j++ {
			files = append(files, fmt.Sprintf("mr-%v-%v", j, i))
		}
		res = append(res, &Task{
			ID:    nextIdx,
			Type:  ReduceTask,
			Files: files,
		})
		nextIdx++
	}
	return res
}
