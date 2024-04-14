package raft

import (
	"fmt"
	"testing"
)

func TestLogEntrys_AppendLogEntry(t *testing.T) {
	logs1 := MakeEmptyLog()
	logs1.AppendLogEntry("123", 1)
	logs1.AppendLogEntry("123", 2)
	logs1.AppendLogEntry("123", 3)
	logs1.AppendLogEntry("123", 4)
	fmt.Println(toJson(logs1))
	data := []*LogEntry{}
	for i := 0; i < 5; i++ {
		data = append(data, &LogEntry{
			Command: 123,
			Term:    int32(i),
		})
	}
	logs1.Delete(5)
	fmt.Println(toJson(logs1))
	logs1.AppendLogEntrys(data)
	fmt.Println(toJson(logs1))
}
