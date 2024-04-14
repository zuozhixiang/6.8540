package raft

import (
	"fmt"
	"testing"
)

func TestLogEntrys_AppendLogEntry(t *testing.T) {
	logs1 := MakeEmptyLog()
	logs1.AppendLogEntry("123", 1)
	logs1.AppendLogEntry("123", 1)
	logs1.AppendLogEntry("123", 1)
	logs1.AppendLogEntry("123", 1)
	logs1.AppendLogEntry("123", 3)
	x := logs1.GetTermMaxIndex(4)
	fmt.Println(x)
}
