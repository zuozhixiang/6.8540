package raft

import (
	"fmt"
	"testing"
)

func TestLogEntrys_AppendLogEntry(t *testing.T) {
	for i := 0; i < 100; i++ {
		fmt.Println(getID())
	}
}
