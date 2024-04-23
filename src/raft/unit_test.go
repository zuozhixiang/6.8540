package raft

import (
	"6.5840/labgob"
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLogEntrys_AppendLogEntry(t *testing.T) {
	logs1 := MakeEmptyLog()
	logs1.AppendLogEntry("123", 1)
	logs1.AppendLogEntry("123", 1)
	logs1.AppendLogEntry("123", 1)
	logs1.AppendLogEntry("123", 1)
	logs1.AppendLogEntry("123", 3)
}

func TestGobLog(t *testing.T) {
	logs := MakeEmptyLog()
	logs.AppendLogEntry("xxx", 1)
	logs.AppendLogEntry("xxx", 1)
	logs.AppendLogEntry("xxx", 1)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(logs)
	state := w.Bytes()
	fmt.Println(len(state), state)

	r := bytes.NewBuffer(state)
	d := labgob.NewDecoder(r)

	var logs1 *LogEntrys
	err := d.Decode(&logs1)
	if err != nil {
		return
	}
	fmt.Println(logs1)
}

func TestGetMinORMax(t *testing.T) {
	logs1 := MakeEmptyLog()
	logs1.AppendLogEntry("123", 1)
	logs1.AppendLogEntry("123", 1)
	logs1.AppendLogEntry("123", 1)
	logs1.AppendLogEntry("123", 1)
	logs1.AppendLogEntry("123", 3)
	idx := logs1.GetTermMinIndex(3)
	assert.Equal(t, 5, idx)
	idx = logs1.GetTermMinIndex(1)
	assert.Equal(t, 1, idx)

	idx = logs1.GetTermMaxIndex(1)
	assert.Equal(t, 4, idx)
}
