package raft

import "sort"

/*
all operation related with log in this file
*/
type LogEntrys struct {
	LogData []LogEntry
	Offset  int
}
type LogEntry struct {
	Command interface{}
	Term    int32
}

func (logs *LogEntrys) GetLastIndex() int {
	return len(logs.LogData) - 1 + logs.Offset
}

// given index, return logEntry of the index
func (logs *LogEntrys) GetEntry(idx int) LogEntry {
	if idx < 0+logs.Offset || idx > logs.GetLastIndex() {
		logger.Panicf(" illegal index: %v, last: %v, offset: %v", idx, logs.GetLastIndex(), logs.Offset)
		return LogEntry{}
	}
	return logs.LogData[idx-logs.Offset]
}
func (logs *LogEntrys) AppendLogEntrys(data []LogEntry) {
	if len(data) == 0 {
		return
	}
	logs.LogData = append(logs.LogData, data...)
}

// get term of the last log
func (logs *LogEntrys) GetLastTerm() int32 {
	return logs.LogData[logs.GetLastIndex()-logs.Offset].Term
}

// append one logEntry
func (logs *LogEntrys) AppendLogEntry(command interface{}, term int32) {
	logs.LogData = append(logs.LogData, LogEntry{
		Command: command,
		Term:    term,
	})
}

// delete after idx logs (include idx), [idx+1:]
func (logs *LogEntrys) Delete(idx int) {
	if idx > logs.GetLastIndex() {
		return
	}
	if idx < 1+logs.Offset {
		logger.Panicf("delete illegal index, idx: %v", idx)
		return
	}
	// reduce references to a slice, for old slice can be gc
	newValue := logs.LogData[:idx-logs.Offset]
	logs.LogData = make([]LogEntry, len(newValue))
	copy(logs.LogData, newValue)
}

func (logs *LogEntrys) Discard(idx int) {
	// discard logs[1:idx] and retain logs[idx+1:]
	if idx < logs.Offset {
		logger.Errorf("Discard idx is illegal: %v", idx)
		return
	}
	lastIncludedTerm := logs.GetEntry(idx).Term
	newValue := logs.LogData[idx+1-logs.Offset:]
	// this set log of index 0, term to be lastIncludedTerm
	logs.LogData = []LogEntry{{
		Command: nil,
		Term:    lastIncludedTerm,
	}}
	// reduce references to a slice, for old slice can be gc
	logs.AppendLogEntrys(newValue)
}

func (logs *LogEntrys) GetSlice(left, right int) []LogEntry {
	if left < 1+logs.Offset || right > logs.GetLastIndex() || left > right {
		logger.Panicf("illegal index, left: %v, right: %v, last: %v", left, right, logs.GetLastIndex())
		return nil
	}
	return logs.LogData[left-logs.Offset : right+1-logs.Offset]
}

func (logs *LogEntrys) GetTermMaxIndex(term int32) int {
	idx := -1
	n := logs.GetLastIndex() - logs.Offset

	ret := sort.Search(n+1, func(i int) bool {
		return logs.LogData[i].Term >= term+1
	})
	if ret == n+1 {
		return -1
	}
	return ret - 1 + logs.Offset
	for i := n; i >= 1; i-- {
		if logs.LogData[i].Term == term {
			idx = i
			break
		}
	}
	return idx + logs.Offset
}

func (logs *LogEntrys) GetTermMinIndex(term int32) int {
	n := logs.GetLastIndex() - logs.Offset

	idx := sort.Search(n+1, func(i int) bool {
		return logs.LogData[i].Term >= term
	})
	if idx == n+1 {
		return -1
	}
	return idx + logs.Offset
	//for i := 1; i <= n; i++ {
	//	if logs.LogData[i].Term == term {
	//		return i + logs.Offset
	//	}
	//}
	//return n + logs.Offset
}

func (logs *LogEntrys) SetOffset(newV int) {
	logs.Offset = newV
}

func MakeEmptyLog() *LogEntrys {
	res := &LogEntrys{LogData: []LogEntry{{
		Command: nil,
		Term:    -1,
	}}}
	res.Offset = 0
	return res
}
