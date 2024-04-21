package raft

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

func (logs *LogEntrys) GetEntry(idx int) LogEntry {
	if idx < 0+logs.Offset || idx > logs.GetLastIndex() {
		logger.Panicf(" 非法idx: %v, last: %v, offset: %v", idx, logs.GetLastIndex(), logs.Offset)
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

func (logs *LogEntrys) GetLastTerm() int32 {
	return logs.LogData[logs.GetLastIndex()-logs.Offset].Term
}

func (logs *LogEntrys) AppendLogEntry(command interface{}, term int32) {
	logs.LogData = append(logs.LogData, LogEntry{
		Command: command,
		Term:    term,
	})
}

func (logs *LogEntrys) Delete(idx int) {
	if idx > logs.GetLastIndex() {
		return
	}
	if idx < 1+logs.Offset {
		logger.Panicf("delete 非法, idx: %v", idx)
		return
	}
	newValue := logs.LogData[:idx-logs.Offset]
	logs.LogData = make([]LogEntry, len(newValue))
	copy(logs.LogData, newValue)
}

func (logs *LogEntrys) Discard(idx int) {
	// 删除idx以及之前的， 也就是从[1:idx+1],
	//也就是留下[idx+1]后面的
	if idx < logs.Offset {
		logger.Errorf("Discard idx is illegal: %v", idx)
		return
	}
	lastIncludedTerm := logs.GetEntry(idx).Term
	newValue := logs.LogData[idx+1-logs.Offset:]
	logs.LogData = []LogEntry{{
		Command: nil,
		Term:    lastIncludedTerm,
	}}
	logs.AppendLogEntrys(newValue)
}

func (logs *LogEntrys) GetSlice(left, right int) []LogEntry {
	if left < 1+logs.Offset || right > logs.GetLastIndex() || left > right {
		logger.Panicf("非法范围, left: %v, right: %v, last: %v", left, right, logs.GetLastIndex())
		return nil
	}
	return logs.LogData[left-logs.Offset : right+1-logs.Offset]
}

func (logs *LogEntrys) GetTermMaxIndex(term int32) int {
	idx := -1
	n := logs.GetLastIndex() - logs.Offset
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
	for i := 1; i <= n; i++ {
		if logs.LogData[i].Term == term {
			return i + logs.Offset
		}
	}
	return n + logs.Offset
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
