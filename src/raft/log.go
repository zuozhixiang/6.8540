package raft

type LogEntrys struct {
	LogData []*LogEntry
}
type LogEntry struct {
	Command interface{}
	Term    int32
}

func (logs *LogEntrys) GetLastIndex() int {
	return len(logs.LogData) - 1
}

func (logs *LogEntrys) GetEntry(idx int) *LogEntry {
	if idx < 0 || idx > logs.GetLastIndex() {
		logger.Errorf("非法idx: %v, last: %v", idx, logs.GetLastIndex())
		return nil
	}
	return logs.LogData[idx]
}
func (logs *LogEntrys) AppendLogEntrys(data []*LogEntry) {
	logs.LogData = append(logs.LogData, data...)
}

func (logs *LogEntrys) GetLastTerm() int32 {
	return logs.LogData[logs.GetLastIndex()].Term
}

func (logs *LogEntrys) AppendLogEntry(command interface{}, term int32) {
	logs.LogData = append(logs.LogData, &LogEntry{
		Command: command,
		Term:    term,
	})
}

func (logs *LogEntrys) Delete(idx int) {
	if idx > logs.GetLastIndex() {
		return
	}
	if idx <= 1 {
		logger.Errorf("delete 非法, idx: %v", idx)
		return
	}
	logs.LogData = logs.LogData[:idx]
}

func (logs *LogEntrys) GetSlice(left, right int) []*LogEntry {
	if left < 1 || right > logs.GetLastIndex() || left > right {
		logger.Errorf("非法范围, left: %v, right: %v, last: %v", left, right, logs.GetLastIndex())
		return nil
	}
	return logs.LogData[left : right+1]
}

func MakeEmptyLog() *LogEntrys {
	res := &LogEntrys{LogData: []*LogEntry{{
		Command: nil,
		Term:    -1,
	}}}
	return res
}
