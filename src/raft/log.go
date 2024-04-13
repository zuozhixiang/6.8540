package raft

type LogEntrys struct {
	logData []LogEntry
}
type LogEntry struct {
	Command interface{}
	Term    int
}

func (logs *LogEntrys) GetLastIndex() int {
	return len(logs.logData) - 1
}

func (logs *LogEntrys) GetLastTerm() int {
	return logs.logData[logs.GetLastIndex()].Term
}

func (logs *LogEntrys) AppendLogEntry(command interface{}, term int) {
	logs.logData = append(logs.logData, LogEntry{
		Command: command,
		Term:    term,
	})
}

func MakeEmptyLog() *LogEntrys {
	res := &LogEntrys{logData: []LogEntry{{
		Command: nil,
		Term:    -1,
	}}}
	return res
}
