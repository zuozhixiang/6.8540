package raft

import (
	"6.5840/labgob"
	"bytes"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	e.Encode(rf.LastIncludedIndex)
	e.Encode(rf.LastIncludedTerm)
	// e.Encode(rf.yyy)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.SnapshotData)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int32
	var votedFor int
	var lastIncludedIndex int
	var lastIncludedTerm int32
	var logs *LogEntrys
	if err := d.Decode(&term); err != nil {
		logger.Panicf("decode fail: %v", err)
	}
	if err := d.Decode(&votedFor); err != nil {
		logger.Panicf("decode fail: %v", err)
	}
	if err := d.Decode(&logs); err != nil {
		logger.Panicf("decode fail: %v", err)
	}
	if err := d.Decode(&lastIncludedIndex); err != nil {
		logger.Panicf("decode fail: %v", err)
	}
	if err := d.Decode(&lastIncludedTerm); err != nil {
		logger.Panicf("decode fail: %v", err)
	}
	rf.CurrentTerm = term
	rf.VotedFor = votedFor
	rf.Logs = logs
	rf.LastIncludedTerm = lastIncludedTerm
	rf.LastIncludedIndex = lastIncludedIndex
}
func (rf *Raft) readSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	rf.SnapshotData = data
}
