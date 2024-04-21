package raft

import (
	"math/rand"
	"time"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

func (rf *Raft) apply() {
	rf.Lock()
	defer rf.Unlock()
	needApplyMsg := []ApplyMsg{}
	for rf.CommitIndex > rf.LastApplied {
		rf.LastApplied++
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.Logs.GetEntry(rf.LastApplied).Command,
			CommandIndex: rf.LastApplied,
		}
		needApplyMsg = append(needApplyMsg, msg)
	}
	for _, msg := range needApplyMsg {
		rf.applyChan <- msg
	}
	if len(needApplyMsg) > 0 {
		rf.debugf(ApplyMess, "len: %v, data: %+v", len(needApplyMsg), needApplyMsg)
	}
}

func (rf *Raft) ApplyMessage() {
	for !rf.killed() {
		ms := 30 + (rand.Int63() % 101)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.apply()
	}
}
