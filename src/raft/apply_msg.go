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

func (rf *Raft) ApplyMessage(applyCh chan ApplyMsg) {
	for !rf.killed() {
		ms := HeartBeatMinTime + (rand.Int63() % 151)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.Lock()
		for rf.CommitIndex > rf.LastApplied {
			rf.LastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.Logs.GetEntry(rf.LastApplied),
				CommandIndex: rf.LastApplied,
			}
			applyCh <- msg
			rf.debugf(ApplyMess, "msg: %v", toJson(msg))
		}
		rf.Unlock()
	}
}
