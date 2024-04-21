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
	CommandValid bool        `json:"CommandValid,omitempty"`
	Command      interface{} `json:"Command,omitempty"`
	CommandIndex int         `json:"CommandIndex,omitempty"`

	// For 3D:
	SnapshotValid bool   `json:"SnapshotValid,omitempty"`
	Snapshot      []byte `json:"Snapshot,omitempty"`
	SnapshotTerm  int    `json:"SnapshotTerm,omitempty"`
	SnapshotIndex int    `json:"SnapshotIndex,omitempty"`
}

func getPrintMsg(msgs []ApplyMsg) string {
	res := []ApplyMsg{}
	for _, msg := range msgs {
		res = append(res, ApplyMsg{
			CommandValid:  msg.CommandValid,
			Command:       msg.Command,
			CommandIndex:  msg.CommandIndex,
			SnapshotValid: msg.SnapshotValid,
			SnapshotTerm:  msg.SnapshotTerm,
			SnapshotIndex: msg.SnapshotIndex,
		})
	}
	return toJson(res)
}

func (rf *Raft) apply() {
	rf.Lock()
	needApplyMsg := []ApplyMsg{}
	rf.LastApplied = max(rf.LastApplied, rf.LastIncludedIndex)
	if rf.NeedApplySnapshot != nil {
		msg := ApplyMsg{
			SnapshotValid: true,
			Snapshot:      rf.SnapshotData,
			SnapshotTerm:  int(rf.NeedApplyIncludedTerm),
			SnapshotIndex: rf.NeedApplyInlucdedIndex,
		}
		needApplyMsg = append(needApplyMsg, msg)
		rf.NeedApplySnapshot = nil
	} else {
		for rf.CommitIndex > rf.LastApplied {
			rf.LastApplied++
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.Logs.GetEntry(rf.LastApplied).Command,
				CommandIndex: rf.LastApplied,
			}
			needApplyMsg = append(needApplyMsg, msg)
		}
	}
	if len(needApplyMsg) > 0 {
		rf.debugf(ApplyMess, "commitIndex:%v, lastApplied: %v, len: %v, data: %+v", rf.CommitIndex, rf.LastApplied, len(needApplyMsg), getPrintMsg(needApplyMsg))
	}
	rf.Unlock()
	// this for , do not exec hold lock, it come to dead lock, beacase, applychan is full, and then can not release lock.
	for _, msg := range needApplyMsg {
		rf.applyChan <- msg
	}
}

func (rf *Raft) ApplyMessage() {
	for !rf.killed() {
		ms := 50 + (rand.Int63() % 51)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.apply()
	}
}
