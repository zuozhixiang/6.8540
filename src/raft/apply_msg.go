package raft

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

func GetPrintMsg(msgs []ApplyMsg) string {
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

	rf.LastApplied = max(rf.LastApplied, rf.LastIncludedIndex)
	tempLastApplied := rf.LastApplied
	for !(rf.CommitIndex > tempLastApplied || rf.NeedSendMsg != nil) {
		rf.cond.Wait()
		rf.LastApplied = max(rf.LastApplied, rf.LastIncludedIndex)
		tempLastApplied = rf.LastApplied
	}
	needApplyMsg := []ApplyMsg{}
	if rf.NeedSendMsg != nil {
		msg := *rf.NeedSendMsg
		//rf.LastApplied = msg.SnapshotIndex
		// rf.CommitIndex = max(msg.SnapshotIndex, rf.CommitIndex)
		needApplyMsg = append(needApplyMsg, msg)
		rf.NeedSendMsg = nil
	} else {
		for rf.CommitIndex > tempLastApplied {
			tempLastApplied += 1
			msg := ApplyMsg{
				CommandValid: true,
				Command:      rf.Logs.GetEntry(tempLastApplied).Command,
				CommandIndex: tempLastApplied,
			}
			rf.LastApplied = tempLastApplied
			needApplyMsg = append(needApplyMsg, msg)
		}
	}
	if len(needApplyMsg) > 0 {
		rf.debugf(ApplyMess, "commitIndex:%v, lastApplied: %v, len: %v, data: %+v", rf.CommitIndex, rf.LastApplied, len(needApplyMsg), GetPrintMsg(needApplyMsg))
	}
	// me := rf.me
	rf.Unlock()
	// this for , do not exec hold lock, it come to dead lock, beacase, applychan is full, and then can not release lock.
	for _, msg := range needApplyMsg {
		//rf.Lock()
		//if msg.CommandIndex != rf.LastApplied+1 {
		//	rf.Unlock()
		//	continue
		//}
		//rf.Unlock()
		//// logger.Infof("[S%v], send cmd: %v", me, GetPrintMsg([]ApplyMsg{msg}))
		//rf.applyChan <- msg
		//rf.Lock()
		//if msg.CommandIndex != rf.LastApplied+1 {
		//	rf.Unlock()
		//	continue
		//}
		//rf.LastApplied = msg.CommandIndex
		//rf.Unlock()

		//rf.Lock()
		//if msg.CommandIndex != rf.LastApplied+1 {
		//	rf.Unlock()
		//	continue
		//}
		//rf.applyChan <- msg
		//rf.LastApplied = msg.CommandIndex
		//rf.Unlock()
		rf.applyChan <- msg
	}
}

func (rf *Raft) ApplyMessage() {
	for !rf.killed() {
		//ms := 10 + (rand.Int63() % 5)
		//time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.apply()
	}
}
