package raft

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.

func (rf *Raft) saveSnapshot(index int, snapshot []byte) {
	if rf.LastIncludedIndex >= index {
		rf.debugf(Snapshot, "warn index already in snapshot")
		return
	}
	if rf.CommitIndex < index {
		rf.debugf(Snapshot, "warn index > commit index")
		return
	}

	rf.LastIncludedIndex = index
	rf.LastIncludedTerm = rf.Logs.GetEntry(index).Term
	rf.Logs.Discard(index)
	rf.Logs.SetOffset(index)
	rf.SnapshotData = snapshot
	if rf.LastApplied < index {
		rf.LastApplied = index
	}
	rf.persist()
	rf.debugf(Snapshot, "index: %v, logs: %v", index, toJson(rf.Logs))
}
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.Lock()
	defer rf.Unlock()
	rf.saveSnapshot(index, snapshot)
	return
}

type InstallSnapshotRequest struct {
	ID                int64
	Term              int32
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int32
	Data              []byte
}

type InstallSnapshotRespnse struct {
	Term int32
}

func getJsonReq(req *InstallSnapshotRequest) string {
	newReq := InstallSnapshotRequest{
		Term:              req.Term,
		LeaderID:          req.LeaderID,
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
	}
	return toJson(newReq)
}

func (rf *Raft) InstallSnapshot(req *InstallSnapshotRequest, resp *InstallSnapshotRespnse) {
	if rf.killed() {
		return
	}
	rf.Lock()
	defer rf.Unlock()
	if req.Term < rf.CurrentTerm {
		rf.debugf(ReciveSnap, "outdate term, leadId:[S%v], Term: %v, curTerm: %v", req.LeaderID, req.Term, rf.CurrentTerm)
		resp.Term = rf.CurrentTerm
		return
	}
	if req.Term > rf.CurrentTerm {
		rf.VotedFor = NoneVote
		rf.CurrentTerm = req.Term
		rf.TransFollower()
	}
	rf.RestartTimeOutElection()
	if req.LastIncludedIndex <= rf.LastIncludedIndex {
		rf.debugf(ReciveSnap, "Leader[S%v] snapshot already in, applyIncludedIndex:%v, req: %v", req.LeaderID,
			rf.LastIncludedIndex, getJsonReq(req))
		return
	}
	lastLogIndex := rf.Logs.GetLastIndex()
	if req.LastIncludedIndex >= lastLogIndex {
		rf.Logs.LogData = []LogEntry{{Term: req.LastIncludedTerm}}
	} else {
		// retain [req.lastInclude + 1:]
		newLog := []LogEntry{{Term: req.LastIncludedTerm}}
		newLog = append(newLog, rf.Logs.GetSlice(req.LastIncludedIndex+1, lastLogIndex)...)
		rf.Logs.LogData = newLog
	}
	rf.Logs.SetOffset(req.LastIncludedIndex)
	rf.SnapshotData = req.Data
	rf.LastIncludedTerm = req.LastIncludedTerm
	rf.LastIncludedIndex = req.LastIncludedIndex
	rf.LeaderID = req.LeaderID
	rf.persist()
	rf.debugf(ReciveSnap, "Leader[S%v] snapshot success, req: %v", req.LeaderID, getJsonReq(req))
	if rf.LastApplied < req.LastIncludedIndex {
		rf.LastApplied = req.LastIncludedIndex
	}
	if rf.CommitIndex < req.LastIncludedIndex {
		rf.CommitIndex = req.LastIncludedIndex
	}
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      req.Data,
		SnapshotTerm:  int(req.LastIncludedTerm),
		SnapshotIndex: req.LastIncludedIndex,
	}
	rf.applyChan <- msg
}

func (rf *Raft) SendSnapshot(server int, req *InstallSnapshotRequest, resp *InstallSnapshotRespnse) {
	if rf.killed() {
		return
	}
	for !rf.killed() {
		ok := rf.peers[server].Call("Raft.InstallSnapshot", req, resp)
		rf.Lock()
		if rf.killed() || rf.State != Leader || rf.CurrentTerm != req.Term {
			rf.Unlock()
			return
		}
		rf.Unlock()
		if ok {
			break
		}
	}

	rf.Lock()
	defer rf.Unlock()
	if resp.Term > rf.CurrentTerm {
		rf.TransFollower()
		rf.RestartTimeOutElection()
		rf.VotedFor = NoneVote
		rf.LeaderID = -1
		rf.CurrentTerm = resp.Term
		rf.debugf(SendSnap, "->S[%v] fail outdate term, req: %v, resp: %v", server, getJsonReq(req), toJson(resp))
	} else {
		// success
		rf.MatchIndex[server] = max(req.LastIncludedIndex, rf.MatchIndex[server])
		rf.NextIndex[server] = max(req.LastIncludedIndex+1, rf.NextIndex[server])
		rf.debugf(SendSnap, "->S[%v] success req: %v, resp: %v", server, getJsonReq(req), toJson(resp))
		// rf.TryUpdateCommitIndex()
	}
}
