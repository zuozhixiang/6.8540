package raft

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.Lock()
	defer rf.Unlock()
	if index > rf.CommitIndex {
		rf.debugf(Snapshot, "warn index > commit index")
		return
	}
	if index <= rf.Logs.Offset {
		rf.debugf(Snapshot, "warn index already in snapshot")
		return
	}
	rf.Logs.Discard(index)
	rf.Logs.SetOffset(index)
	rf.SnapshotData = snapshot
	rf.persist()
	rf.debugf(Snapshot, "index: %v, logs: %v", index, toJson(rf.Logs))
}

type InstallSnapshotRequest struct {
	Term              int32
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int32
}
type InstallSnapshotRespnse struct {
	Term int32
}

func (rf *Raft) InstallSnapshot(req *InstallSnapshotRequest, resp *InstallSnapshotRespnse) {
	if rf.killed() {
		return
	}
	rf.Lock()
	defer rf.Unlock()
	if req.Term < rf.CurrentTerm {
		resp.Term = rf.CurrentTerm
		return
	}
	if req.Term > rf.CurrentTerm {
		rf.VotedFor = NoneVote
		rf.CurrentTerm = req.Term
		rf.TransFollower()

	}
	rf.RestartTimeOutElection()
	rf.LeaderID = req.LeaderID
}
