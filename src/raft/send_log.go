package raft

import (
	"math/rand"
	"time"
)

const (
	HeartBeatMinTime = 80
)

type AppendEntriesRequest struct {
	ID           int64
	Term         int32
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int32
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesResponse struct {
	Term                  int32
	Success               bool
	Status                Status
	ConflictingTerm       int32
	FirstConflictingIndex int
}

func (rf *Raft) AppendEntries(req *AppendEntriesRequest, resp *AppendEntriesResponse) {
	m := ReciveData
	if len(req.Entries) == 0 {
		m = ReciveHeart
	}
	if rf.killed() {
		return
	}
	rf.Lock()
	defer rf.Unlock()
	resp.Success = false
	curTerm := rf.CurrentTerm
	if curTerm > req.Term {
		rf.debugf(m, "Leader[S%v]-> fail, leader term is old req: %v, resp: %v",
			req.LeaderID, toJson(req), toJson(resp))
		resp.Term = curTerm
		resp.Status = OutDateTerm
		return
	}
	needPersist := false
	if rf.State == Candidate {
		rf.VotedFor = NoneVote
		rf.TransFollower()
		needPersist = true
	}
	if req.Term > rf.CurrentTerm {
		rf.VotedFor = NoneVote
		rf.CurrentTerm = req.Term
		rf.TransFollower()
		rf.LeaderID = -1
		needPersist = true
	}
	rf.RestartTimeOutElection()
	rf.LeaderID = req.LeaderID
	if req.PrevLogIndex < rf.LastIncludedIndex {
		//log already be compact into snapshot, only append after rf.lastIncludedIndex‘s log
		alreadyInSnapShotLen := rf.LastIncludedIndex - req.PrevLogIndex
		if alreadyInSnapShotLen <= len(req.Entries) {
			newReq := &AppendEntriesRequest{
				ID:           req.ID,
				Term:         req.Term,
				LeaderID:     req.LeaderID,
				PrevLogIndex: rf.LastIncludedIndex,
				PrevLogTerm:  rf.LastIncludedTerm,
				Entries:      req.Entries[alreadyInSnapShotLen:],
				LeaderCommit: req.LeaderCommit,
			}
			req = newReq
			rf.debugf(m, "Leader[S%v]->[S%v], partial logs in snapshot, preLogIndex: %v, lastIncludeIndex:%v, Entrys: %v", rf.LeaderID,
				rf.me, req.PrevLogIndex, rf.LastIncludedIndex, req.Entries)
		} else {
			// entries all in snapshot,
			rf.debugf(m, "Leader[S%v]->[S%v], all logs in snapshot, preLogIndex: %v, lastIncludeIndex:%v, Entrys: %v", rf.LeaderID,
				rf.me, req.PrevLogIndex, rf.LastIncludedIndex, req.Entries)
			resp.Success = true
			return
		}
	}

	if req.PrevLogIndex > rf.Logs.GetLastIndex() || rf.Logs.GetEntry(req.PrevLogIndex).Term != req.PrevLogTerm {
		if req.PrevLogIndex > rf.Logs.GetLastIndex() {
			rf.debugf(m, "Leader[S%v]-> fail, PrevIndex: %v, lastIndex: %v, state: %v", req.LeaderID, req.PrevLogIndex,
				rf.Logs.GetLastTerm(), toJson(rf))
			resp.Status = NoMatch
			resp.ConflictingTerm = -1
			resp.FirstConflictingIndex = rf.Logs.GetLastIndex() + 1
		} else {
			rf.debugf(m, "Leader[S%v]-> fail, not match PrevIndex: %v, %v!=%v, state: %v", req.LeaderID,
				req.PrevLogIndex, rf.Logs.GetEntry(req.PrevLogIndex).Term, req.PrevLogTerm, toJson(rf))
			resp.Status = NoMatch
			resp.ConflictingTerm = rf.Logs.GetEntry(req.PrevLogIndex).Term
			resp.FirstConflictingIndex = rf.Logs.GetTermMinIndex(resp.ConflictingTerm)
		}
	} else {
		resp.Success = true
		rf.debugf(m, "Leader[S%v]-> success, req: %v, state: %v", req.LeaderID, toJson(req), toJson(rf))
		lastIndex := rf.Logs.GetLastIndex()
		for i, entry := range req.Entries {
			x := i + 1 + req.PrevLogIndex
			if (x <= lastIndex && rf.Logs.GetEntry(x).Term != entry.Term) || (x > lastIndex) {
				// 这一条以及之后的都截断
				rf.Logs.Delete(x)
				rf.Logs.AppendLogEntrys(req.Entries[i:])
				break
			} else if x > lastIndex {
				rf.Logs.AppendLogEntrys(req.Entries[i:])
				break
			}
		}
		rf.TransFollower() // todo
		if req.LeaderCommit > rf.CommitIndex {
			rf.CommitIndex = min(req.LeaderCommit, req.PrevLogIndex+len(req.Entries))
		}
		needPersist = true
	}
	if needPersist {
		rf.persist()
	}
}

func (rf *Raft) SendLogData(server int, req *AppendEntriesRequest, resp *AppendEntriesResponse, nextIndex int) {
	for !rf.killed() {
		ok := rf.peers[server].Call("Raft.AppendEntries", req, resp)
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
	m := SendData
	if len(req.Entries) == 0 {
		m = SendHeart
	}
	if resp.Success {
		rf.debugf(m, "success, ->[S%v], req: %v, resp: %v, state: %v",
			server, toJson(req), toJson(resp), toJson(rf))
		rf.NextIndex[server] = max(rf.NextIndex[server], nextIndex)
		rf.MatchIndex[server] = max(rf.MatchIndex[server], nextIndex-1)
		rf.TryUpdateCommitIndex()
	} else {
		if resp.Status == OutDateTerm {
			rf.debugf(m, "fail to Follower  ->[S%v], req: %v, resp: %v", server, toJson(req), toJson(resp))
			if resp.Term > rf.CurrentTerm {
				rf.CurrentTerm = max(rf.CurrentTerm, resp.Term)
				rf.VotedFor = NoneVote
				rf.TransFollower()
				rf.LeaderID = -1
				rf.RestartTimeOutElection()
				rf.persist()
			}
		} else if resp.Status == NoMatch {
			// No match
			rf.debugf(m, "fail not match ->[S%v], notmatchIndex: %v, Term: %v", server, req.PrevLogIndex, req.PrevLogTerm)
			if resp.ConflictingTerm == -1 {
				if rf.NextIndex[server] < resp.FirstConflictingIndex {
					logger.Errorf("zzx123")
				}
				rf.NextIndex[server] = resp.FirstConflictingIndex
			} else {
				last := rf.Logs.GetTermMaxIndex(resp.ConflictingTerm)
				if last != -1 {
					if resp.FirstConflictingIndex < last {
						logger.Errorf("FirstConflictingIndex: %v, last: %v", resp.FirstConflictingIndex, last)
					}
					rf.NextIndex[server] = min(resp.FirstConflictingIndex, last)
				} else {
					if resp.FirstConflictingIndex > rf.NextIndex[server] {
						logger.Error(resp.FirstConflictingIndex, rf.NextIndex[server])
					}
					rf.NextIndex[server] = min(resp.FirstConflictingIndex, rf.NextIndex[server])
				}
			}
			rf.MatchIndex[server] = rf.NextIndex[server] - 1
			if rf.NextIndex[server] <= rf.LastIncludedIndex {
				// need to send snapshot
				snapReq := &InstallSnapshotRequest{
					ID:                getID(),
					Term:              rf.CurrentTerm,
					LeaderID:          rf.me,
					LastIncludedIndex: rf.LastIncludedIndex,
					LastIncludedTerm:  rf.LastIncludedTerm,
					Data:              rf.SnapshotData,
				}
				snapResp := &InstallSnapshotRespnse{}
				go rf.SendSnapshot(server, snapReq, snapResp)
			} else {
				req.PrevLogIndex = rf.MatchIndex[server]
				req.PrevLogTerm = rf.Logs.GetEntry(req.PrevLogIndex).Term
				nextIndex = rf.Logs.GetLastIndex() + 1
				req.Entries = rf.Logs.GetSlice(rf.NextIndex[server], nextIndex-1)
				logger.Infof("retry, id: %v, conflictedIndex: %v, term:%v, nextIndex: %v", req.ID, resp.FirstConflictingIndex,
					resp.ConflictingTerm, rf.NextIndex[server])
				go rf.SendLogData(server, req, resp, nextIndex)
			}
			return
		}
	}
}

func (rf *Raft) TryUpdateCommitIndex() {
	n := rf.Logs.GetLastIndex()
	old_commitIndex := rf.CommitIndex
	for N := n; N > old_commitIndex && rf.Logs.GetEntry(N).Term == rf.CurrentTerm; N-- {
		cnt := 1
		for i, x := range rf.MatchIndex {
			if i == rf.me {
				continue
			}
			if x >= N {
				cnt++
			}
		}
		if cnt > rf.n/2 {
			rf.CommitIndex = N
			break
		}
	}
	if rf.CommitIndex > old_commitIndex {
		rf.debugf(UpdateCommitIndex, "old idx [%v]->[%v]", old_commitIndex, rf.CommitIndex)
	}
}

func (rf *Raft) SendAllHeartBeat() {
	for i, _ := range rf.peers {
		if i != rf.me {
			lastIndex := rf.Logs.GetLastIndex()
			if rf.NextIndex[i] <= rf.LastIncludedIndex {
				// prevLogIndex already compact into snapshot,
				// log be compact into snapshot, not find term.
				// so, need to send snapshot
				snaptReq := &InstallSnapshotRequest{
					ID:                getID(),
					Term:              rf.CurrentTerm,
					LeaderID:          rf.me,
					LastIncludedIndex: rf.LastIncludedIndex,
					LastIncludedTerm:  rf.LastIncludedTerm,
					Data:              rf.SnapshotData,
				}
				snapResp := &InstallSnapshotRespnse{}
				go rf.SendSnapshot(i, snaptReq, snapResp)
			} else {
				nextIndex := rf.NextIndex[i]
				req := AppendEntriesRequest{
					ID:           getID(),
					Term:         rf.CurrentTerm,
					LeaderID:     rf.me,
					Entries:      nil,
					LeaderCommit: rf.CommitIndex,
					PrevLogIndex: nextIndex - 1, // todo
				}
				req.PrevLogTerm = rf.Logs.GetEntry(req.PrevLogIndex).Term // todo
				resp := AppendEntriesResponse{}
				nextIdx := rf.Logs.GetLastIndex() + 1
				if rf.NextIndex[i] <= lastIndex {
					// send log data
					req.Entries = rf.Logs.GetSlice(rf.NextIndex[i], lastIndex)
				}
				go rf.SendLogData(i, &req, &resp, nextIdx)
			}
		}
	}
}

func (rf *Raft) sendHeartBeat() {
	for !rf.killed() {
		rf.Lock()
		if rf.State == Leader {
			rf.SendAllHeartBeat()
		}
		rf.Unlock()
		ms := HeartBeatMinTime + (rand.Int63() % 50)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
