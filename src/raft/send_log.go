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
	resp.Success = false
	curTerm := rf.getTerm()
	resp.Term = curTerm
	if rf.killed() {
		return
	}
	if curTerm > req.Term {
		rf.debugf(ReciveData, "Leader[S%v]-> fail, leader term is old req: %v, resp: %v",
			req.LeaderID, toJson(req), toJson(resp))
		resp.Status = OutDateTerm
		return
	}
	if rf.isCandidate() {
		rf.VotedFor = NoneVote
		rf.TransFollower()
	}
	rf.Lock()
	defer rf.Unlock()
	// todo, look paper
	if req.PrevLogIndex > rf.Logs.GetLastIndex() || rf.Logs.GetEntry(req.PrevLogIndex).Term != req.PrevLogTerm {
		if req.PrevLogIndex > rf.Logs.GetLastIndex() {
			rf.debugf(ReciveData, "Leader[S%v]-> fail, PrevIndex: %v, lastIndex: %v, state: %v", req.LeaderID, req.PrevLogIndex,
				rf.Logs.GetLastTerm(), toJson(rf))
			resp.Status = NoMatch
			resp.ConflictingTerm = -1
			resp.FirstConflictingIndex = len(rf.Logs.LogData)
		} else {
			rf.debugf(ReciveData, "Leader[S%v]-> fail, not match PrevIndex: %v, %v!=%v, state: %v", req.LeaderID,
				req.PrevLogIndex, rf.Logs.GetEntry(req.PrevLogIndex).Term, req.PrevLogTerm, toJson(rf))
			resp.Status = NoMatch
			resp.ConflictingTerm = rf.Logs.GetEntry(req.PrevLogIndex).Term
			ret := req.PrevLogIndex - 1
			for rf.Logs.GetEntry(ret).Term == resp.ConflictingTerm {
				ret--
			}
			resp.FirstConflictingIndex = ret + 1
		}
	} else {
		resp.Success = true
		rf.debugf(ReciveData, "Leader[S%v]-> success, req: %v, state: %v", req.LeaderID, toJson(req), toJson(rf))
		lastIndex := rf.Logs.GetLastIndex()
		for i, entry := range req.Entries {
			x := i + 1 + req.PrevLogIndex
			if x <= lastIndex && rf.Logs.GetEntry(x).Term != entry.Term || x > lastIndex {
				// 这一条以及之后的都截断
				rf.Logs.Delete(i + 1 + req.PrevLogIndex)
				rf.Logs.AppendLogEntrys(req.Entries[i:])
				break
			}
		}
		rf.VotedFor = NoneVote
		rf.TransFollower()
		rf.CommitIndex = min(req.LeaderCommit, req.PrevLogIndex+len(req.Entries))
	}
}

func (rf *Raft) SendLogData(server int, req *AppendEntriesRequest, resp *AppendEntriesResponse, nextIndex int) {
	for !rf.killed() {
		ok := rf.peers[server].Call("Raft.AppendEntries", req, resp)
		if rf.killed() || !rf.isLeader() || rf.getTerm() != req.Term {
			return
		}
		if ok {
			break
		}
	}
	rf.Lock()
	defer rf.Unlock()
	if resp.Success {
		rf.NextIndex[server] = max(rf.NextIndex[server], nextIndex)
		rf.MatchIndex[server] = max(rf.MatchIndex[server], nextIndex-1)
		rf.TryUpdateCommitID()
		rf.debugf(SendData, "success, ->[S%v], req: %v, resp: %v, state: %v",
			server, toJson(req), toJson(resp), toJson(rf))
	} else {
		if resp.Status == OutDateTerm {
			rf.debugf(SendData, "fail to Follower  ->[S%v], req: %v, resp: %v", server, toJson(req), toJson(resp))
			rf.VotedFor = NoneVote
			rf.TransFollower()
			rf.setTerm(resp.Term)
		} else if resp.Status == NoMatch {
			// No match
			rf.debugf(SendData, "fail not match ->[S%v], notmatchIndex: %v, Term: %v", server, req.PrevLogIndex, req.PrevLogTerm)
			if resp.ConflictingTerm == -1 {
				rf.NextIndex[server] = resp.FirstConflictingIndex
			} else {
				last := rf.Logs.GetTermMaxIndex(resp.ConflictingTerm)
				if last != -1 {
					rf.NextIndex[server] = min(resp.FirstConflictingIndex, last)
				} else {
					if resp.FirstConflictingIndex > rf.NextIndex[server] {
						logger.Error(resp.FirstConflictingIndex, rf.NextIndex[server])
					}
					rf.NextIndex[server] = resp.FirstConflictingIndex
				}
			}
			rf.MatchIndex[server] = rf.NextIndex[server] - 1
			req.PrevLogIndex = rf.MatchIndex[server]
			req.PrevLogTerm = rf.Logs.GetEntry(req.PrevLogIndex).Term
			nextIndex = rf.Logs.GetLastIndex() + 1
			req.Entries = rf.Logs.GetSlice(rf.NextIndex[server], nextIndex-1)
			go rf.SendLogData(server, req, resp, nextIndex)
		}
	}
}

func (rf *Raft) TryUpdateCommitID() {
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
			req := AppendEntriesRequest{
				ID:           getID(),
				Term:         rf.CurrentTerm,
				LeaderID:     rf.me,
				Entries:      nil,
				LeaderCommit: rf.CommitIndex,
				PrevLogIndex: rf.MatchIndex[i], // todo
			}
			req.PrevLogTerm = rf.Logs.GetEntry(req.PrevLogIndex).Term // todo
			resp := AppendEntriesResponse{}
			nextIdx := rf.Logs.GetLastIndex() + 1
			if rf.NextIndex[i] <= rf.Logs.GetLastIndex() {
				req.Entries = rf.Logs.GetSlice(rf.NextIndex[i], rf.Logs.GetLastIndex())
			}
			go rf.SendLogData(i, &req, &resp, nextIdx)
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
		ms := HeartBeatMinTime + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
