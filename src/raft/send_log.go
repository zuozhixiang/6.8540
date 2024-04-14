package raft

import (
	"math/rand"
	"time"
)

const (
	HeartBeatMinTime = 150
)

type AppendEntriesRequest struct {
	ID           int64
	Term         int32
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int32
	Entries      []*LogEntry
	LeaderCommit int
}
type AppendEntriesResponse struct {
	Term    int32
	Success bool
}

func (rf *Raft) AppendEntries(req *AppendEntriesRequest, resp *AppendEntriesResponse) {
	if rf.killed() {
		return
	}
	rf.Lock()
	defer rf.Unlock()
	resp.Success = false
	resp.Term = rf.CurrentTerm
	if !rf.CheckTermAndUpdate(req.Term) {
		rf.debugf(ReciveData, "Leader[S%v]-> fail, leader term is old req: %v, resp: %v",
			req.LeaderID, toJson(req), toJson(resp))
		return
	}
	rf.VotedFor = NoneVote
	rf.TransFollower()
	if len(req.Entries) == 0 {
		// heartbeat
		resp.Success = true
		rf.debugf(ReciveHeart, "Leader[S%v]-> success req: %v, resp: %v", req.LeaderID, toJson(req), toJson(resp))
		return
	}
	// todo, look paper
	if req.PrevLogIndex > rf.Logs.GetLastIndex() || rf.Logs.GetEntry(req.PrevLogIndex).Term != req.PrevLogTerm {
		if req.PrevLogIndex > rf.Logs.GetLastIndex() {
			rf.debugf(ReciveData, "Leader[S%v]-> fail, PrevIndex: %v, lastIndex: %v", req.LeaderID, rf.Logs.GetLastTerm())
		} else {
			rf.debugf(ReciveData, "Leader[S%v]-> fail, not match PrevIndex: %v, %v!=%v ", req.LeaderID,
				req.PrevLogIndex, rf.Logs.GetEntry(req.PrevLogIndex).Term, req.PrevLogTerm)
		}
	} else {
		resp.Success = true
		rf.debugf(ReciveData, "Leader[S%v]-> success, req: %v", req.LeaderID, toJson(req))
		//
		rf.Logs.Delete(req.PrevLogIndex + 1)
		rf.Logs.AppendLogEntrys(req.Entries)
		if req.LeaderCommit > rf.CommitIndex {
			rf.CommitIndex = min(req.LeaderCommit, rf.Logs.GetLastIndex())
		}
	}
}

func (rf *Raft) SendLogData(server int, req *AppendEntriesRequest, resp *AppendEntriesResponse, nextIndex int) {
	req.ID = getID()
	for !rf.killed() {
		rf.debugf(SendData, "->[S%v] , req: %v", server, toJson(req))
		ok := rf.peers[server].Call("Raft.AppendEntries", req, resp)
		if rf.killed() {
			rf.infof("be killed")
			break
		}
		if rf.GetTerm() != req.Term {
			rf.infof("outdated message: %v", req.ID)
			return
		}
		if !ok {
			rf.debugf(SendData, "fail, ->[S%v], req: %v", server, toJson(req))
		} else {
			rf.Lock()
			if resp.Success {
				rf.debugf(SendData, "success, ->[S%v], req: %v, resp: %v", server, toJson(req), toJson(resp))
				rf.NextIndex[server] = nextIndex
				rf.MatchIndex[server] = nextIndex - 1
				// 尝试更新commitId
				rf.TryUpdateCommitID()
				rf.Unlock()
				return
			} else {
				rf.debugf(SendData, "not match ->[S%v], notmatchIndex: %v, Term: %v", server, req.PrevLogIndex, req.PrevLogTerm)
				rf.NextIndex[server] -= 1
				rf.MatchIndex[server] = rf.NextIndex[server] - 1
				req.PrevLogIndex = rf.MatchIndex[server]
				req.PrevLogTerm = rf.Logs.GetEntry(req.PrevLogIndex).Term
				req.Entries = rf.Logs.GetSlice(rf.NextIndex[server], rf.Logs.GetLastIndex())
				rf.Unlock()
			}
		}
	}
}

func (rf *Raft) TryUpdateCommitID() {
	n := rf.Logs.GetLastIndex()
	old_commitIndex := rf.CommitIndex
	for N := old_commitIndex + 1; N <= n; N++ {
		n := rf.n
		cnt := 0
		for _, x := range rf.MatchIndex {
			if x >= N && rf.Logs.GetEntry(N).Term == rf.CurrentTerm {
				cnt++
			}
		}
		if cnt > n/2 {
			rf.CommitIndex = N
		} else {
			break
		}
	}
	if rf.CommitIndex > old_commitIndex {
		rf.debugf(UpdateCommitIndex, "old idx [%v]->[%v]", old_commitIndex, rf.CommitIndex)
	}
}

func (rf *Raft) SendHeartBeat(server int, req *AppendEntriesRequest, resp *AppendEntriesResponse) {
	req.ID = getID()
	for !rf.killed() {
		rf.debugf(SendHeart, "->[S%v] heartbeat, req: %v", server, toJson(req))
		ok := rf.peers[server].Call("Raft.AppendEntries", req, resp)
		if rf.killed() {
			rf.infof("be killed")
			break
		}
		if rf.GetTerm() != req.Term {
			rf.infof("outdated message: %v", req.ID)
			return
		}
		if !rf.isLeader() {
			rf.infof("not leader")
			return
		}
		if !ok {
			rf.debugf(SendHeart, "->[S%v] heartbeat, fail req: %v", server, toJson(req))
		} else {
			rf.Lock()
			defer rf.Unlock()
			if resp.Success {
				rf.debugf(SendHeart, "->[S%v] heartbeat success, id: %v", server, req.ID)
				return
			} else {
				if rf.CheckTermNewer(resp.Term) {
					rf.debugf(SendHeart, "->[S%v] heartbeat success, but fail become follower, req: %v, resp: %v",
						server, toJson(req), toJson(resp))
					rf.TransFollower()
					return
				} else {
					rf.warnf("->[S%v] heartbeat success, but fail others reasonreq: %v, resp: %v",
						server, toJson(req), toJson(resp))
					return
				}
			}
			return
		}
	}
}

func (rf *Raft) SendAllHeartBeat() {
	for i, _ := range rf.peers {
		if i != rf.me {
			req := AppendEntriesRequest{
				Term:         rf.CurrentTerm,
				LeaderID:     rf.me,
				Entries:      nil,
				LeaderCommit: rf.CommitIndex,
				PrevLogIndex: rf.MatchIndex[i], // todo
			}
			req.PrevLogTerm = rf.Logs.GetEntry(req.PrevLogIndex).Term // todo
			resp := AppendEntriesResponse{}
			if rf.NextIndex[i] <= rf.Logs.GetLastIndex() {
				req.Entries = rf.Logs.GetSlice(rf.NextIndex[i], rf.Logs.GetLastIndex())
				go rf.SendLogData(i, &req, &resp, rf.Logs.GetLastIndex()+1)
			} else {
				go rf.SendHeartBeat(i, &req, &resp)
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
		ms := HeartBeatMinTime + (rand.Int63() % 151)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
