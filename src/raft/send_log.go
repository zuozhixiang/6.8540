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
	PrevLogTerm  int
	Entries      []LogEntry
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
		rf.debugf("Leader[S%v]-> append fail, leader term is old req: %v, resp: %v",
			req.LeaderID, toJson(req), toJson(resp))
		return
	}
	rf.VotedFor = NoneVote
	rf.TransFollower()
	if len(req.Entries) == 0 {
		// heartbeat
		resp.Success = true
		rf.debugf("Leader[S%v]-> heartbeat success req: %v, resp: %v", req.LeaderID, toJson(req), toJson(resp))
		return
	}
	// todo success votedfor set NoneVote
	rf.errorf("Leader[S%v]-> receive append log: req: %v, resp: %v", req.LeaderID, toJson(req), toJson(resp))

}

func (rf *Raft) SendHeartBeat(server int, req *AppendEntriesRequest, resp *AppendEntriesResponse) {
	req.ID = getID()
	for !rf.killed() {
		rf.debugf("->[S%v] heartbeat, req: %v", server, toJson(req))
		ok := rf.peers[server].Call("Raft.AppendEntries", req, resp)
		if rf.killed() {
			rf.infof("be killed")
			break
		}
		rf.Lock()
		if rf.CurrentTerm != req.Term {
			rf.infof("outdated message: %v", req.ID)
			rf.Unlock()
			return
		}
		rf.Unlock()
		if !rf.isLeader() {
			rf.infof("not leader")
			return
		}
		if !ok {
			rf.debugf("->[S%v] heartbeat, fail req: %v", server, toJson(req))
		} else {
			rf.Lock()
			defer rf.Unlock()
			if resp.Success {
				rf.debugf("->[S%v] heartbeat success, id: %v", server, req.ID)
				return
			} else {
				if rf.CheckTermNewer(resp.Term) {
					rf.debugf("->[S%v] heartbeat success, but fail become follower, req: %v, resp: %v",
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
			}
			resp := AppendEntriesResponse{}
			go rf.SendHeartBeat(i, &req, &resp)
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
