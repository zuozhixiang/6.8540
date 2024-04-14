package raft

import (
	"math/rand"
	"sync/atomic"
	"time"
)

const (
	MinTimeOutElection = 500
	MaxTimeOutElection = 1000
)

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	ID           int64
	Term         int32
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int32
}
type Status int

const (
	Success Status = iota
	OutDateTerm
	NoMatch
)

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int32
	VoteGranted bool
	Status      Status
}

func (rf *Raft) CheckTermAndUpdate(term int32) bool {
	// if term >= currentTerm , then update, return true
	if term >= rf.CurrentTerm {
		atomic.StoreInt32(&rf.CurrentTerm, term)
		return true
	}
	return false
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(req *RequestVoteArgs, resp *RequestVoteReply) {
	resp.VoteGranted = false
	curTerm := rf.getTerm()
	resp.Term = curTerm
	if rf.killed() {
		return
	}
	if curTerm > req.Term {
		resp.Status = OutDateTerm
		return
	}
	rf.Lock()
	defer rf.Unlock()
	if req.Term > curTerm {
		rf.setTerm(req.Term)
		rf.VotedFor = NoneVote
		rf.TransFollower()
	}
	// candidate term is bigger, and votedfor no one,  candidate's logs at least >= self's logs
	if rf.VotedFor == NoneVote || rf.VotedFor == req.CandidateID {
		lastIndex := rf.Logs.GetLastIndex()
		lastTerm := rf.Logs.GetLastTerm()
		// 要求， 候选人的日志比自己的新， 要么任期 比我大， 要么任期相同，小于比我大
		if lastTerm < req.LastLogTerm || (lastTerm == req.LastLogTerm && lastIndex <= req.LastLogIndex) {
			rf.debugf(ReciveVote, "Candidate[S%v]-> S[%v]-> success req: %v", req.CandidateID, rf.me, toJson(req))
			resp.VoteGranted = true
			rf.VotedFor = req.CandidateID // todo, receive leader's heartbeat , clear VotedFor
			rf.TransFollower()
		} else {
			rf.debugf(ReciveVote, "Candidate[S%v]-> S[%v]-> fail log old req: %v", req.CandidateID, rf.me, toJson(req))
		}
	} else {
		rf.debugf(ReciveVote, "Candidate[S%v]-> S[%v] fail votedFor:[S%v] req: %v", req.CandidateID, rf.VotedFor, rf.me, toJson(req))
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.

func (rf *Raft) sendRequestVote(server int, req *RequestVoteArgs, resp *RequestVoteReply, cnt *int32) {
	for !rf.killed() {
		ok := rf.peers[server].Call("Raft.RequestVote", req, resp)
		if rf.killed() || !rf.isCandidate() || rf.getTerm() != req.Term {
			return
		}
		if ok {
			break
		}
	}

	rf.Lock()
	defer rf.Unlock()
	if resp.VoteGranted {
		atomic.AddInt32(cnt, 1)
		if atomic.LoadInt32(cnt) > int32(rf.n/2) {
			if rf.isLeader() {
				return
			}
			if rf.isCandidate() {
				rf.TransLeader()
			}
		}
	} else {
		if resp.Status == OutDateTerm {
			rf.VotedFor = NoneVote
			rf.setTerm(max(rf.CurrentTerm, resp.Term))
			rf.TransFollower()
		}
	}
}

func (rf *Raft) isLeader() bool {
	state := atomic.LoadInt32(&rf.State)
	ret := state == Leader
	return ret
}

func (rf *Raft) isCandidate() bool {
	state := atomic.LoadInt32(&rf.State)
	ret := state == Candidate
	return ret
}

func (rf *Raft) isFollower() bool {
	state := atomic.LoadInt32(&rf.State)
	ret := state == Follower
	return ret
}

func (rf *Raft) TransLeader() {
	rf.VotedFor = NoneVote
	atomic.StoreInt32(&rf.State, Leader)
	for i := 0; i < rf.n; i++ {
		rf.NextIndex[i] = rf.Logs.GetLastIndex() + 1
		rf.MatchIndex[i] = 0
	}
	rf.SendAllHeartBeat()
}

func (rf *Raft) TransFollower() {
	atomic.StoreInt32(&rf.State, Follower)
	rf.RestartTimeOutElection()
}

func (rf *Raft) StartElection() {
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	atomic.StoreInt32(&rf.State, Candidate)
	rf.RestartTimeOutElection()
	rf.SendAllRequestVote()
}

func (rf *Raft) SendAllRequestVote() {
	id := rf.me
	lastTerm := rf.Logs.GetLastTerm()
	lastIndex := rf.Logs.GetLastIndex()
	term := rf.CurrentTerm
	cnt := int32(1)
	for i, _ := range rf.peers {
		if i != rf.me {
			resp := RequestVoteReply{}
			req := RequestVoteArgs{
				ID:           getID(),
				Term:         term,
				CandidateID:  id,
				LastLogIndex: lastIndex,
				LastLogTerm:  lastTerm,
			}
			go rf.sendRequestVote(i, &req, &resp, &cnt)
		}
	}
}

func (rf *Raft) checkTimeoutElection() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.Lock()
		if rf.State == Follower || rf.State == Candidate {
			diff := GetNow() - rf.TimeOutElection
			if diff >= 0 {
				// start leader election
				rf.debugf(LeaderElection, "timeout: %vms", rf.TimeOutDuration)
				rf.StartElection()
			}
		}
		rf.Unlock()
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) RestartTimeOutElection() {
	ms := MinTimeOutElection + (rand.Int63() % (MaxTimeOutElection - MinTimeOutElection))
	rf.TimeOutDuration = ms
	rf.TimeOutElection = GetNow() + ms
}
