package raft

import (
	"math/rand"
	"time"
)

//
// RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term         int	//candidate’s term
	CandidateId  int	//candidate requesting vote
	LastLogIndex int	//index of candidate’s last log entry (§5.4)
	LastLogTerm  int	//term of candidate’s last log entry (§5.4)
}

//
// RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int		//currentTerm, for candidate to update itself
	VoteGranted bool	//true means candidate received vote
}

func generateRandElectionTimeout() time.Duration {
	randTime := (rand.Int63n(ElectionTimeout) + ElectionTimeout)
	return time.Duration(randTime)*time.Millisecond
}

//
// this is when a raft server receives a voting request from other candidate
// Receiver implementation:
// 1. Reply false if term < currentTerm (§5.1)
// 2. If votedFor is null or candidateId, and candidate’s log is at
// 	  least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	DPrintf("server %d got vote request", rf.me)

	if args.Term < rf.CurrentTerm {
		DPrintf("term error")
		return
	}

	//current raft server falls behind
	//change state to follower and clear VoteFor, so that rf can still vote
	//if args.Term > rf.CurrentTerm {
	//	rf.convertToFollower(args.Term)
	//}

	//check if this sever have already voted
	if args.Term == rf.CurrentTerm {
		if rf.CurrentState == Leader {
			return
		}

		//Had already voted for this candidate
		if rf.VotedFor == args.CandidateId {
			reply.VoteGranted = true///???
			DPrintf("already voted")
			return
		}

		//Had already voted for another candidate
		if rf.VotedFor != -1 && rf.VotedFor != args.CandidateId {
			return
		}
	}

	////check if candidate's log term&index are more up to date
	//lastLogTerm, lastLogIndex := rf.getLastLogInfo()
	//if lastLogTerm > args.LastLogTerm ||
	//	(lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex) {
	//	return
	//}


	//satisfied condition, vote for this candidate
	rf.convertToFollower(args.Term)
	rf.electionTimer.Reset(generateRandElectionTimeout())
	rf.VotedFor = args.CandidateId
	reply.VoteGranted = true
	return
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, replyCh chan <- RequestVoteReply) {
	var reply RequestVoteReply
	rf.peers[server].Call("Raft.RequestVote", &args, &reply)
	replyCh <- reply
}

//
//This is a candidate asking votes from peers
//
func (rf *Raft) startElection() {
	//DPrintf("Server %d(%d) start election...", rf.me, rf.CurrentState)
	rf.mu.Lock()

	if rf.CurrentState == Leader {
		rf.mu.Unlock()
		return
	}
	rf.convertToCandidate()

	DPrintf("Server %d become candidate! start election term = %d", rf.me, rf.CurrentTerm)
	args := RequestVoteArgs{
		Term: 	rf.CurrentTerm,
		CandidateId: rf.me,
	}
	args.LastLogTerm, args.LastLogIndex = rf.getLastLogInfo()
	electionTime := generateRandElectionTimeout()
	rf.electionTimer.Reset(electionTime)
	timeoutTimer := time.After(electionTime)
	rf.mu.Unlock()

	replyCh := make(chan RequestVoteReply, len(rf.peers) - 1)


	votesReceived := 0

	for i,_ := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, args, replyCh)
	}

	for votesReceived < len(rf.peers) / 2  {
		select {
		case <-rf.dead:
			return
		case <-timeoutTimer://election time out
			return
		case reply := <-replyCh:
			if reply.VoteGranted {
				votesReceived += 1
			} else {//not granted
				rf.mu.Lock()
				if rf.CurrentTerm < reply.Term {
					rf.convertToFollower(reply.Term)
				}
				rf.mu.Unlock()
			}
		}
	}

	//got enough vote
	rf.mu.Lock()
	if rf.CurrentState == Candidate {
		rf.convertToLeader()
		DPrintf("Server %d become leader! start 心跳", rf.me)
		//start heartbeat
		go rf.startHeartBeat()
	}
	rf.mu.Unlock()

}