package raft

import "time"

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PervLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int	//currentTerm, for leader to update itself
	Success   bool	//true if follower contained entry matching
					//	prevLogIndex and prevLogTerm
}

//
//This is when a server receives appendentry(Heartbeat) instruction from a leader
//Receiver implementation:
//1. Reply false if term < currentTerm (§5.1)
//2. Reply false if log doesn’t contain an entry at prevLogIndex
//		whose term matches prevLogTerm (§5.3)
//3. If an existing entry conflicts with a new one (same index
//		but different terms), delete the existing entry and all that
//		follow it (§5.3)
//4. Append any new entries not already in the log
//5. If leaderCommit > commitIndex, set commitIndex =
//		min(leaderCommit, index of last new entry)
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//DPrintf("Server %d 收到心跳 from %d", rf.me, args.LeaderId)
	if rf.CurrentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.CurrentTerm
		return
	}

	rf.electionTimer.Reset(generateRandElectionTimeout())

	if args.Term > rf.CurrentTerm{
		rf.convertToFollower(args.Term)
	}
	reply.Term = args.Term
	rf.LeaderID = args.LeaderId

	reply.Success = true
}

//func (rf *Raft) sendAppendEntries(peerIndex int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
//	ok := rf.peers[peerIndex].Call("Raft.AppendEntries", args, reply)
//	return ok
//}

func (rf *Raft) startHeartBeat() {
	timer := time.NewTimer(HeartBeatTimeout)
	for {
		select {
		case <-rf.dead:
			return
		case <-timer.C:
			if _, isLeader := rf.GetState(); !isLeader {
				return
			}
			go func() {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				for i,_ := range rf.peers {
					if i == rf.me {
						continue
					} else {
						go rf.sendHeartBeat(i)
					}
				}
			}()
			timer.Reset(HeartBeatTimeout)
		}
	}
}


func (rf *Raft) sendHeartBeat(peerIndex int) {

	rf.mu.Lock()

	if rf.CurrentState != Leader {
		rf.mu.Unlock()
		return
	}

	args := AppendEntriesArgs{
		Term:         rf.CurrentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: 0,
		PervLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	}
	rf.mu.Unlock()

	reply := AppendEntriesReply{}

	if rf.peers[peerIndex].Call("Raft.AppendEntries", &args, &reply) {
		rf.mu.Lock()
		if !reply.Success{
			if reply.Term > rf.CurrentTerm {
				rf.convertToFollower(reply.Term)
			}
		}else {

		}
		rf.mu.Unlock()
	}


}
