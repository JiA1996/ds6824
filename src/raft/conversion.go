package raft

func (rf *Raft) convertToLeader() {
	rf.CurrentState = Leader
	rf.LeaderID = rf.me
	//rf.persist()
	rf.electionTimer.Stop()
}

func (rf *Raft) convertToCandidate() {
	rf.LeaderID = -1
	rf.CurrentState = Candidate
	rf.CurrentTerm += 1
	rf.VotedFor = rf.me
	//rf.persist()
}

func (rf *Raft) convertToFollower(term int) {
	rf.CurrentState = Follower
	rf.CurrentTerm = term
	rf.VotedFor = -1
	rf.LeaderID = -1///???
	//rf.persist()

}