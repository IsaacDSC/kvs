package raft

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (r State) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return "unknown"
}
