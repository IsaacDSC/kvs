package node

type State struct {
	ID          string
	Role        string
	Term        int
	CommitIndex int
	LogLen      int
}
