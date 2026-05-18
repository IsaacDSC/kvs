package node

type State struct {
	ID          string `json:"ID"`
	Role        string `json:"Role"`
	LeaderID    string `json:"LeaderID,omitempty"`
	Term        int    `json:"Term"`
	CommitIndex int    `json:"CommitIndex"`
	LogLen      int    `json:"LogLen"`
}
