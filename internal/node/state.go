package node

type State struct {
	ID          string `json:"ID"`
	Role        string `json:"Role"`
	LeaderID    string `json:"LeaderID,omitempty"`
	Term        int    `json:"Term"`
	CommitIndex int    `json:"CommitIndex"`
	LogLen      int    `json:"LogLen"`
	// MajorityRepMinAcks is the Raft majority threshold (⌊N/2⌋+1) for election and lower bound for commits.
	MajorityRepMinAcks int `json:"MajorityRepMinAcks"`
	// EffectiveRepMinAcks is the replication bar when commands.Data.MinAcks is omitted (= full cluster size N).
	EffectiveRepMinAcks int `json:"EffectiveRepMinAcks"`
}
