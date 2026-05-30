package api

// HTTPDefaultRaftMinAcks maps omitted or zero raft_min_acks (query / JSON) to full-cluster
// replication; otherwise leaves v unchanged.
func HTTPDefaultRaftMinAcks(rep ReplicateNodes, v int) int {
	if v != 0 {
		return v
	}
	return rep.FullClusterReplicationMinAcks()
}
