package raft

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	"github.com/IsaacDSC/kvs/internal/commands"
	"github.com/IsaacDSC/kvs/internal/node"
)

// ─── Timing constants (§5.2, §5.6) ──────────────────────────────────────────

const (
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond
	heartbeatInterval  = 50 * time.Millisecond
)

type Node struct {
	mu sync.Mutex

	id    string
	peers []string // addresses of all other nodes

	// Persistent state (simplified: kept only in memory)
	currentTerm int
	votedFor    string
	log         []LogEntry

	// Volatile state
	state       State
	leaderID    string // ID of the current known leader ("" when unknown)
	commitIndex int    // index of highest log entry known to be committed (-1 = none)
	lastApplied int    // index of highest log entry applied to state machine

	// Leader-only volatile state (reset on each election win)
	nextIndex  map[string]int // next log index to send to each peer
	matchIndex map[string]int // highest log index known to be replicated on each peer

	transport *Transport

	// resetElection is signalled by incoming RPCs to prevent spurious elections.
	resetElection chan struct{}

	// applied receives committed entries so the caller can run a state machine.
	applied chan LogEntry

	// commitNotify is a buffered-1 channel that wakes up the delivery goroutine
	// whenever commitIndex advances. Using a separate goroutine for delivery
	// allows blocking channel sends without holding n.mu, eliminating silent drops.
	commitNotify chan struct{}

	// onMetaPersist, when non-nil, is called (outside n.mu) whenever currentTerm
	// or votedFor changes. Implementations should persist these fields to stable
	// storage so they survive restarts (Raft §5.1/§5.2).
	onMetaPersist func(term int, votedFor string)

	logger *slog.Logger
}

// NewNode creates a new Node with no prior state. Call Run to start the election and replication loops.
func NewNode(id string, peers []string, transport *Transport, logger *slog.Logger) *Node {
	return &Node{
		id:            id,
		peers:         peers,
		state:         Follower,
		commitIndex:   -1,
		lastApplied:   -1,
		transport:     transport,
		resetElection: make(chan struct{}, 1),
		applied:       make(chan LogEntry, 128),
		commitNotify:  make(chan struct{}, 1),
		logger:        logger,
	}
}

// PersistedState holds the durable Raft state restored from the RaftWAL on startup.
// All entries in Log are assumed to have been committed and applied to the KV state
// machine; the node starts with commitIndex = lastApplied = len(Log)-1 so the leader
// will not re-deliver already-applied entries to this node.
type PersistedState struct {
	// Log is the ordered list of committed+applied entries recovered from raft.wal.
	Log []LogEntry
	// CurrentTerm is the last term this node persisted (§5.1).
	CurrentTerm int
	// VotedFor is the candidate this node voted for in CurrentTerm (§5.2).
	VotedFor string
}

// NewNodeWithState creates a Node with state restored from a previous run.
// Use instead of NewNode when RaftWAL.Load returned a non-empty result.
func NewNodeWithState(id string, peers []string, transport *Transport, logger *slog.Logger, ps PersistedState) *Node {
	commitIndex := len(ps.Log) - 1
	return &Node{
		id:            id,
		peers:         peers,
		state:         Follower,
		currentTerm:   ps.CurrentTerm,
		votedFor:      ps.VotedFor,
		log:           ps.Log,
		commitIndex:   commitIndex,
		lastApplied:   commitIndex,
		transport:     transport,
		resetElection: make(chan struct{}, 1),
		applied:       make(chan LogEntry, 128),
		commitNotify:  make(chan struct{}, 1),
		logger:        logger,
	}
}

func (n *Node) HandleRequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > n.currentTerm {
		n.stepDown(args.Term)
	}
	reply.Term = n.currentTerm

	lastLogIndex, lastLogTerm := n.lastLogInfo()
	// §5.4.1: candidate's log must be at least as up-to-date as ours.
	candidateUpToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)

	if (n.votedFor == "" || n.votedFor == args.CandidateID) && candidateUpToDate {
		n.votedFor = args.CandidateID
		reply.VoteGranted = true
		n.signalReset()
		n.logger.Info("voted for", "candidate", args.CandidateID, "term", args.Term)
		n.notifyMetaChange()
	}
}

func (n *Node) HandleAppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		reply.Term = n.currentTerm
		reply.Success = false
		return
	}

	if args.Term > n.currentTerm {
		n.stepDown(args.Term)
	} else {
		// Same term: ensure we step down if we thought we were a candidate.
		n.state = Follower
	}

	// Track the current leader so followers can surface it to clients.
	n.leaderID = args.LeaderID

	n.signalReset()
	reply.Term = n.currentTerm

	// §5.3: check that our log contains prevLogIndex with prevLogTerm.
	if args.PrevLogIndex >= 0 {
		if args.PrevLogIndex >= len(n.log) ||
			n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			return
		}
	}

	// Append new entries, overwriting conflicts.
	insertAt := args.PrevLogIndex + 1
	for i, entry := range args.Entries {
		logIdx := insertAt + i
		if logIdx < len(n.log) {
			if n.log[logIdx].Term != entry.Term {
				n.log = append(n.log[:logIdx], args.Entries[i:]...)
				break
			}
		} else {
			n.log = append(n.log, args.Entries[i:]...)
			break
		}
	}

	// Advance commit index based on leader's commit.
	if args.LeaderCommit > n.commitIndex {
		newCommit := args.LeaderCommit
		if lastIdx := len(n.log) - 1; lastIdx < newCommit {
			newCommit = lastIdx
		}
		n.commitIndex = newCommit
		// Wake up the delivery goroutine (non-blocking; one pending signal is enough).
		select {
		case n.commitNotify <- struct{}{}:
		default:
		}
	}

	reply.Success = true

}

func (n *Node) ProposeCommand(command commands.Data) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		if n.leaderID != "" {
			return fmt.Errorf("not the leader: send your request to leader=%s (current state: %s)", n.leaderID, n.state)
		}
		return fmt.Errorf("not the leader: no leader known yet (current state: %s)", n.state)
	}

	n.log = append(n.log, LogEntry{Term: n.currentTerm, Data: command})
	n.logger.Info("proposed", "command", command, "index", len(n.log)-1)

	return nil
}

func (n *Node) Propose(command string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state != Leader {
		// TODO: retornar o leader ID e host para o cliente
		return fmt.Errorf("not the leader (current state: %s)", n.state)
	}

	// TODO: remover depois quando não utilizar mais a rota de command e ter que aceitar string
	n.log = append(n.log, LogEntry{Term: n.currentTerm, Data: commands.Data{}})
	n.logger.Info("proposed", "command", command, "index", len(n.log)-1)
	return nil
}

func (n *Node) State() node.State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return node.State{
		ID:          n.id,
		Role:        n.state.String(),
		LeaderID:    n.leaderID,
		Term:        n.currentTerm,
		CommitIndex: n.commitIndex,
		LogLen:      len(n.log),
	}
}

// SetMetaPersister registers a callback that is invoked (outside n.mu) whenever
// currentTerm or votedFor changes. The callback should durably persist these
// values so they survive restarts (Raft §5.1/§5.2). It must be set before Run.
func (n *Node) SetMetaPersister(fn func(term int, votedFor string)) {
	n.onMetaPersist = fn
}

func (n *Node) Run(ctx context.Context) {
	// runDelivery forwards committed log entries to the applied channel without
	// holding n.mu, so sends are blocking and no entry is ever silently dropped.
	go n.runDelivery(ctx)

	for {
		n.mu.Lock()
		state := n.state
		n.mu.Unlock()

		switch state {
		case Follower:
			n.runFollower(ctx)
		case Candidate:
			n.runCandidate(ctx)
		case Leader:
			n.runLeader(ctx)
		}

		if ctx.Err() != nil {
			return
		}
	}
}

// runDelivery forwards committed log entries (lastApplied+1 … commitIndex) to
// the applied channel. It runs independently of n.mu so that channel sends are
// blocking: if the consumer is slow we apply backpressure instead of dropping.
func (n *Node) runDelivery(ctx context.Context) {
	for {
		select {
		case <-n.commitNotify:
		case <-ctx.Done():
			return
		}

		for {
			n.mu.Lock()
			if n.lastApplied >= n.commitIndex {
				n.mu.Unlock()
				break
			}
			n.lastApplied++
			entry := n.log[n.lastApplied]
			n.mu.Unlock()

			select {
			case n.applied <- entry:
			case <-ctx.Done():
				return
			}
		}
	}
}

// Applied returns a channel that receives each committed log entry in order.
func (n *Node) Applied() <-chan LogEntry {
	return n.applied
}

// runFollower waits for an election timeout. Each valid incoming RPC resets the
// timer. If no RPC arrives in time, the node promotes itself to Candidate.
func (n *Node) runFollower(ctx context.Context) {
	timer := time.NewTimer(n.randomElectionTimeout())
	defer timer.Stop()

	n.mu.Lock()
	n.logger.Info("became follower", "term", n.currentTerm)
	n.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return

		case <-n.resetElection:
			// A valid leader or granting a vote: reset the timeout.
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(n.randomElectionTimeout())

		case <-timer.C:
			n.mu.Lock()
			n.state = Candidate
			n.mu.Unlock()
			return
		}
	}
}

// runCandidate starts a new election, collects votes, and either wins (→ Leader)
// or yields to a higher-term peer (→ Follower) or times out and retries.
func (n *Node) runCandidate(ctx context.Context) {
	n.mu.Lock()
	n.currentTerm++
	n.votedFor = n.id
	n.leaderID = "" // campaigning: leader unknown
	term := n.currentTerm
	lastLogIndex, lastLogTerm := n.lastLogInfo()
	n.logger.Info("became candidate", "term", term)
	n.notifyMetaChange() // persists currentTerm++ and votedFor=self; releases+re-acquires mu
	n.mu.Unlock()

	votes := 1 // vote for self
	quorum := n.quorum()
	voteCh := make(chan bool, len(n.peers))

	if votes >= quorum {
		n.mu.Lock()
		if n.state == Candidate && n.currentTerm == term {
			n.state = Leader
		}
		n.mu.Unlock()
		return
	}

	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  n.id,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	for _, peer := range n.peers {
		peer := peer
		go func() {
			var reply RequestVoteReply
			if err := n.transport.RequestVote(ctx, peer, args, &reply); err != nil {
				voteCh <- false
				return
			}
			n.mu.Lock()
			if reply.Term > n.currentTerm {
				n.stepDown(reply.Term)
			}
			n.mu.Unlock()
			voteCh <- reply.VoteGranted
		}()
	}

	timer := time.NewTimer(n.randomElectionTimeout())
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-n.resetElection:
			// A valid leader appeared while we were campaigning.
			return

		case <-timer.C:
			// Split vote or no quorum: restart election in next loop iteration.
			return

		case granted := <-voteCh:
			if granted {
				votes++
			}
			if votes >= quorum {
				n.mu.Lock()
				// Guard: another goroutine may have stepped us down already.
				if n.state == Candidate && n.currentTerm == term {
					n.state = Leader
				}
				n.mu.Unlock()
				return
			}
		}
	}
}

// runLeader sends heartbeats and replicates log entries to followers.
func (n *Node) runLeader(ctx context.Context) {
	n.mu.Lock()
	n.nextIndex = make(map[string]int, len(n.peers))
	n.matchIndex = make(map[string]int, len(n.peers))
	for _, peer := range n.peers {
		n.nextIndex[peer] = len(n.log)
		n.matchIndex[peer] = -1
	}
	n.leaderID = n.id // we are the leader
	term := n.currentTerm
	n.logger.Info("became leader", "term", term)
	n.mu.Unlock()

	// Send immediate heartbeat so followers don't time out.
	n.broadcastAppendEntries(ctx, term)

	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n.mu.Lock()
			stillLeader := n.state == Leader && n.currentTerm == term
			n.mu.Unlock()
			if !stillLeader {
				return
			}
			n.broadcastAppendEntries(ctx, term)
		}
	}
}

// broadcastAppendEntries sends AppendEntries RPCs to all peers concurrently.
func (n *Node) broadcastAppendEntries(ctx context.Context, term int) {
	for _, peer := range n.peers {
		peer := peer
		go n.sendAppendEntries(ctx, peer, term)
	}
}

func (n *Node) sendAppendEntries(ctx context.Context, peer string, term int) {
	n.mu.Lock()
	if n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	prevLogIndex := n.nextIndex[peer] - 1
	prevLogTerm := 0
	if prevLogIndex >= 0 && prevLogIndex < len(n.log) {
		prevLogTerm = n.log[prevLogIndex].Term
	}
	entries := make([]LogEntry, len(n.log)-n.nextIndex[peer])
	copy(entries, n.log[n.nextIndex[peer]:])
	leaderCommit := n.commitIndex
	n.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}

	var reply AppendEntriesReply
	if err := n.transport.AppendEntries(ctx, peer, args, &reply); err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.currentTerm {
		n.stepDown(reply.Term)
		return
	}
	if n.state != Leader || n.currentTerm != term {
		return
	}

	if reply.Success {
		if len(entries) > 0 {
			n.nextIndex[peer] = len(n.log)
			n.matchIndex[peer] = len(n.log) - 1
			n.maybeAdvanceCommit(term)
		}
	} else if n.nextIndex[peer] > 0 {
		// Log inconsistency: back up and retry on the next heartbeat.
		n.nextIndex[peer]--
	}
}

// maybeAdvanceCommit checks whether a new commit index can be established.
// Must be called with n.mu held.
func (n *Node) maybeAdvanceCommit(term int) {
	// Walk backwards from the newest entry to find the highest index
	// replicated on a quorum of nodes.
	for idx := len(n.log) - 1; idx > n.commitIndex; idx-- {
		// §5.4.2: only commit entries from the current term.
		if n.log[idx].Term != term {
			break
		}
		count := 1 // leader itself
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= idx {
				count++
			}
		}
		if count >= n.quorum() {
			n.commitIndex = idx
			// Wake up the delivery goroutine (non-blocking; one pending signal is enough).
			select {
			case n.commitNotify <- struct{}{}:
			default:
			}
			n.logger.Info("committed", "index", idx, "term", term)
			break
		}
	}
}

// quorum returns the minimum number of votes (including self) needed to win.
func (n *Node) quorum() int {
	return (len(n.peers)+1)/2 + 1
}

// lastLogInfo returns the index and term of the last log entry.
// Must be called with n.mu held.
func (n *Node) lastLogInfo() (index, term int) {
	idx := len(n.log) - 1
	if idx < 0 {
		return -1, 0
	}
	return idx, n.log[idx].Term
}

// stepDown transitions the node to Follower and updates the term.
// Must be called with n.mu held.
func (n *Node) stepDown(term int) {
	n.currentTerm = term
	n.state = Follower
	n.votedFor = ""
	n.leaderID = "" // new term → unknown leader until we receive AppendEntries
	n.notifyMetaChange()
}

// notifyMetaChange calls onMetaPersist outside n.mu if it is set.
// It captures the values while n.mu is held, then releases before calling.
// Callers must hold n.mu; the lock is re-acquired before returning.
func (n *Node) notifyMetaChange() {
	if n.onMetaPersist == nil {
		return
	}
	term, voted := n.currentTerm, n.votedFor
	n.mu.Unlock()
	n.onMetaPersist(term, voted)
	n.mu.Lock()
}

// signalReset notifies the election timer to reset (non-blocking).
// Must be called with n.mu held.
func (n *Node) signalReset() {
	select {
	case n.resetElection <- struct{}{}:
	default:
	}
}

func (n *Node) randomElectionTimeout() time.Duration {
	span := int64(electionTimeoutMax - electionTimeoutMin)
	return electionTimeoutMin + time.Duration(rand.Int63n(span))
}
