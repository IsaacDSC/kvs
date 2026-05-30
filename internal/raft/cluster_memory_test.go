package raft

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/IsaacDSC/kvs/internal/commands"
	"github.com/IsaacDSC/kvs/internal/dto"
)

// memoryBridge is an in-process Network for deterministic Raft tests (no gRPC).
type memoryBridge struct {
	mu   sync.RWMutex
	byID map[string]*Node // peer id strings used as network addresses (e.g. "p1")
}

func newMemoryBridge() *memoryBridge {
	return &memoryBridge{byID: make(map[string]*Node)}
}

func (b *memoryBridge) Register(peerAddr string, n *Node) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.byID[peerAddr] = n
}

func (b *memoryBridge) node(peerAddr string) *Node {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.byID[peerAddr]
}

func (b *memoryBridge) RequestVote(ctx context.Context, peer string, args RequestVoteArgs, reply *RequestVoteReply) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	target := b.node(peer)
	if target == nil {
		panic("memoryBridge: unknown peer " + peer)
	}
	target.HandleRequestVote(args, reply)
	return nil
}

func (b *memoryBridge) AppendEntries(ctx context.Context, peer string, args AppendEntriesArgs, reply *AppendEntriesReply) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	target := b.node(peer)
	if target == nil {
		panic("memoryBridge: unknown peer " + peer)
	}
	target.HandleAppendEntries(args, reply)
	return nil
}

func (b *memoryBridge) Close() {}

func drainApplied(ctx context.Context, n *Node) {
	go func(nn *Node) {
		for {
			select {
			case <-nn.Applied():
			case <-ctx.Done():
				return
			}
		}
	}(n)
}

func waitLeader(t *testing.T, nodes []*Node, timeout time.Duration) *Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, nn := range nodes {
			st := nn.State()
			if st.Role == Leader.String() {
				return nn
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no leader within %v", timeout)
	return nil
}

func assertCommittedPrefix(t *testing.T, nodes []*Node, commitIndexWant int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for _, nn := range nodes {
			if nn.State().CommitIndex < commitIndexWant {
				ok = false
				break
			}
		}
		if ok {
			for _, nn := range nodes {
				if got := nn.State().CommitIndex; got != commitIndexWant {
					t.Fatalf("cluster commitIndex uneven: node commitIndex=%d want %d", got, commitIndexWant)
				}
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	var got []int
	for _, nn := range nodes {
		got = append(got, nn.State().CommitIndex)
	}
	t.Fatalf("clusters did not reach commitIndex=%v within %v: got %+v", commitIndexWant, timeout, got)
}

// Validates election + replication + commit + Applied delivery with MinAcks=N on a 3-node cluster.
func TestCluster_threeMembers_minAcksFullReplication(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	br := newMemoryBridge()
	defer br.Close()

	const p1, p2, p3 = "net1", "net2", "net3"

	n1 := NewNode("n1", []string{p2, p3}, br, log)
	n2 := NewNode("n2", []string{p1, p3}, br, log)
	n3 := NewNode("n3", []string{p1, p2}, br, log)

	br.Register(p1, n1)
	br.Register(p2, n2)
	br.Register(p3, n3)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	drainApplied(ctx, n1)
	drainApplied(ctx, n2)
	drainApplied(ctx, n3)

	go n1.Run(ctx)
	go n2.Run(ctx)
	go n3.Run(ctx)

	leader := waitLeader(t, []*Node{n1, n2, n3}, 3*time.Second)

	data := commands.Data{
		Cmd:       commands.SetCmd,
		TableName: "tbl",
		Item: dto.Item{
			Key:   "k1",
			SK:    "sk1",
			Value: map[string]any{"hello": "world"},
		},
		MinAcks: 3,
	}

	if rp := leader.ProposeCommand(data); rp != nil {
		t.Fatalf("ProposeCommand: %v", rp.Err())
	}

	assertCommittedPrefix(t, []*Node{n1, n2, n3}, 0, 2*time.Second)
}
