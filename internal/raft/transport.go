package raft

import (
	"context"
	"fmt"
	"sync"

	"github.com/IsaacDSC/kvs/internal/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

type Transport struct {
	mu    sync.Mutex
	peers map[string]*peerConn
}

func NewTransport() *Transport {
	return &Transport{peers: make(map[string]*peerConn)}
}

// Close tears down every open gRPC connection.
func (t *Transport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, pc := range t.peers {
		pc.close()
	}
	t.peers = make(map[string]*peerConn)
}

// RequestVote sends a unary RequestVote RPC to peer.
func (t *Transport) RequestVote(ctx context.Context, peer string, args RequestVoteArgs, reply *RequestVoteReply) error {
	pc, err := t.getOrDial(peer)
	if err != nil {
		return err
	}

	resp, err := pc.client.RequestVote(ctx,
		&raftpb.VoteRequest{
			Term:         int32(args.Term),
			CandidateID:  args.CandidateID,
			LastLogIndex: int32(args.LastLogIndex),
			LastLogTerm:  int32(args.LastLogTerm),
		},
		grpc.ForceCodec(encoding.GetCodec(raftpb.CodecName)),
	)
	if err != nil {
		return fmt.Errorf("RequestVote rpc: %w", err)
	}

	reply.Term = int(resp.Term)
	reply.VoteGranted = resp.VoteGranted
	return nil
}

// AppendEntries sends an AppendEntries message over the persistent
// bidirectional Replicate stream to peer.
func (t *Transport) AppendEntries(ctx context.Context, peer string, args AppendEntriesArgs, reply *AppendEntriesReply) error {
	pc, err := t.getOrDial(peer)
	if err != nil {
		return err
	}
	return pc.appendEntries(args, reply)
}

// getOrDial returns the existing peerConn for addr or dials a new one.
func (t *Transport) getOrDial(addr string) (*peerConn, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if pc, ok := t.peers[addr]; ok {
		return pc, nil
	}
	pc, err := NewDialPeerConn(addr)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	t.peers[addr] = pc
	return pc, nil
}
