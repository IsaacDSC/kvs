package raft

import (
	"context"
	"fmt"
	"sync"

	"github.com/IsaacDSC/kvs/internal/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
)

type peerConn struct {
	conn   *grpc.ClientConn
	client *raftpb.Client

	mu           sync.Mutex
	leaderStream *raftpb.LeaderStream
}

func (pc *peerConn) close() {
	pc.mu.Lock()
	if pc.leaderStream != nil {
		pc.leaderStream.CloseSend() //nolint:errcheck
		pc.leaderStream = nil
	}
	pc.mu.Unlock()
	pc.conn.Close() //nolint:errcheck
}

func NewDialPeerConn(addr string) (*peerConn, error) {
	// grpc.NewClient does not block; the connection is established lazily.
	cc, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}
	return &peerConn{
		conn:   cc,
		client: raftpb.NewClient(cc),
	}, nil
}

// appendEntries serialises one request+response over the persistent stream.
// It holds pc.mu for the duration so that heartbeat goroutines do not
// interleave their send/recv pairs.
func (pc *peerConn) appendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	stream, err := pc.ensureStream()
	if err != nil {
		return fmt.Errorf("open replicate stream: %w", err)
	}

	req := toReplicateRequest(args)
	if err := stream.Send(req); err != nil {
		pc.leaderStream = nil // mark broken; will be recreated next call
		return fmt.Errorf("stream send: %w", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		pc.leaderStream = nil
		return fmt.Errorf("stream recv: %w", err)
	}

	reply.Term = int(resp.Term)
	reply.Success = resp.Success
	return nil
}

// ensureStream returns the open stream or creates a new one.
// Must be called with pc.mu held.
func (pc *peerConn) ensureStream() (*raftpb.LeaderStream, error) {
	if pc.leaderStream != nil {
		return pc.leaderStream, nil
	}
	// context.Background: the stream outlives any individual AppendEntries call.
	stream, err := pc.client.Replicate(
		context.Background(),
		grpc.ForceCodec(encoding.GetCodec(raftpb.CodecName)),
	)
	if err != nil {
		return nil, err
	}
	pc.leaderStream = stream
	return stream, nil
}
