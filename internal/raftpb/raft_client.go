package raftpb

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
)

type Client struct {
	conn grpc.ClientConnInterface
}

func NewClient(conn grpc.ClientConnInterface) *Client {
	return &Client{conn: conn}
}

func (rc Client) RequestVote(ctx context.Context, request *VoteRequest, opts ...grpc.CallOption) (*VoteResponse, error) {
	out := new(VoteResponse)
	if err := rc.conn.Invoke(ctx, "/raft.Raft/RequestVote", request, out, opts...); err != nil {
		return nil, fmt.Errorf("request vote: %w", err)
	}
	return out, nil
}

// Replicate opens the bidirectional Replicate stream to the remote peer.
// The returned LeaderStream stays open across heartbeats; the caller must
// recreate it when it returns an error.

func (rc Client) Replicate(ctx context.Context, opts ...grpc.CallOption) (*LeaderStream, error) {
	stream, err := rc.conn.NewStream(ctx, &ServiceDesc.Streams[0], "/raft.Raft/Replicate", opts...)
	if err != nil {
		return nil, fmt.Errorf("new stream: %w", err)
	}

	return &LeaderStream{stream}, nil
}
