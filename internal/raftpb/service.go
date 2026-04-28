package raftpb

import (
	"context"

	"google.golang.org/grpc"
)

type Server interface {
	RequestVote(context.Context, *VoteRequest) (*VoteResponse, error)
	Replicate(*FollowerStream) error
}

var ServiceDesc = grpc.ServiceDesc{
	ServiceName: "raft.Raft",
	HandlerType: (*Server)(nil),
	Methods: []grpc.MethodDesc{{
		MethodName: "RequestVote",
		Handler: func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
			in := new(VoteRequest)
			if err := dec(in); err != nil {
				return nil, err
			}
			if interceptor == nil {
				return srv.(Server).RequestVote(ctx, in)
			}
			return interceptor(ctx, in,
				&grpc.UnaryServerInfo{Server: srv, FullMethod: "/raft.Raft/RequestVote"},
				func(ctx context.Context, req any) (any, error) {
					return srv.(Server).RequestVote(ctx, req.(*VoteRequest))
				},
			)
		},
	}},
	Streams: []grpc.StreamDesc{{
		StreamName: "Replicate",
		Handler: func(srv any, s grpc.ServerStream) error {
			return srv.(Server).Replicate(&FollowerStream{s})
		},
		ServerStreams: true,
		ClientStreams: true,
	}},
	Metadata: "proto/raft/raft.proto",
}
