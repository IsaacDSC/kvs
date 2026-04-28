package raftpb

import "google.golang.org/grpc"

type FollowerStream struct{ grpc.ServerStream }

func (s *FollowerStream) Recv() (*ReplicateRequest, error) {
	req := new(ReplicateRequest)
	return req, s.RecvMsg(req)
}

func (s *FollowerStream) Send(resp *ReplicateResponse) error {
	return s.SendMsg(resp)
}
