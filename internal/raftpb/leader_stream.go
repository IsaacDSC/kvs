package raftpb

import "google.golang.org/grpc"

type LeaderStream struct{ grpc.ClientStream }

func (s *LeaderStream) Send(req *ReplicateRequest) error {
	return s.SendMsg(req)
}

func (s *LeaderStream) Recv() (*ReplicateResponse, error) {
	resp := new(ReplicateResponse)
	return resp, s.RecvMsg(resp)
}
