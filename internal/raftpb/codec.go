// Package raftpb holds the wire types and gRPC service definition for the Raft
// protocol.  The code in this package is the manual equivalent of what
// protoc-gen-go and protoc-gen-go-grpc would generate from proto/raft/raft.proto.
//
// To regenerate from the .proto file (requires protoc):
//
//	protoc --go_out=. --go_opt=paths=source_relative \
//	       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
//	       proto/raft/raft.proto
package raftpb

import (
	"encoding/json"

	"google.golang.org/grpc/encoding"
)

// CodecName is the content-subtype used for content-type negotiation.
// Both client and server register this codec so gRPC can resolve it from
// "Content-Type: application/grpc+json".
const CodecName = "json"

func init() {
	encoding.RegisterCodec(jsonCodec{})
}

type jsonCodec struct{}

func (jsonCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (jsonCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
func (jsonCodec) Name() string                       { return CodecName }
