package cfg

import (
	"errors"
	"flag"
	"fmt"
	"strings"
)

// ErrMissingNodeID is returned when the node process starts without -id.
var ErrMissingNodeID = errors.New("node id is required (flag -id)")

// NodeFlags holds validated CLI flags for the storage node (identity and listen addresses).
type NodeFlags struct {
	ID       string
	HTTPAddr string
	GRPCAddr string
	Peers    []string
}

// ParseNodeFlags registers node flags on fs, parses args (typically os.Args[1:]),
// and returns validated values. Call before [Load] when using both CLI and environment config.
func ParseNodeFlags(fs *flag.FlagSet, args []string) (NodeFlags, error) {
	id := fs.String("id", "", "node id")
	httpAddr := fs.String("http-addr", "", "node addrs")
	peersRaw := fs.String("peers", "", "node peers")
	grpcAddr := fs.String("grpc-addr", ":9080", "gRPC listen address for node-to-node RPCs (e.g. :9001)")

	if err := fs.Parse(args); err != nil {
		return NodeFlags{}, fmt.Errorf("cfg: parse flags: %w", err)
	}

	out := NodeFlags{
		ID:       strings.TrimSpace(*id),
		HTTPAddr: strings.TrimSpace(*httpAddr),
		GRPCAddr: strings.TrimSpace(*grpcAddr),
		Peers:    parsePeers(*peersRaw),
	}

	if out.ID == "" {
		return NodeFlags{}, fmt.Errorf("cfg: %w", ErrMissingNodeID)
	}

	return out, nil
}

func parsePeers(raw string) []string {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
