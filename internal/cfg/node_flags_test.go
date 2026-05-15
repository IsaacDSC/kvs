package cfg_test

import (
	"errors"
	"flag"
	"path/filepath"
	"slices"
	"testing"

	"github.com/IsaacDSC/kvs/internal/cfg"
)

func TestParseNodeFlags(t *testing.T) {
	t.Parallel()
	t.Run("missing id", func(t *testing.T) {
		t.Parallel()
		fs := flag.NewFlagSet(t.Name(), flag.ContinueOnError)
		_, err := cfg.ParseNodeFlags(fs, nil)
		if !errors.Is(err, cfg.ErrMissingNodeID) {
			t.Fatalf("error: got %v want wrap %v", err, cfg.ErrMissingNodeID)
		}
	})
	t.Run("ok with peers", func(t *testing.T) {
		t.Parallel()
		fs := flag.NewFlagSet(t.Name(), flag.ContinueOnError)
		got, err := cfg.ParseNodeFlags(fs, []string{
			"-id", "node1",
			"-http-addr", ":8001",
			"-grpc-addr", ":9001",
			"-peers", " localhost:9002 , localhost:9003 ",
		})
		if err != nil {
			t.Fatal(err)
		}
		if got.ID != "node1" || got.HTTPAddr != ":8001" || got.GRPCAddr != ":9001" {
			t.Fatalf("got %#v", got)
		}
		wantPeers := []string{"localhost:9002", "localhost:9003"}
		if !slices.Equal(got.Peers, wantPeers) {
			t.Fatalf("Peers: got %q want %q", got.Peers, wantPeers)
		}
		wantFS := filepath.Join("tmp", "node1")
		if got.FsDefaultDir != wantFS {
			t.Fatalf("FsDefaultDir: got %q want %q", got.FsDefaultDir, wantFS)
		}
	})
	t.Run("default grpc addr", func(t *testing.T) {
		t.Parallel()
		fs := flag.NewFlagSet(t.Name(), flag.ContinueOnError)
		got, err := cfg.ParseNodeFlags(fs, []string{"-id", "a"})
		if err != nil {
			t.Fatal(err)
		}
		if got.GRPCAddr != ":9080" {
			t.Fatalf("GRPCAddr: got %q", got.GRPCAddr)
		}
	})
	t.Run("default fsdb dir is tmp under node id", func(t *testing.T) {
		t.Parallel()
		fs := flag.NewFlagSet(t.Name(), flag.ContinueOnError)
		got, err := cfg.ParseNodeFlags(fs, []string{"-id", "node1"})
		if err != nil {
			t.Fatal(err)
		}
		want := filepath.Join("tmp", "node1")
		if got.FsDefaultDir != want {
			t.Fatalf("FsDefaultDir: got %q want %q", got.FsDefaultDir, want)
		}
	})
	t.Run("fsdb dir flag override", func(t *testing.T) {
		t.Parallel()
		fs := flag.NewFlagSet(t.Name(), flag.ContinueOnError)
		got, err := cfg.ParseNodeFlags(fs, []string{"-id", "node1", "-fs-default-dir", "/data/kvs"})
		if err != nil {
			t.Fatal(err)
		}
		if got.FsDefaultDir != "/data/kvs" {
			t.Fatalf("FsDefaultDir: got %q", got.FsDefaultDir)
		}
	})
}
