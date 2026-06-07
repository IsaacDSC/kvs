package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/IsaacDSC/kvs/internal/api"
	"github.com/IsaacDSC/kvs/internal/cfg"
	"github.com/IsaacDSC/kvs/internal/code"
	"github.com/IsaacDSC/kvs/internal/raft"
	"github.com/IsaacDSC/kvs/internal/raftpb"
	"github.com/IsaacDSC/kvs/pkg/www"
	"google.golang.org/grpc"

	"github.com/IsaacDSC/kvs/cmd/node/setup"
)

// Cluster de 3 nós (um terminal por comando; espelha make run1 / run2 / run3):
//
//	go run ./cmd/node/main.go -id node1 -grpc-addr :9001 -http-addr :8001 -peers localhost:9002,localhost:9003
//	go run ./cmd/node/main.go -id node2 -grpc-addr :9002 -http-addr :8002 -peers localhost:9001,localhost:9003
//	go run ./cmd/node/main.go -id node3 -grpc-addr :9003 -http-addr :8003 -peers localhost:9001,localhost:9002
func main() {
	nodeFlags, err := cfg.ParseNodeFlags(flag.CommandLine, os.Args[1:])
	if err != nil {
		slog.Error("invalid flags", "error", err)
		os.Exit(1)
	}

	if err := cfg.Load(); err != nil {
		panic(err)
	}
	conf := cfg.Get()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})).With("node", nodeFlags.ID)

	clusterMode := "multi-node"
	if len(nodeFlags.Peers) == 0 {
		clusterMode = "single-node"
	}
	logger.Info("starting node",
		"id", nodeFlags.ID,
		"http", nodeFlags.HTTPAddr,
		"grpc", nodeFlags.GRPCAddr,
		"peers", nodeFlags.Peers,
		"cluster", clusterMode,
		"fsdb", nodeFlags.FsDefaultDir,
	)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	codec := code.NewCBOR()

	// ── Raft ─────────────────────────────────────────────────────────────────
	transport := raft.NewTransport()
	defer transport.Close()

	raftNode, err := setup.OpenRaft(filepath.Dir(nodeFlags.WALPath), nodeFlags.ID, nodeFlags.Peers, transport, logger, codec)
	if err != nil {
		logger.Error("failed to open raft node", "error", err)
		os.Exit(1)
	}
	defer raftNode.Close()

	// ── KV database ───────────────────────────────────────────────────────────
	kvStore, err := setup.OpenKV(ctx, nodeFlags, conf, codec, logger)
	if err != nil {
		logger.Error("failed to open kv store", "error", err)
		os.Exit(1)
	}
	defer kvStore.Close()

	database := kvStore.DB()

	// ── gRPC server ───────────────────────────────────────────────────────────
	grpcSrv := grpc.NewServer()
	grpcSrv.RegisterService(&raftpb.ServiceDesc, raft.NewGRPCServer(raftNode.Node))

	grpcLis, err := net.Listen("tcp", nodeFlags.GRPCAddr)
	if err != nil {
		logger.Error("gRPC listen failed", "addr", nodeFlags.GRPCAddr, "err", err)
		os.Exit(1)
	}
	go func() {
		logger.Info("gRPC listening", "addr", nodeFlags.GRPCAddr)
		if err := grpcSrv.Serve(grpcLis); err != nil {
			logger.Error("gRPC server error", "err", err)
			stop()
		}
	}()

	// ── HTTP server ───────────────────────────────────────────────────────────
	mux := http.NewServeMux()
	for _, r := range setup.GetRoutes(database, raftNode.Node) {
		mux.HandleFunc(r.Pattern, www.HandlerHttp(r.Fn))
	}

	httpSrv := &http.Server{Addr: nodeFlags.HTTPAddr, Handler: www.RequestLatency(logger)(mux)}
	go func() {
		logger.Info("HTTP listening", "addr", nodeFlags.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", "err", err)
			stop()
		}
	}()

	// ── Raft state machine + applied-entry loop ───────────────────────────────
	go raftNode.Run(ctx)

	raftNode.RunAppliedLoop(ctx, api.GrpcHandle(logger, database, raftNode), func(err error) {
		logger.Error("applied-entry loop fatal error", "error", err)
		os.Exit(1)
	})

	// ── Graceful shutdown ─────────────────────────────────────────────────────
	<-ctx.Done()
	stop()

	grpcStopped := make(chan struct{})
	go func() {
		grpcSrv.GracefulStop()
		close(grpcStopped)
	}()
	select {
	case <-grpcStopped:
	case <-time.After(3 * time.Second):
		logger.Warn("gRPC graceful stop timed out; forcing stop")
		grpcSrv.Stop()
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		logger.Warn("HTTP shutdown error", "err", err)
	}
}
