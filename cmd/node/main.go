package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/IsaacDSC/kvs/internal/api"
	"github.com/IsaacDSC/kvs/internal/code"
	"github.com/IsaacDSC/kvs/internal/commands"
	"github.com/IsaacDSC/kvs/internal/db"
	"github.com/IsaacDSC/kvs/internal/fsdb"
	"github.com/IsaacDSC/kvs/internal/memdb"
	"github.com/IsaacDSC/kvs/internal/raft"
	"github.com/IsaacDSC/kvs/internal/raftpb"
	"github.com/IsaacDSC/kvs/internal/store"
	"github.com/IsaacDSC/kvs/internal/wal"
	"github.com/IsaacDSC/kvs/pkg/www"
	"google.golang.org/grpc"
)

const (
	defaultDir     = "tmp"
	defaultDataDir = "tmp/data.wal"
)

// go run cmd/node/main.go -id 1 -addrs 127.0.0.1:8080 -peers 127.0.0.1:8081,127.0.0.1:8082
// go run cmd/node/main.go -id 2 -addrs 127.0.0.1:8081 -peers 127.0.0.1:8080,127.0.0.1:8082
// go run cmd/node/main.go -id 3 -addrs 127.0.0.1:8082 -peers 127.0.0.1:8080,127.0.0.1:8081
func main() {
	// id
	id := flag.String("id", "", "node id")
	// httpAddr
	httpAddr := flag.String("http-addr", "", "node addrs")
	// peers
	peersRaw := flag.String("peers", "", "node peers")
	// grpc addr
	grpcAddr := flag.String("grpc-addr", ":9080", "gRPC listen address for node-to-node RPCs (e.g. :9001)")
	memdbMaxEntries := flag.Int("memdb-max-entries", 0, "max entries per in-memory table (0=unlimited); LRU eviction applies when set")

	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})).With("node", *id)

	logger.Info("starting node", "id", *id, "addrs", *httpAddr, "peers", *peersRaw)

	if *id == "" {
		slog.Error("flag -id is required")
		os.Exit(1)
	}

	peers, err := NewPeers(*peersRaw)
	if err != nil {
		logger.Error("failed to parse peers", "error", err)
		os.Exit(1)
	}

	clusterMode := "multi-node"
	if len(peers) == 0 {
		clusterMode = "single-node"
	}

	transport := raft.NewTransport()
	node := raft.NewNode(*id, peers, transport, logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := os.MkdirAll(defaultDir, 0o755); err != nil {
		logger.Error("failed to create data dir", "error", err)
		os.Exit(1)
	}

	codec := code.NewCBOR()

	wal, err := wal.New(defaultDataDir, wal.Options{Durability: wal.SyncEveryWrite}, codec)
	if err != nil {
		panic(err)
	}

	// Start Database
	db := db.New(memdb.NewDB(memdb.Options{MaxEntriesPerTable: *memdbMaxEntries}), fsdb.NewDb(store.DefaultDataDir), wal)
	defer db.Close()

	//  Read the WAL and apply the operations to the database
	if err := db.Load(ctx); err != nil {
		panic(err)
	}

	// Start GRPC Server
	grpcSrv := grpc.NewServer()
	grpcSrv.RegisterService(&raftpb.ServiceDesc, raft.NewGRPCServer(node))

	grpcLis, err := net.Listen("tcp", *grpcAddr)
	if err != nil {
		logger.Error("gRPC listen failed", "addr", *grpcAddr, "err", err)
		os.Exit(1)
	}

	go func() {
		logger.Info("gRPC listening", "addr", *grpcAddr)
		if err := grpcSrv.Serve(grpcLis); err != nil {
			logger.Error("gRPC server error", "err", err)
			stop()
		}
	}()

	// Start HTTP Server
	mux := http.NewServeMux()

	routes := []www.Handler{
		api.CreateTableHandler(db, node),
		api.PutHandler(db, node),
		api.DeleteHandler(db, node),
		api.GetHandler(db),
		api.GetBySecondaryKeyHandler(db),
		api.StateHandler(node),
	}

	for _, r := range routes {
		mux.HandleFunc(r.Pattern, r.Fn)
	}

	httpSrv := &http.Server{Addr: *httpAddr, Handler: mux}

	go func() {
		logger.Info("HTTP listening", "addr", *httpAddr, "peers", peers, "cluster-mode", clusterMode)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", "err", err)
			stop()
		}
	}()

	// Start Raft Node State Machine
	go node.Run(ctx)

	go func() {
		for {
			select {
			case entry := <-node.Applied():
				logger.Info("applied entry", "command", entry.Data.Cmd, "term", entry.Term, "state", node.State())

				if node.State().Role == raft.Follower.String() {
					switch entry.Data.Cmd {
					case commands.CreateTableCmd:
						if err := db.CreateTable(entry.Data.TableName); err != nil {
							logger.Error("failed to create table", "error", err)
							os.Exit(1)
						}
					case commands.SetCmd:
						if err := db.Set(ctx, entry.Data.TableName, entry.Data.Item); err != nil {
							logger.Error("failed to set item", "error", err)
							os.Exit(1)
						}
					case commands.DeleteCmd:
						if err := db.Delete(ctx, entry.Data.TableName, entry.Data.Item.Key); err != nil {
							logger.Error("failed to delete item", "error", err)
							os.Exit(1)
						}

					}
				}

			case <-ctx.Done():
				return
			}
		}
	}()

	//  Shutdown the node
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

	transport.Close()
}

type Peers []string

func NewPeers(raw string) (Peers, error) {
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}

	return out, nil
}
