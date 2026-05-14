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
	"github.com/IsaacDSC/kvs/internal/tasks"
	"github.com/IsaacDSC/kvs/internal/wal"
	"github.com/IsaacDSC/kvs/pkg/www"
	"google.golang.org/grpc"
)

const (
	defaultDir           = "tmp"
	defaultDataDir       = "tmp/data.wal"
	defaultCheckpointDir = "tmp/checkpoint"
)

// Cluster de 3 nós (um terminal por comando; espelha make run1 / run2 / run3):
//
//	go run ./cmd/node/main.go -id node1 -grpc-addr :9001 -http-addr :8001 -peers localhost:9002,localhost:9003
//	go run ./cmd/node/main.go -id node2 -grpc-addr :9002 -http-addr :8002 -peers localhost:9001,localhost:9003
//	go run ./cmd/node/main.go -id node3 -grpc-addr :9003 -http-addr :8003 -peers localhost:9001,localhost:9002
//
// Checkpoint WAL (LastSeq) periódico: default 5m; desligar com -checkpoint-interval=0 (ou make … CHECKPOINT_INTERVAL=0).
// Flush do batcher fsdb é tarefa separada: -fs-flush-interval (relevante com -fs-defer-writes).
// Memdb LRU: default -memdb-max-entries=1000 (0=ilimitado); make run* usa 2000 via Makefile.
func main() {
	// id
	id := flag.String("id", "", "node id")
	// httpAddr
	httpAddr := flag.String("http-addr", "", "node addrs")
	// peers
	peersRaw := flag.String("peers", "", "node peers")
	// grpc addr
	grpcAddr := flag.String("grpc-addr", ":9080", "gRPC listen address for node-to-node RPCs (e.g. :9001)")
	memdbMaxEntries := flag.Int("memdb-max-entries", 1000, "max entries per in-memory table (0=unlimited); LRU eviction applies when set")
	checkpointEvery := flag.Duration("checkpoint-interval", 5*time.Minute, "periodic WAL LastSeq metadata checkpoint (0 disables)")
	fsDeferWrites := flag.Bool("fs-defer-writes", false, "batch coalesced writes to fsdb (LWW); use -fs-flush-interval and/or shutdown flush — see fsdb.WriteBatcher")
	fsFlushEvery := flag.Duration("fs-flush-interval", time.Minute, "periodic flush of batched fsdb writes when -fs-defer-writes (0 disables); should be ≤ checkpoint-interval if both run")

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

	wal, err := wal.New(defaultDataDir, wal.Options{
		Durability: wal.SyncEveryWrite,
		Checkpoint: wal.CheckpointConfig{Dir: defaultCheckpointDir},
	}, codec)
	if err != nil {
		panic(err)
	}

	// Start Database (filesystem writes go through WriteBatcher: LWW coalesce + optional defer; default is sync flush per op)
	rawFS := fsdb.NewDb(store.DefaultDataDir)
	batchOpts := fsdb.WriteBatcherOptions{}
	if *fsDeferWrites {
		batchOpts.DeferWrites = true
	}
	batchedFS := fsdb.NewWriteBatcher(rawFS, batchOpts)
	defer batchedFS.Stop()

	database := db.New(memdb.NewDB(memdb.Options{MaxEntriesPerTable: *memdbMaxEntries}), batchedFS, wal)
	defer database.Close()

	//  Read the WAL and apply the operations to the database
	if err := database.Load(ctx); err != nil {
		panic(err)
	}

	if *checkpointEvery > 0 {
		go tasks.RunPeriodicWALCheckpoint(ctx, logger, wal, *checkpointEvery)
	}
	if *fsDeferWrites && *fsFlushEvery > 0 {
		go tasks.RunPeriodicFSFlush(ctx, logger, batchedFS, *fsFlushEvery)
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
		api.CreateTableHandler(database, node),
		api.PutHandler(database, node),
		api.DeleteHandler(database, node),
		api.GetHandler(database),
		api.GetBySecondaryKeyHandler(database),
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
						if err := database.CreateTable(entry.Data.TableName); err != nil {
							logger.Error("failed to create table", "error", err)
							os.Exit(1)
						}
					case commands.SetCmd:
						if err := database.Set(ctx, entry.Data.TableName, entry.Data.Item); err != nil {
							logger.Error("failed to set item", "error", err)
							os.Exit(1)
						}
					case commands.DeleteCmd:
						if err := database.Delete(ctx, entry.Data.TableName, entry.Data.Item.Key); err != nil {
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
