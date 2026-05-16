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
	"github.com/IsaacDSC/kvs/internal/commands"
	"github.com/IsaacDSC/kvs/internal/db"
	"github.com/IsaacDSC/kvs/internal/durable"
	"github.com/IsaacDSC/kvs/internal/fsdb"
	"github.com/IsaacDSC/kvs/internal/raft"
	"github.com/IsaacDSC/kvs/internal/raftpb"
	"github.com/IsaacDSC/kvs/internal/tasks"
	"github.com/IsaacDSC/kvs/internal/wal"
	"github.com/IsaacDSC/kvs/pkg/www"
	"google.golang.org/grpc"
)

const defaultDir = "tmp"

// Cluster de 3 nós (um terminal por comando; espelha make run1 / run2 / run3):
//
//	go run ./cmd/node/main.go -id node1 -grpc-addr :9001 -http-addr :8001 -peers localhost:9002,localhost:9003
//	go run ./cmd/node/main.go -id node2 -grpc-addr :9002 -http-addr :8002 -peers localhost:9001,localhost:9003
//	go run ./cmd/node/main.go -id node3 -grpc-addr :9003 -http-addr :8003 -peers localhost:9001,localhost:9002
//
// Checkpoint WAL (LastSeq) periódico: ver CHECKPOINT_INTERVAL em .env / env (default 5m; 0 desliga).
// Flush do batcher fsdb: FS_FLUSH_INTERVAL; FS_PERIODIC_POLL_INTERVAL (gatilho por tamanho, min 100ms); FS_FLUSH_OP_TIMEOUT (min 1s).
// Estado KV: apenas WAL + fsdb (persistência em disco sob -fs-default-dir).
func main() {
	nodeFlags, err := cfg.ParseNodeFlags(flag.CommandLine, os.Args[1:])
	if err != nil {
		slog.Error("invalid flags", "error", err)
		os.Exit(1)
	}

	if err := cfg.Load(); err != nil {
		panic(err)
	}

	nodeCfg := cfg.Get()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})).With("node", nodeFlags.ID)

	logger.Info("starting node", "id", nodeFlags.ID, "addrs", nodeFlags.HTTPAddr, "peers", nodeFlags.Peers, "fsdb", nodeFlags.FsDefaultDir)

	clusterMode := "multi-node"
	if len(nodeFlags.Peers) == 0 {
		clusterMode = "single-node"
	}

	transport := raft.NewTransport()
	node := raft.NewNode(nodeFlags.ID, nodeFlags.Peers, transport, logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := os.MkdirAll(filepath.Dir(nodeFlags.WALPath), 0o755); err != nil {
		logger.Error("failed to create wal parent dir", "error", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(defaultDir, 0o755); err != nil {
		logger.Error("failed to create data dir", "error", err)
		os.Exit(1)
	}

	codec := code.NewCBOR()

	// Filesystem writes go through WriteBatcher before WAL construction so Load can flush
	// deferred state before post-recovery Checkpoint (LastSeq must not advance ahead of disk).
	rawFS := fsdb.NewDb(nodeFlags.FsDefaultDir)
	batchOpts := fsdb.WriteBatcherOptions{}
	if nodeCfg.FSDeferWrites {
		batchOpts.DeferWrites = true
	}
	batchedFS := fsdb.NewWriteBatcher(rawFS, batchOpts)
	defer batchedFS.Stop()

	wal, err := wal.New(nodeFlags.WALPath, wal.Options{
		Durability:      wal.SyncEveryWrite,
		CheckpointDir:   nodeFlags.CheckpointDefaultDir,
		CheckpointStore: durable.NewFileCheckpointStore(),
		BeforeCheckpoint: func(ckptCtx context.Context) error {
			return batchedFS.Flush(ckptCtx)
		},
	}, codec)
	if err != nil {
		panic(err)
	}

	database := db.New(batchedFS, wal)
	defer database.Close()

	//  Read the WAL and apply the operations to the database
	if err := database.Load(ctx); err != nil {
		panic(err)
	}

	if nodeCfg.CheckpointInterval > 0 {
		go tasks.RunPeriodicWALCheckpoint(ctx, logger, wal, nodeCfg.CheckpointInterval)
	}
	if nodeCfg.FSDeferWrites && nodeCfg.FSFlushInterval > 0 {
		maxKeys, maxBytes := batchedFS.DirtyFlushThresholds()
		go tasks.RunPeriodicFSFlush(ctx, logger, batchedFS, tasks.FSPeriodicFlushLimits{
			Interval:         nodeCfg.FSFlushInterval,
			MaxPendingKeys:   maxKeys,
			MaxPendingBytes:  maxBytes,
			PendingPollEvery: nodeCfg.FSPeriodicPoll,
			PerFlushTimeout:  nodeCfg.FSFlushOpTimeout,
		}, batchedFS)
	}

	// Start GRPC Server
	grpcSrv := grpc.NewServer()
	grpcSrv.RegisterService(&raftpb.ServiceDesc, raft.NewGRPCServer(node))

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

	// Start HTTP Server
	mux := http.NewServeMux()

	routes := []www.Handler{
		api.PingHandler(),
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

	httpSrv := &http.Server{Addr: nodeFlags.HTTPAddr, Handler: www.RequestLatency(logger)(mux)}

	go func() {
		logger.Info("HTTP listening", "addr", nodeFlags.HTTPAddr, "peers", nodeFlags.Peers, "cluster-mode", clusterMode)
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
					case commands.SetCmd, commands.OptimisticSetCmd:
						if err := database.Set(ctx, entry.Data.TableName, entry.Data.Item); err != nil {
							logger.Error("failed to set item", "error", err)
							os.Exit(1)
						}
					case commands.DeleteCmd, commands.OptimisticDelCmd:
						if err := database.Delete(ctx, entry.Data.TableName, entry.Data.Item.DelItem()); err != nil {
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
