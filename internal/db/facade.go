package db

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/IsaacDSC/kvs/internal/commands"
	"github.com/IsaacDSC/kvs/internal/fsdb"
	"github.com/IsaacDSC/kvs/internal/item"
	"github.com/IsaacDSC/kvs/internal/memdb"
	"github.com/IsaacDSC/kvs/internal/wal"
)

type ReplicateNodes interface {
	ProposeCommand(command commands.Data) error
}

const defaultDataDir = "tmp"

type Facade struct {
	mu sync.Mutex

	dir  string
	opts wal.Options

	memdb *memdb.DB
	fsdb  *fsdb.Db

	wal            *wal.WAL
	replicateNodes ReplicateNodes
}

func New(memdb *memdb.DB, fsdb *fsdb.Db, replicateNodes ReplicateNodes) (*Facade, error) {
	f := &Facade{
		dir:            defaultDataDir,
		opts:           wal.Options{Durability: wal.SyncEveryWrite},
		memdb:          memdb,
		fsdb:           fsdb,
		replicateNodes: replicateNodes,
	}
	if err := f.ensureDurableLocked(); err != nil {
		return nil, err
	}
	return f, nil
}

func (f *Facade) ensureDurableLocked() error {
	if err := os.MkdirAll(f.dir, 0o755); err != nil {
		return fmt.Errorf("db: create data dir: %w", err)
	}

	walPath := filepath.Join(f.dir, wal.WalFileName)
	_ = wal.RepairTruncatesTail(walPath)

	log, err := wal.Open(walPath, f.opts)
	if err != nil {
		return fmt.Errorf("db: open wal: %w", err)
	}

	f.wal = log
	return nil
}

func (f *Facade) CreateTable(table string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.fsdb.CreateTable(table); err != nil {
		return err
	}

	if err := f.memdb.CreateTable(table); err != nil {
		return err
	}

	if err := f.replicateNodes.ProposeCommand(commands.Data{
		Cmd:       commands.CreateTableCmd,
		TableName: table,
	}); err != nil {
		return fmt.Errorf("db: propose command: %w", err)
	}

	return nil
}

func (f *Facade) GetTable(tableName string) (*memdb.Table, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	table, err := f.memdb.GetTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("db: get table: %w", err)
	}

	return table, nil
}

func (f *Facade) Set(ctx context.Context, tableName string, entity item.Entity) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	table, err := f.memdb.GetTable(tableName)
	if err != nil {
		return fmt.Errorf("db: get table: %w", err)
	}

	if err := f.replicateNodes.ProposeCommand(commands.Data{
		Cmd:       commands.SetCmd,
		TableName: tableName,
		Item:      entity,
	}); err != nil {
		return fmt.Errorf("db: propose command: %w", err)
	}

	return table.Set(entity)
}

func (f *Facade) Get(ctx context.Context, tableName string, key string) (item.Entity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	table, err := f.memdb.GetTable(tableName)
	if err != nil {
		return item.Entity{}, fmt.Errorf("db: get table: %w", err)
	}

	return table.Get(key)
}

func (f *Facade) GetBySecondaryKey(ctx context.Context, tableName string, secondaryKey string) ([]item.Entity, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	table, err := f.memdb.GetTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("db: get table: %w", err)
	}

	return table.GetBySecondaryKey(secondaryKey)
}

func (f *Facade) Delete(ctx context.Context, tableName string, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	table, err := f.memdb.GetTable(tableName)
	if err != nil {
		return fmt.Errorf("db: get table: %w", err)
	}

	return table.Delete(key)
}

func (f *Facade) Load() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// ler tudo que esta no wal e aplicar no memdb e fsdb

	return nil
}

func (f *Facade) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.wal == nil {
		return nil
	}
	return f.wal.Close()
}
