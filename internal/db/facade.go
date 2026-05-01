package db

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/IsaacDSC/kvs/internal/fsdb"
	"github.com/IsaacDSC/kvs/internal/memdb"
	"github.com/IsaacDSC/kvs/internal/wal"
)

const defaultDataDir = "tmp"

type Facade struct {
	mu sync.Mutex

	dir  string
	opts wal.Options

	memdb *memdb.DB
	fsdb  *fsdb.Db

	wal *wal.WAL
}

func New(memdb *memdb.DB, fsdb *fsdb.Db) (*Facade, error) {
	f := &Facade{
		dir:   defaultDataDir,
		opts:  wal.Options{Durability: wal.SyncEveryWrite},
		memdb: memdb,
		fsdb:  fsdb,
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

// TODO: talvez vale ter um method somente para criar se não existir sendo safe
func (f *Facade) CreateTable(table string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := f.fsdb.CreateTable(table); err != nil {
		return err
	}

	f.memdb.GetOrCreateTable(table)

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
