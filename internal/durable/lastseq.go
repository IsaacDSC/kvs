package durable

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/fxamacker/cbor/v2"
)

// LoadLastSeq reads checkpoint.cbor in dir and returns the stored LastSeq without
// loading table snapshots. Missing file returns (0, nil).
func LoadLastSeq(dir string) (uint64, error) {
	path := filepath.Join(dir, CheckpointFileName)
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	var cf checkpointFile
	if err := cbor.Unmarshal(raw, &cf); err != nil {
		return 0, fmt.Errorf("durable: decode checkpoint: %w", err)
	}
	if cf.Version != 1 {
		return 0, fmt.Errorf("durable: unsupported checkpoint version %d", cf.Version)
	}
	return cf.LastSeq, nil
}

// SaveLastSeq writes only version and LastSeq (no table snapshot) atomically.
// Use after durable state on disk (e.g. fsdb) reflects all mutations through seq.
func SaveLastSeq(dir string, seq uint64) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("durable: mkdir checkpoint dir: %w", err)
	}
	cf := checkpointFile{
		Version: 1,
		LastSeq: seq,
		Tables:  nil,
	}
	raw, err := cbor.Marshal(cf)
	if err != nil {
		return err
	}
	path := filepath.Join(dir, CheckpointFileName)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
