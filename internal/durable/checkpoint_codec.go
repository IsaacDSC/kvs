package durable

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/IsaacDSC/kvs/internal/cfg"
	"github.com/fxamacker/cbor/v2"
)

type checkpointFile struct {
	Version int                  `cbor:"v"`
	LastSeq uint64               `cbor:"seq"`
	Tables  map[string]tableSnap `cbor:"t"`
}

type tableSnap struct {
	Data map[string][]byte   `cbor:"d"`
	Fk   map[string][]string `cbor:"fk"`
}

// parseCheckpointDir reads and decodes checkpoint.cbor. A missing file yields a zero
// checkpointFile and a nil error (LastSeq 0, no tables).
func parseCheckpointDir(dir string) (checkpointFile, error) {
	conf := cfg.Get()
	path := filepath.Join(dir, conf.CheckpointFileName)
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return checkpointFile{}, nil
		}
		return checkpointFile{}, err
	}
	var cf checkpointFile
	if err := cbor.Unmarshal(raw, &cf); err != nil {
		return checkpointFile{}, fmt.Errorf("durable: decode checkpoint: %w", err)
	}
	if cf.Version != 1 {
		return checkpointFile{}, fmt.Errorf("durable: unsupported checkpoint version %d", cf.Version)
	}
	return cf, nil
}

func writeCheckpointAtomic(dir string, cf checkpointFile) error {
	conf := cfg.Get()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("durable: mkdir checkpoint dir: %w", err)
	}
	raw, err := cbor.Marshal(cf)
	if err != nil {
		return err
	}
	path := filepath.Join(dir, conf.CheckpointFileName)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func copyBytesMap(m map[string][]byte) map[string][]byte {
	if m == nil {
		return nil
	}
	out := make(map[string][]byte, len(m))
	for k, v := range m {
		vv := make([]byte, len(v))
		copy(vv, v)
		out[k] = vv
	}
	return out
}
