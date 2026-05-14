// Package cfg loads node runtime tuning from a dotenv file and/or the process
// environment using [github.com/ilyakaznacheev/cleanenv].
package cfg

import (
	"fmt"
	"time"

	"github.com/ilyakaznacheev/cleanenv"
)

// Config holds memdb, WAL checkpoint, and fsdb batcher settings (formerly CLI flags).
type Config struct {
	MemDBMaxEntries    int           `env:"MEMDB_MAX_ENTRIES" env-default:"1000" env-description:"max entries per in-memory table (0=unlimited); LRU eviction applies when set"`
	CheckpointInterval time.Duration `env:"CHECKPOINT_INTERVAL" env-default:"5m" env-description:"periodic WAL LastSeq metadata checkpoint (0 disables)"`
	FSDeferWrites      bool          `env:"FS_DEFER_WRITES" env-default:"false" env-description:"batch coalesced writes to fsdb (LWW); use FS_FLUSH_INTERVAL and/or shutdown flush — see fsdb.WriteBatcher"`
	FSFlushInterval    time.Duration `env:"FS_FLUSH_INTERVAL" env-default:"1m" env-description:"periodic flush of batched fsdb writes when FS_DEFER_WRITES (0 disables); should be ≤ CHECKPOINT_INTERVAL if both run"`
}

var c Config

// Load reads defaultEnvFile from the working directory when present; otherwise
// only environment variables and env-default tags apply.
func Load() error {
	if err := cleanenv.ReadEnv(&c); err != nil {
		return fmt.Errorf("cfg: read environment: %w", err)
	}

	return nil
}

func Get() Config {
	return c
}
