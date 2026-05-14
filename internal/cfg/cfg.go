// Package cfg loads node CLI flags ([ParseNodeFlags]) and runtime tuning from the
// process environment using [github.com/ilyakaznacheev/cleanenv].
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
	FSPeriodicPoll     time.Duration `env:"FS_PERIODIC_POLL_INTERVAL" env-default:"1s" env-description:"with FS_DEFER_WRITES, interval to poll dirty-buffer size for early flush; 0 disables (see tasks.RunPeriodicFSFlush)"`
	FSFlushOpTimeout   time.Duration `env:"FS_FLUSH_OP_TIMEOUT" env-default:"30s" env-description:"per-call deadline for periodic fsdb Flush; 0 means no extra timeout beyond the loop context"`
	CheckpointFileName string        `env:"CHECKPOINT_FILE_NAME" env-default:"checkpoint.cbor"`
}

var c Config

// Load reads process environment variables (and env-default tags on [Config]).
func Load() error {
	if err := cleanenv.ReadEnv(&c); err != nil {
		return fmt.Errorf("cfg: read environment: %w", err)
	}
	return nil
}

func Get() Config {
	return c
}

// LoadFromFile reads tuning fields from a dotenv, YAML, or TOML file (by extension).
// It is intended for tests and tooling; the node binary uses [Load] with the environment.
func LoadFromFile(path string) (Config, error) {
	var out Config
	if err := cleanenv.ReadConfig(path, &out); err != nil {
		return Config{}, fmt.Errorf("cfg: read config file %q: %w", path, err)
	}
	return out, nil
}
