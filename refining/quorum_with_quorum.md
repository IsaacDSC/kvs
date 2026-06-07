# Adicionar configurações de Quorum.
Este serviço que trabalhamos com uma aplicação de banco de dados 
distibruidos utilizando raft, precisamos ter algum tipo de configuração 
para setar o nível do Quorum.

### Utilizar config global por env 
    - Menor flexibilidade
    - Simplicidade na implementação


> Obs: considerar utilizar aqui a opção em configs de QUORUM
```go 
type Config struct {
	CacheTTL           time.Duration `env:"CACHE_TTL" env-default:"5m"`
	CacheMaxEntries    int           `env:"CACHE_MAX_ENTRIES" env-default:"1000" env-description:"max entries per in-memory table (0=unlimited); LRU eviction applies when set"`
	CheckpointInterval time.Duration `env:"CHECKPOINT_INTERVAL" env-default:"5m" env-description:"periodic WAL LastSeq metadata checkpoint (0 disables)"`
	FSDeferWrites      bool          `env:"FS_DEFER_WRITES" env-default:"false" env-description:"batch coalesced writes to fsdb (LWW); use FS_FLUSH_INTERVAL and/or shutdown flush — see fsdb.WriteBatcher"`
	FSFlushInterval    time.Duration `env:"FS_FLUSH_INTERVAL" env-default:"1m" env-description:"periodic flush of batched fsdb writes when FS_DEFER_WRITES (0 disables); should be ≤ CHECKPOINT_INTERVAL if both run"`
	FSPeriodicPoll     time.Duration `env:"FS_PERIODIC_POLL_INTERVAL" env-default:"1s" env-description:"with FS_DEFER_WRITES, interval to poll dirty-buffer size for early flush; 0 disables (see tasks.RunPeriodicFSFlush)"`
	FSFlushOpTimeout   time.Duration `env:"FS_FLUSH_OP_TIMEOUT" env-default:"30s" env-description:"per-call deadline for periodic fsdb Flush; 0 means no extra timeout beyond the loop context"`
	CheckpointFileName string        `env:"CHECKPOINT_FILE_NAME" env-default:"checkpoint.cbor"`
}
```

### Utilizar config via comando de escrita
    - Maior flexibilidade
    - Maior complexidade


> Obs: considerar utilizar aqui a opcão de passar mais de um quorum
```go
replicateNodes.ProposeCommand(commands.Data{
				Cmd:       cmd,
				TableName: params.TableName,
				Item:      it,
			})
```
