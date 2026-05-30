package commands

import (
	"fmt"

	"github.com/IsaacDSC/kvs/internal/dto"
)

type Data struct {
	Cmd       Commands `json:"cmd"`
	TableName string   `json:"table_name"`
	Item      dto.Item `json:"item"` // optional
	// MinAcks quorum required to commit THIS log entry (HTTP ?raft_min_acks / JSON raft_min_acks).
	// Omit or zero is normalized at the HTTP boundary to replication by every member (N); other
	// ProposeCommand callers that leave MinAcks at 0 defer to Raft (effective default = full cluster).
	MinAcks int `json:"min_acks,omitempty"`
}

type Database interface {
	CreateTable(table string) error
}

func (d *Data) Execute(db Database) error {
	// register create table in WAL
	switch d.Cmd {
	case CreateTableCmd:
		return db.CreateTable(d.TableName)
	default:
		return fmt.Errorf("unsupported command: %s", d.Cmd)
	}
}
