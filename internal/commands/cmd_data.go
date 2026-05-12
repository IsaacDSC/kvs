package commands

import (
	"fmt"

	"github.com/IsaacDSC/kvs/internal/item"
)

type Data struct {
	Cmd       Commands    `json:"cmd"`
	TableName string      `json:"table_name"`
	Item      item.Entity `json:"item,omitempty"` // optional
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
