package commands

import (
	"fmt"

	"github.com/IsaacDSC/kvs/internal/dto"
)

type Data struct {
	Cmd       Commands `json:"cmd"`
	TableName string   `json:"table_name"`
	Item      dto.Item `json:"item"` // optional
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
