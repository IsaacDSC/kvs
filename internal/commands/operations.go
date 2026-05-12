package commands

import (
	"context"

	"github.com/IsaacDSC/kvs/internal/item"
)

type Operations interface {
	CreateTable(tableName string) error
	Set(ctx context.Context, tableName string, entity item.Entity) error
	Del(ctx context.Context, tableName string, key string) error
}
