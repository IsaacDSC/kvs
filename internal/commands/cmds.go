package commands

type Commands string

const (
	CreateTableCmd   Commands = "create_table"
	OptimisticSetCmd Commands = "optimistic_set_cmd"
	OptimisticDelCmd Commands = "optimistic_del_cmd"
	SetCmd           Commands = "set"
	BulkPutCmd       Commands = "bulk_put"
	BulkDelCmd       Commands = "bulk_del"
	GetCmd           Commands = "get"
	GetBySkCmd       Commands = "get_by_sk"
	DeleteCmd        Commands = "delete"
)
