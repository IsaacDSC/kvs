package commands

type Commands string

const (
	CreateTableCmd Commands = "create_table"
	SetCmd         Commands = "set"
	GetCmd         Commands = "get"
	GetBySkCmd     Commands = "get_by_sk"
	DeleteCmd      Commands = "delete"
)
