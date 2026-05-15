package dto

type OperationType string

const (
	OperationTypeOptimisticLock OperationType = "optimistic_lock"
	OperationTypeNormal         OperationType = "normal"
)
