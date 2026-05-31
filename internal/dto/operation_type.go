package dto

type OperationType string

const (
	OperationTypeOptimisticLock  OperationType = "optimistic_lock"
	OperationTypeMultipleWritter OperationType = "multiple_writter"
	OperationTypeNormal          OperationType = "normal"
)
