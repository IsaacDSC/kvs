package dto

import (
	"errors"
	"strings"
)

type DeleteItem struct {
	Key     string   `json:"key" param:"key"`
	Version *Version `json:"version"`
}

func (d DeleteItem) Validate(operationType OperationType) *FieldError {
	output := NewFieldError()

	if strings.Trim(d.Key, "") == "" {
		output.AddErr(errors.New("field key is required"))
	}

	if operationType == OperationTypeOptimisticLock && d.Version == nil {
		output.AddErr(errors.New("field version is required when use optimistic_del"))
	}

	return output.Build()
}

func (d DeleteItem) Item() Item {
	return Item{
		Key:     d.Key,
		Version: d.Version,
	}
}
