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

type DeleteItems []DeleteItem

// Validate rejects empty batches and items without a key. Version is ignored on
// the bulk path (no optimistic lock per item; see specs/bulk-delete.md).
func (ds DeleteItems) Validate() *FieldError {
	output := NewFieldError()

	if len(ds) == 0 {
		output.AddErr(errors.New("bulk delete requires at least one item"))
		return output.Build()
	}

	for _, d := range ds {
		if strings.TrimSpace(d.Key) == "" {
			output.AddErr(errors.New("field key is required"))
		}
	}

	return output.Build()
}

// Items converts the batch for transport in the Raft log (commands.Data.Items).
func (ds DeleteItems) Items() Items {
	out := make(Items, len(ds))
	for i, d := range ds {
		out[i] = d.Item()
	}
	return out
}

// Keys extracts the primary keys for the WAL/fsdb delete path.
func (ds DeleteItems) Keys() []string {
	out := make([]string, len(ds))
	for i, d := range ds {
		out[i] = d.Key
	}
	return out
}
