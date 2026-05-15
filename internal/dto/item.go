package dto

import (
	"errors"
	"strings"

	"github.com/IsaacDSC/kvs/internal/item"
)

type Item struct {
	Key     string         `json:"key"`
	SK      string         `json:"sk"`
	Value   map[string]any `json:"value"`
	Version *Version       `json:"version"`
}

type Version struct {
	PromoteVersion string `json:"propose_version"`
	OldVersion     string `json:"old_version"`
}

func (i Item) Entity() item.Entity {
	ent := item.Entity{
		Key:   i.Key,
		Value: i.Value,
		SK:    i.SK,
	}
	if i.Version != nil {
		ent.Version = i.Version.PromoteVersion
	}
	return ent
}

func (i Item) Validate() *FieldError {
	output := NewFieldError()
	if i.Version != nil {
		if strings.Trim(i.Version.PromoteVersion, "") == "" {
			output.AddErr(errors.New("if use optimistic_lock is required propose_version"))
		}
	}

	if i.Key == "" {
		output.AddErr(errors.New("is required key"))
	}

	if len(i.Value) == 0 {
		output.AddErr(errors.New("is required value to persistence"))
	}

	return output.Build()
}
