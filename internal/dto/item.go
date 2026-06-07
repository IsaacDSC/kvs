package dto

import (
	"errors"
	"strings"

	"github.com/IsaacDSC/kvs/internal/item"
)

type Items []Item

func (is Items) Validate() *FieldError {
	for _, i := range is {
		if err := i.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (is Items) Entities() []item.Entity {
	output := make([]item.Entity, len(is))
	for idx, i := range is {
		output[idx] = i.Entity()
	}

	return output
}

type Item struct {
	Key     string         `json:"key"`
	SK      string         `json:"sk"`
	Value   map[string]any `json:"value"`
	Version *Version       `json:"version,omitempty"`
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

// DelItems converts items replicated through the Raft log back into the bulk
// delete flow (the inverse of DeleteItems.Items).
func (is Items) DelItems() DeleteItems {
	out := make(DeleteItems, len(is))
	for i, it := range is {
		out[i] = it.DelItem()
	}
	return out
}

func (i Item) DelItem() DeleteItem {
	return DeleteItem{
		Key:     i.Key,
		Version: i.Version,
	}
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
