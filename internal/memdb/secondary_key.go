package memdb

import (
	"slices"

	"github.com/IsaacDSC/kvs/internal/code"
	"github.com/IsaacDSC/kvs/internal/item"
)

type secondaryKey map[string]keySet

type keySet []string

func (s keySet) add(key string) keySet {
	if slices.Contains(s, key) {
		return s
	}
	return append(s, key)
}

func (s keySet) remove(key string) keySet {
	return slices.DeleteFunc(s, func(s string) bool {
		return s == key
	})
}

func (s secondaryKey) add(key, sk string) {
	if sk == "" {
		return
	}
	s[sk] = s[sk].add(key)
}

func (s secondaryKey) remove(key, sk string) {
	if sk == "" {
		return
	}
	set := s[sk]
	if len(set) == 0 {
		return
	}
	if len(set) == 1 && set[0] == key {
		delete(s, sk)
		return
	}
	s[sk] = set.remove(key)
}

func (s secondaryKey) removePreviousIfChanged(key string, oldB []byte, newSK string) {
	var old item.Entity
	if err := code.Decode(oldB, &old); err != nil {
		return
	}
	if old.SK == "" || old.SK == newSK {
		return
	}
	s.remove(key, old.SK)
}
