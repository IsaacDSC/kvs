package main

import (
	"container/list"
	"encoding/json"
	"fmt"
	"time"
)

// Stub: historical example logic is commented in version control.
// Keeps `go build ./...` succeeding for tooling.
func main() {
	err := Service()
	if err != nil {
		panic(err)
	}

	fmt.Println("Ok")
}

func Service() error {
	return nil
}

type Kv struct {
	Key   string
	Value []byte
}

type MemCache struct {
	db      map[string]*list.Element
	queue   *list.List
	maxSize int64
}

func NewMemCache(maxItem int64) *MemCache {
	return &MemCache{
		db:      make(map[string]*list.Element),
		queue:   list.New(),
		maxSize: maxItem,
	}
}

func (mc *MemCache) Get(key string, arg any) error {
	if elem, ok := mc.db[key]; ok {
		mc.queue.MoveToFront(elem)
		vb := elem.Value.(*Kv).Value
		if err := json.Unmarshal(vb, arg); err != nil {
			return err
		}

		return nil
	}

	return fmt.Errorf("not found element")
}

func (mc *MemCache) Set(key string, value any, ttl time.Duration) error {
	bv, err := json.Marshal(value)
	if err != nil {
		return err
	}

	if elem, ok := mc.db[key]; ok {
		mc.queue.MoveToFront(elem)
		elem.Value.(*Kv).Value = bv
		return nil
	}

	if mc.queue.Len() == int(mc.maxSize) {
		oldElem := mc.queue.Back()
		delete(mc.db, oldElem.Value.(*Kv).Key)
		mc.queue.Remove(oldElem)
	}

	elem := mc.queue.PushFront(&Kv{key, bv})
	mc.db[key] = elem

	return nil
}

func (mc *MemCache) Clear() {
	tricker := time.NewTicker(time.Minute * 1)
	for {
		select {
		case _ = <-tricker.C:

		}
	}
}
