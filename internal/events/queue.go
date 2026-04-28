package events

import (
	"context"
	"sync"
	"time"

	"github.com/IsaacDSC/kvs/internal/item"
)

type Op string

const (
	OpPut Op = "put"
	OpDel Op = "del"
)

type Payload struct {
	Item item.Entity
	Op   Op
}

// MemQueue buffers pending payloads por chave. Operações na mesma chave são
// serializadas por um mutex dedicado; chaves diferentes não bloqueiam umas às outras.
type MemQueue struct {
	queue    sync.Map // string -> Payload
	keyLocks sync.Map // string -> *sync.Mutex
}

func NewMemQueue() *MemQueue {
	return &MemQueue{}
}

// getKeyLock devolve o mutex da chave, criando um novo só quando ainda não existe.
func (q *MemQueue) getKeyLock(key string) *sync.Mutex {
	if v, ok := q.keyLocks.Load(key); ok {
		return v.(*sync.Mutex)
	}
	mu := new(sync.Mutex)
	if v, loaded := q.keyLocks.LoadOrStore(key, mu); loaded {
		return v.(*sync.Mutex)
	}
	return mu
}

func (q *MemQueue) AsyncPut(item item.Entity) {
	mu := q.getKeyLock(item.Key)
	mu.Lock()
	defer mu.Unlock()
	q.queue.Store(item.Key, Payload{
		Item: item,
		Op:   OpPut,
	})
}

func (q *MemQueue) AsyncDel(item item.Entity) {
	mu := q.getKeyLock(item.Key)
	mu.Lock()
	defer mu.Unlock()
	q.queue.Store(item.Key, Payload{
		Item: item,
		Op:   OpDel,
	})
}

func (q *MemQueue) Consumer(ctx context.Context, fn func(ctx context.Context, payload Payload) error) {
	for {
		q.queue.Range(func(key, value interface{}) bool {
			k := key.(string)
			func() {
				mu := q.getKeyLock(k)
				mu.Lock()
				defer mu.Unlock()

				v, ok := q.queue.Load(k)
				if !ok {
					return
				}
				payload := v.(Payload)
				if err := fn(ctx, payload); err != nil {
					return
				}
				q.queue.Delete(k)
				q.keyLocks.Delete(k)
			}()
			return true
		})

		time.Sleep(1 * time.Second)
	}
}
