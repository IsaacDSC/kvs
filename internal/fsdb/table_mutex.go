package fsdb

import "sync"

type TbMutex map[string]*sync.RWMutex

func (m TbMutex) Lock(table string) {
	mu, ok := m[table]
	if !ok {
		mu = new(sync.RWMutex)
		m[table] = mu
	}
	mu.Lock()
}

func (m TbMutex) RLock(table string) {
	mu, ok := m[table]
	if !ok {
		mu = new(sync.RWMutex)
		m[table] = mu
	}
	mu.RLock()
}

func (m TbMutex) Unlock(table string) {
	m[table].Unlock()
}

func (m TbMutex) RUnlock(table string) {
	m[table].RUnlock()
}
