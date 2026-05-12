package memdb

import "container/list"

type lruTracker struct {
	maxEntries int
	order      *list.List
	byKey      map[string]*list.Element
}

func newLRUTracker(maxEntries int) lruTracker {
	if maxEntries <= 0 {
		return lruTracker{}
	}
	return lruTracker{
		maxEntries: maxEntries,
		order:      list.New(),
		byKey:      make(map[string]*list.Element),
	}
}

func (l *lruTracker) markRecentlyUsed(key string) {
	if !l.enabled() {
		return
	}
	if el, ok := l.byKey[key]; ok {
		l.order.MoveToFront(el)
		return
	}
	l.byKey[key] = l.order.PushFront(key)
}

func (l *lruTracker) remove(key string) {
	if !l.enabled() {
		return
	}
	el, ok := l.byKey[key]
	if !ok {
		return
	}
	l.order.Remove(el)
	delete(l.byKey, key)
}

func (l *lruTracker) leastRecentlyUsed() (string, bool) {
	if !l.enabled() {
		return "", false
	}
	el := l.order.Back()
	if el == nil {
		return "", false
	}
	key, ok := el.Value.(string)
	return key, ok
}

func (l *lruTracker) maxExceeded(entries int) bool {
	return l.enabled() && entries > l.maxEntries
}

func (l *lruTracker) len() int {
	if !l.enabled() {
		return 0
	}
	return l.order.Len()
}

func (l *lruTracker) enabled() bool {
	return l.maxEntries > 0 && l.order != nil
}
