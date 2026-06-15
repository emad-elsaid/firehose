package cache

import (
	"context"
	"time"

	fh "github.com/emad-elsaid/firehose"

	gocache "github.com/patrickmn/go-cache"
)

func NewMemory[O any](defaultTTL, cleanup time.Duration) Memory[O] {
	return Memory[O]{
		cache: gocache.New(defaultTTL, cleanup),
	}
}

type Memory[O any] struct {
	cache *gocache.Cache
}

type MemoryItem[O any] struct {
	Value  O
	Report fh.Report
}

func (m Memory[O]) Get(ctx context.Context, key string) (O, fh.Report, bool) {
	v, ok := m.cache.Get(key)
	if ok {
		item, ok := v.(MemoryItem[O])
		if ok {
			return item.Value, item.Report, true
		}
	}

	var zero O
	return zero, fh.NewReport(fh.StatusError, nil), false
}

func (m Memory[O]) Set(ctx context.Context, key string, value O, report fh.Report, ttl time.Duration) fh.Report {
	m.cache.Set(key, MemoryItem[O]{Value: value, Report: report}, ttl)

	return fh.NewReport(fh.StatusSuccess, nil)
}
func (m Memory[O]) GetOrSet(ctx context.Context, key string, ttl time.Duration, cb func() (O, fh.Report)) (O, fh.Report, bool) {
	v, report, ok := m.Get(ctx, key)
	if ok {
		return v, report, true
	}

	o, report := cb()
	m.Set(ctx, key, o, report, ttl)

	return o, report, false
}
