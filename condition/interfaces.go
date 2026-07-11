// Package condition provides condition implementations for rule evaluation.
package condition

import (
	"context"
	"time"
)

// CacheStorage is an interface for storing and retrieving cached values.
type CacheStorage[V any] interface {
	Get(ctx context.Context, key string) (value V, ok bool, err error)
	Set(ctx context.Context, key string, ttl time.Duration, value V) error
}
