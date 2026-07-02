// Package ifs provides condition implementations for rule evaluation.
package ifs

import (
	"context"
	"time"

	"github.com/emad-elsaid/firehose"
)

// CacheStorage is an interface for storing and retrieving cached values.
type CacheStorage[V any] interface {
	Get(ctx context.Context, key string) (value V, report firehose.Report, ok bool)
	Set(ctx context.Context, key string, value V, report firehose.Report, ttl time.Duration) firehose.Report
}
