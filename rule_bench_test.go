package firehose

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/emad-elsaid/boolexpr"
)

// Global sink to prevent compiler optimizations from eliminating benchmark work
var (
	benchSinkEvent  *EventMock
	benchSinkReport error
	benchEventCount atomic.Uint64
)

// Action that does minimal work but prevents optimization
type benchAction struct{}

func (a benchAction) Process(ctx context.Context, event *EventMock, syms boolexpr.Symbols) (*EventMock, error) {
	// Write to global sink to prevent dead code elimination
	benchSinkEvent = event
	return event, nil
}

// Destination that counts events to prevent optimization
type benchDestination struct{}

func (d benchDestination) Send(ctx context.Context, event *EventMock) error {
	// Atomic increment prevents compiler from optimizing away the call
	benchEventCount.Add(1)
	// report := nil
	// // benchSinkReport = report
	return nil
}

// benchSource is a simple manual source for benchmarking that allows controlled event emission
type benchSource struct {
	mutex    sync.RWMutex
	callback Callback[*EventMock]
}

func (s *benchSource) Start(ctx context.Context, cb Callback[*EventMock]) (<-chan struct{}, error) {
	s.mutex.Lock()
	s.callback = cb
	s.mutex.Unlock()
	done := make(chan struct{})
	return done, nil
}

func (s *benchSource) Emit(ctx context.Context, event *EventMock, report ErrorHandler) {
	s.mutex.RLock()
	callback := s.callback
	s.mutex.RUnlock()

	if callback != nil {
		callback(ctx, event, report)
	}
}

// BenchmarkEngine_SingleRule measures event processing throughput with a single rule.
// This represents the baseline overhead of the engine with one source/action/destination.
func BenchmarkEngine_SingleRule(b *testing.B) {
	ctx := context.Background()
	source := &benchSource{}

	rule := &MockSQLRule{
		ID:     "bench-rule",
		Select: benchAction{},
		From:   source,
		Into:   benchDestination{},
	}

	head, err := Add(ctx, nil, rule)
	if err != nil {
		b.Fatalf("failed to add rule: %v", err)
	}

	Start(ctx, head, func(err error) {
		if err != nil {
			b.Errorf("start error: %v", err)
		}
	})

	event := NewEventMock(nil)

	// Reset counter before benchmark
	benchEventCount.Store(0)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		source.Emit(ctx, event, func(error) {})
	}

	b.StopTimer()

	// Verify work was actually done
	if count := benchEventCount.Load(); count != uint64(b.N) {
		b.Fatalf("expected %d events processed, got %d", b.N, count)
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "events/sec")
}

// BenchmarkEngine_100Rules measures event processing throughput with 100 rules
// sharing the same source. This tests the engine's ability to handle multiple
// rules efficiently and reveals the overhead of rule chaining.
func BenchmarkEngine_100Rules(b *testing.B) {
	ctx := context.Background()
	source := &benchSource{}

	var head Rule
	var err error

	// Create 100 rules sharing the same source
	for i := 0; i < 100; i++ {
		rule := &MockSQLRule{
			ID:     "bench-rule-" + string(rune('0'+i%10)),
			Select: benchAction{},
			From:   source,
			Into:   benchDestination{},
		}

		head, err = Add(ctx, head, rule)
		if err != nil {
			b.Fatalf("failed to add rule %d: %v", i, err)
		}
	}

	Start(ctx, head, func(err error) {
		if err != nil {
			b.Errorf("start error: %v", err)
		}
	})

	event := NewEventMock(nil)

	// Reset counter before benchmark
	benchEventCount.Store(0)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Each event will be processed by all 100 rules
		source.Emit(ctx, event, func(error) {})
	}

	b.StopTimer()

	// Verify all rules processed all events
	expected := uint64(b.N * 100)
	if count := benchEventCount.Load(); count != expected {
		b.Fatalf("expected %d total processing operations, got %d", expected, count)
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "events/sec")
}
