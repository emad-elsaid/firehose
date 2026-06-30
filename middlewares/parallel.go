package middlewares

import (
	"context"
	"sync"

	fh "github.com/emad-elsaid/firehose"
)

// TaskRunner executes tasks asynchronously.
type TaskRunner interface {
	Run(task func())
}

// Parallel is a callback middleware that executes rules in parallel using a task runner.
type Parallel[I, O any] struct {
	Runner TaskRunner

	rule *fh.Rule[I, O]
}

// WrapCallback stores the rule and returns the parallel callback function.
func (s *Parallel[I, O]) WrapCallback(
	_ context.Context,
	rule *fh.Rule[I, O],
	_ fh.Callback[I],
	_ I,
) (fh.Callback[I], error) {
	s.rule = rule

	return s.callback, nil
}

// WrapAction passes through the action unchanged.
func (s *Parallel[I, O]) WrapAction(_ context.Context, _ *fh.Rule[I, O], action fh.Action[I, O], _ I) (fh.Action[I, O], error) {
	return action, nil
}

// WrapDestination passes through the destination unchanged.
func (s *Parallel[I, O]) WrapDestination(_ context.Context, _ *fh.Rule[I, O], destination fh.Destination[O], _ O) (fh.Destination[O], error) {
	return destination, nil
}

func (s Parallel[I, O]) callback(ctx context.Context, event I, report fh.ReportFunc) {
	syms := fh.EventSymbols(event)

	var waitGroup sync.WaitGroup
	var mutex sync.Mutex

	safeReport := func(r fh.Report) {
		mutex.Lock()
		defer mutex.Unlock()
		report(r)
	}

	for current := fh.Runnable[I](s.rule); current != nil; current = current.NextRunnable() {
		waitGroup.Add(1)

		s.Runner.Run(func() {
			defer waitGroup.Done()

			current.Run(ctx, event, syms, safeReport)
		})
	}

	waitGroup.Wait()
}
