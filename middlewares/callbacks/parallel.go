package callbacks

import (
	"context"
	"fmt"
	"sync"

	"github.com/emad-elsaid/boolexpr"
	fh "github.com/emad-elsaid/firehose"
)

// TaskRunner executes tasks asynchronously.
type TaskRunner interface {
	Run(task func())
}

// Parallel is a callback middleware that executes rules in parallel using a task runner.
type Parallel[I, O fh.Event] struct {
	Runner TaskRunner

	rule *fh.Rule[I, O]
}

// Wrap stores the rule and returns the parallel callback function.
func (s *Parallel[I, O]) Wrap(
	_ context.Context,
	rule *fh.Rule[I, O],
	_ fh.Callback[I],
	_ I,
) (fh.Callback[I], error) {
	s.rule = rule

	return s.callback, nil
}

func (s Parallel[I, O]) callback(ctx context.Context, event I, reports chan<- fh.Report) {
	attrs, err := fh.EventAttributes(ctx, event)
	if err != nil {
		reports <- fh.NewRuleReport(s.rule.ID, fh.StatusError, fmt.Errorf("failed to get event attributes: %w", err))

		return
	}

	syms := boolexpr.NewSymbolsCached(attrs)

	var waitGroup sync.WaitGroup

	for current := fh.Runnable[I](s.rule); current != nil; current = current.NextRunnable() {
		waitGroup.Add(1)

		s.Runner.Run(func() {
			defer waitGroup.Done()

			current.Run(ctx, event, syms, reports)
		})
	}

	waitGroup.Wait()
}
