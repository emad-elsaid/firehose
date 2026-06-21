package callbacks

import (
	"context"
	"fmt"
	"sync"

	"github.com/emad-elsaid/boolexpr"
	fh "github.com/emad-elsaid/firehose"
)

type TaskRunner interface {
	Run(func())
}

type Parallel[I, O fh.Event] struct {
	Runner TaskRunner

	rule *fh.Rule[I, O]
}

// Wrap stores the downstream callback to be wrapped with logging and returns
// the callback function to be used by the source.
func (s *Parallel[I, O]) Wrap(
	_ context.Context,
	rule *fh.Rule[I, O],
	callback fh.Callback[I],
	_ I,
) (fh.Callback[I], error) {
	s.rule = rule

	return s.callback, nil
}

func (s Parallel[I, O]) callback(ctx context.Context, event I, reports chan<- fh.Report) {
	attrs, err := fh.EventAttributes(ctx, event)
	if err != nil {
		reports <- fh.NewRuleReport(s.rule.Id, fh.StatusError, fmt.Errorf("failed to get event attributes: %w", err))

		return
	}

	syms := boolexpr.NewSymbolsCached(attrs)

	var wg sync.WaitGroup
	for current := fh.Runnable[I](s.rule); current != nil; current = current.NextRunnable() {
		wg.Add(1)

		s.Runner.Run(func() {
			defer wg.Done()

			current.Run(ctx, event, syms, reports)
		})
	}

	wg.Wait()
}
