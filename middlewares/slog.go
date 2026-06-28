package middlewares

import (
	"context"
	"log/slog"
	"sync"

	fh "github.com/emad-elsaid/firehose"
)

// Slog is a callback middleware that logs the events and reports using Go
// structured logging.
type Slog[I, O any] struct {
	downstream fh.Callback[I]
	source     fh.Source[I]
}

// WrapCallback stores the downstream callback to be wrapped with logging and returns
// the callback function to be used by the source.
func (s *Slog[I, O]) WrapCallback(
	_ context.Context,
	rule *fh.Rule[I, O],
	callback fh.Callback[I],
	_ I,
) (fh.Callback[I], error) {
	s.downstream = callback
	s.source = rule.On

	return s.callback, nil
}

// WrapAction passes through the action unchanged.
func (s *Slog[I, O]) WrapAction(_ context.Context, _ *fh.Rule[I, O], action fh.Action[I, O], _ I) (fh.Action[I, O], error) {
	return action, nil
}

// WrapDestination passes through the destination unchanged.
func (s *Slog[I, O]) WrapDestination(_ context.Context, _ *fh.Rule[I, O], destination fh.Destination[O], _ O) (fh.Destination[O], error) {
	return destination, nil
}

func (s Slog[I, O]) callback(ctx context.Context, event I, reports chan<- fh.Report) {
	reportsChan := make(chan fh.Report)

	var waitGroup sync.WaitGroup

	waitGroup.Add(1)

	defer waitGroup.Wait()
	defer close(reportsChan)

	go func() {
		defer waitGroup.Done()

		results := []fh.Report{}

		for report := range reportsChan {
			results = append(results, report)
			reports <- report
		}

		slog.InfoContext(ctx, "", "source", s.source, "event", event, "reports", results)
	}()

	s.downstream(ctx, event, reportsChan)
}
