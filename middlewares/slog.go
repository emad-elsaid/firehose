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
) (fh.Callback[I], error) {
	s.downstream = callback
	s.source = rule.From

	return s.callback, nil
}

// WrapAction passes through the action unchanged.
func (s *Slog[I, O]) WrapAction(
	_ context.Context,
	_ *fh.Rule[I, O],
	action fh.Action[I, O],
) (fh.Action[I, O], error) {
	return action, nil
}

// WrapDestination passes through the destination unchanged.
func (s *Slog[I, O]) WrapDestination(
	_ context.Context,
	_ *fh.Rule[I, O],
	destination fh.Destination[O],
) (fh.Destination[O], error) {
	return destination, nil
}

func (s Slog[I, O]) callback(ctx context.Context, event I, report fh.ReportFunc) {
	results := []fh.Report{}

	var mutex sync.Mutex

	reportSink := func(r fh.Report) {
		mutex.Lock()
		defer mutex.Unlock()

		results = append(results, r)
		report(r)
	}

	s.downstream(ctx, event, reportSink)

	slog.InfoContext(ctx, "", "source", s.source, "event", event, "reports", results)
}
