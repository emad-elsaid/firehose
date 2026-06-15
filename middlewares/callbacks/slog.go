package callbacks

import (
	"context"
	"log/slog"

	fh "github.com/emad-elsaid/firehose"
)

// Slog is a callback middleware that logs the events and reports using Go
// structured logging.
type Slog[I, O fh.Event] struct {
	downstream fh.Callback[I]
	source     fh.Source[I]
}

// Wrap stores the downstream callback to be wrapped with logging and returns
// the callback function to be used by the source.
func (s *Slog[I, O]) Wrap(
	_ context.Context,
	rule fh.Rule[I, O],
	callback fh.Callback[I],
	_ I,
) (fh.Callback[I], error) {
	s.downstream = callback
	s.source = rule.When

	return s.callback, nil
}

func (s Slog[I, O]) callback(ctx context.Context, event I, reports chan<- fh.Report) {
	reportsChan := make(chan fh.Report)
	defer close(reportsChan)

	go func() {

		results := []fh.Report{}

		for report := range reportsChan {
			results = append(results, report)
			reports <- report
		}

		slog.InfoContext(ctx, "", "source", s.source, "event", event, "reports", results)
	}()

	s.downstream(ctx, event, reportsChan)
}
