package callbacks

import (
	"context"
	"log/slog"

	"github.com/emad-elsaid/firehose"
)

// Slog is a callback middleware that logs the events and reports using Go
// structured logging.
type Slog[In, Out firehose.Event] struct {
	downstream firehose.SourceCallback[In]
	source     firehose.Source[In]
}

// Wrap stores the downstream callback to be wrapped with logging and returns
// the callback function to be used by the source.
func (s *Slog[In, Out]) Wrap(
	_ context.Context,
	rule firehose.Rule[In, Out],
	callback firehose.SourceCallback[In],
	_ In,
) (firehose.SourceCallback[In], error) {
	s.downstream = callback
	s.source = rule.When

	return s.callback, nil
}

func (s Slog[In, Out]) callback(ctx context.Context, event In) <-chan firehose.Report {
	reports := make(chan firehose.Report)

	reportsChan := s.downstream(ctx, event)

	go func() {
		defer close(reports)

		results := make([]firehose.Report, 0)

		for report := range reportsChan {
			results = append(results, report)
		}

		slog.InfoContext(ctx, "", "source", s.source, "event", event, "reports", results)
	}()

	return reports
}
