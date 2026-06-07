// Package destinations provides event destination implementations.
package destinations

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/emad-elsaid/firehose"
)

// Stdout is a destination that writes events to standard output.
type Stdout[T any] struct{}

// Send writes the event to standard output.
func (s Stdout[T]) Send(_ context.Context, event T) firehose.Report {
	_, err := io.WriteString(os.Stdout, fmt.Sprintf("%v\n", event))
	if err != nil {
		return firehose.NewReport(firehose.StatusDestinationError, fmt.Errorf("error sending event to stdout: %w", err))
	}

	return firehose.NewReport(firehose.StatusSuccess, nil)
}
