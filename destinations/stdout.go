// Package destinations provides event destination implementations.
package destinations

import (
	"fmt"
	"io"
	"os"
)

// Stdout is a destination that writes events to standard output.
type Stdout[T any] struct{}

// Send writes the event to standard output.
func (s Stdout[T]) Send(event T) error {
	_, err := io.WriteString(os.Stdout, fmt.Sprintf("%v\n", event))
	if err != nil {
		return fmt.Errorf("error sending event to stdout: %w", err)
	}

	return nil
}
