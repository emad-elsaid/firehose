package events

import (
	"bytes"
	"context"
	"fmt"
	"os"
)

// Process is an event that represents a process on the system.
type Process struct {
	PID int
}

// Attributes returns the attributes of the process.
func (p Process) Attributes(_ context.Context) (map[string]any, error) {
	return map[string]any{
		"cmd": p.Cmdline,
	}, nil
}

// Cmdline returns the command line of the process.
func (p Process) Cmdline() (string, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", p.PID))
	if err != nil {
		return "", err
	}

	data = bytes.TrimRight(data, "\x00")

	return string(data), nil
}
