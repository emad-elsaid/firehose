package events

import (
	"bytes"
	"context"
	"fmt"
	"os"
)

type Process struct {
	PID int
}

func (p Process) Attributes(ctx context.Context) map[string]any {
	return map[string]any{
		"pid": p.PID,
		"cwd": p.Cwd,
		"exe": p.Exe,
		"cmd": p.Cmdline,
	}
}

func (p Process) Cwd() (string, error) {
	return os.Readlink(fmt.Sprintf("/proc/%d/cwd", p.PID))
}

func (p Process) Exe() (string, error) {
	return os.Readlink(fmt.Sprintf("/proc/%d/exe", p.PID))
}

func (p Process) Cmdline() (string, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", p.PID))
	if err != nil {
		return "", err
	}

	data = bytes.TrimRight(data, "\x00")

	return string(data), nil
}
