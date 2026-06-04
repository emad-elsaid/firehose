package sources

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/events"
	"github.com/emad-elsaid/types"
)

const periodicCheck = 5 * time.Second

// Process is a source that emits events when new processes are created.
type Process struct{}

// Start begins monitoring for new processes and emits events when they are created.
func (s Process) Start(ctx context.Context, callback firehose.SourceCallback[events.Process]) (context.Context, error) {
	sourceCtx, cancel := context.WithCancel(context.Background())

	go func() {
		defer cancel()

		lastPIDs, err := s.allPIDs()
		if err != nil {
			slog.Error("error reading PIDs", "error", err)

			return
		}

		s.monitor(ctx, sourceCtx, callback, lastPIDs)
	}()

	return sourceCtx, nil
}

func (s Process) monitor(
	ctx, sourceCtx context.Context,
	callback firehose.SourceCallback[events.Process],
	lastPIDs []int,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-sourceCtx.Done():
			return
		case <-time.After(periodicCheck):
			lastPIDs = s.checkForNewProcesses(sourceCtx, callback, lastPIDs)
		}
	}
}

func (s Process) checkForNewProcesses(
	ctx context.Context,
	callback firehose.SourceCallback[events.Process],
	lastPIDs []int,
) []int {
	pids, err := s.allPIDs()
	if err != nil {
		slog.Error("Error reading PIDs", "error", err)

		return lastPIDs
	}

	last := types.NewSet(lastPIDs...)
	current := types.NewSet(pids...)
	newPIDs := current.Difference(last)

	for _, pid := range newPIDs.ToSlice() {
		reports := callback(ctx, pidToProcess(pid))
		for report := range reports {
			if report.Err != nil {
				slog.Error("Error processing new process event", "error", report.Err)
			}
		}
	}

	return pids
}

func (s Process) allPIDs() ([]int, error) {
	dir, err := os.ReadDir("/proc")
	if err != nil {
		return nil, err
	}

	pid := make([]int, 0, len(dir))
	for _, entry := range dir {
		if entry.IsDir() {
			var id int

			_, err := fmt.Sscanf(entry.Name(), "%d", &id)
			if err == nil {
				pid = append(pid, id)
			}
		}
	}

	return pid, nil
}

func pidToProcess(pid int) events.Process {
	return events.Process{
		PID: pid,
	}
}
