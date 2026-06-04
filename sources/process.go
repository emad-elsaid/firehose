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

type Process struct{}

func (s Process) Start(ctx context.Context, callback firehose.SourceCallback[events.Process]) (context.Context, error) {
	sourceCtx, cancel := context.WithCancel(context.Background())

	go func() {
		lastPIDs, err := s.allPIDs()
		if err != nil {
			slog.Error("Error reading PIDs", "error", err)
			cancel()
			return
		}

	outer:
		for {
			select {
			case <-ctx.Done():
				break outer
			case <-sourceCtx.Done():
				break outer
			case <-time.After(5 * time.Second):
				pids, err := s.allPIDs()
				if err != nil {
					fmt.Printf("Error reading PIDs: %v\n", err)
					continue
				}

				last := types.NewSet(lastPIDs...)
				current := types.NewSet(pids...)
				newPIDs := current.Difference(last)

				for _, pid := range newPIDs.ToSlice() {
					reports := callback(sourceCtx, pidToProcess(pid))
					for report := range reports {
						if report.Err != nil {
							slog.Error("Error processing new process event", "error", report.Err)
						}
					}
				}

				lastPIDs = pids
			}
		}

		cancel()
	}()

	return sourceCtx, nil
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
