package sources

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log/slog"
	"os"

	fh "github.com/emad-elsaid/firehose"
	fs "github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/events"
)

type inputEvent struct {
	Time  struct{ Sec, Usec int64 } // 64-bit timestamps on modern Linux
	Type  uint16
	Code  uint16
	Value int32
}

const (
	EV_KEY   = 0x01 // Event type for keyboard keys
	VALUE_DP = 1    // Value for key down/press (0 is release, 2 is repeat)
)

type Keyboard struct{}

func (Keyboard) Start(ctx context.Context, cb fs.Callback[events.KeyPress]) (done context.Context, err error) {
	done, cancel := context.WithCancel(context.Background())

	devicePath := "/dev/input/event9"

	file, err := os.Open(devicePath)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		fmt.Println("You likely need to add your user to the input group to access the input devices.")
		return
	}

	reports := make(chan fh.Report)

	go func() {
		for range reports {
		}
	}()

	go func() {
		defer cancel()
		defer file.Close()

	OUTER:
		for {
			select {
			case <-ctx.Done():
				return
			case <-done.Done():
				return
			default:
				buffer := make([]byte, 24) // size of input_event struct on 64-bit Linux

				_, err := file.Read(buffer)
				if err != nil {
					slog.Error("Failed to read input event", "error", err)
					break OUTER
				}

				var ev inputEvent
				err = binary.Read(bytes.NewReader(buffer), binary.LittleEndian, &ev)
				if err != nil {
					slog.Error("Failed to parse input event", "error", err)
					continue OUTER
				}

				// Filter for a Key Press event
				if ev.Type == EV_KEY && ev.Value == VALUE_DP {
					event := events.KeyPress{
						Key: ev.Code,
					}

					cb(ctx, event, reports)
				}
			}
		}
	}()

	return done, nil
}
