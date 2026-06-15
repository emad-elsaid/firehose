package events

import (
	"log/slog"
	"strconv"
)

type KeyPress struct {
	Key uint16
}

func (k KeyPress) String() string {
	return strconv.FormatUint(uint64(k.Key), 10)
}

func (k KeyPress) LogValue() slog.Value {
	return slog.StringValue(k.String())
}
