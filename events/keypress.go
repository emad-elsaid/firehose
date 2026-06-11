package events

import (
	"context"
	"log/slog"
	"strconv"
)

type KeyPress struct {
	Key uint16
}

func (k KeyPress) Attributes(_ context.Context) (map[string]any, error) {
	return map[string]any{
		"key": int(k.Key),
	}, nil
}

func (k KeyPress) String() string {
	return strconv.FormatUint(uint64(k.Key), 10)
}

func (k KeyPress) LogValue() slog.Value {
	return slog.StringValue(k.String())
}
