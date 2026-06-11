package events

import "context"

type HTTPRes struct {
	StatusCode int
	Header     map[string][]string
	Body       []byte
}

func (k HTTPRes) Attributes(_ context.Context) (map[string]any, error) {
	return map[string]any{}, nil
}
