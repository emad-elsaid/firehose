package events

import (
	"context"
	"net/http"
)

type HTTPReq struct {
	http.Request
}

func (k HTTPReq) Attributes(_ context.Context) (map[string]any, error) {
	return map[string]any{}, nil
}
