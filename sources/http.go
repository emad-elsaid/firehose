package sources

import (
	"context"
	"net/http"

	fs "github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/events"
)

type HTTP struct {
	Endpoint string `validate:"required"`
}

func (h HTTP) Start(ctx context.Context, cb fs.Callback[events.HTTPReq]) (done context.Context, err error) {
	done, cancel := context.WithCancel(context.Background())
	reports := make(chan fs.Report)

	http.HandleFunc(h.Endpoint, func(w http.ResponseWriter, r *http.Request) {
		event := events.HTTPReq{Request: *r}
		ctx := context.WithValue(r.Context(), "response", w)

		cb(ctx, event, reports)
	})

	go func() {
		defer cancel()

		for {
			select {
			case <-ctx.Done():
				return
			case <-done.Done():
				return
			case <-reports:
			}
		}
	}()

	return done, nil
}
