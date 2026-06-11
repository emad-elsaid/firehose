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

func (h HTTP) Start(ctx context.Context, cb fs.SourceCallback[events.HTTPReq]) (done context.Context, err error) {
	done, cancel := context.WithCancel(context.Background())

	http.HandleFunc(h.Endpoint, func(w http.ResponseWriter, r *http.Request) {
		event := events.HTTPReq{Request: *r}
		ctx := context.WithValue(r.Context(), "response", w)

		reports := cb(ctx, event)

		for range reports {
		}
	})

	go func() {
		defer cancel()

		for {
			select {
			case <-ctx.Done():
				return
			case <-done.Done():
			default:
			}
		}
	}()

	return done, nil
}
