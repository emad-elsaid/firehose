package destinations

import (
	"context"
	"errors"
	"net/http"

	"github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/events"
)

var ErrMissingResponseWriter = errors.New("missing response writer in context")

type HTTP struct{}

func (HTTP) Send(ctx context.Context, event events.HTTPRes) firehose.Report {
	w, ok := ctx.Value("response").(http.ResponseWriter)
	if !ok {
		return firehose.NewReport(firehose.StatusDestinationError, ErrMissingResponseWriter)
	}

	for key, value := range event.Header {
		for _, v := range value {
			w.Header().Add(key, v)
		}
	}

	w.WriteHeader(event.StatusCode)

	_, err := w.Write(event.Body)
	if err != nil {
		return firehose.NewReport(firehose.StatusDestinationError, err)
	}

	return firehose.NewReport(firehose.StatusSuccess, nil)
}
