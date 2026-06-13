package actions

import (
	"bytes"
	"context"
	"embed"
	"html/template"
	"net/http"

	"github.com/emad-elsaid/boolexpr"
	"github.com/emad-elsaid/firehose"
	"github.com/emad-elsaid/firehose/events"
)

//go:embed templates
var templates embed.FS

type TwitchRace struct {
	Me   *int
	Chat *int
}

func (tr TwitchRace) Process(ctx context.Context, event events.HTTPReq, syms boolexpr.Symbols) (events.HTTPRes, firehose.Report) {
	t, err := template.ParseFS(templates, "templates/*.html")
	if err != nil {
		return events.HTTPRes{
			StatusCode: http.StatusInternalServerError,
			Header:     map[string][]string{"Content-Type": {"text/plain; charset=utf-8"}},
			Body:       []byte(err.Error()),
		}, firehose.NewReport(firehose.StatusActionError, err)
	}

	var body bytes.Buffer

	err = t.ExecuteTemplate(&body, "race", map[string]any{
		"Me":   tr.Me,
		"Chat": tr.Chat,
	})

	return events.HTTPRes{
		StatusCode: http.StatusOK,
		Header:     map[string][]string{"Content-Type": {"text/html; charset=utf-8"}},
		Body:       body.Bytes(),
	}, firehose.NewReport(firehose.StatusSuccess, nil)
}
