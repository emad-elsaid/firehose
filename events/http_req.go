package events

import (
	"net/http"
)

type HTTPReq struct {
	http.Request
}
