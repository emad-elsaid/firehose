package events

type HTTPRes struct {
	StatusCode int
	Header     map[string][]string
	Body       []byte
}
