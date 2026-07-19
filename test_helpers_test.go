package firehose

import "github.com/emad-elsaid/boolexpr"

// EventMock is a simple test event that implements boolexpr.Symbols
type EventMock struct {
	boolexpr.Symbols
}

// NewEventMock creates a new EventMock with the given attributes
func NewEventMock(attrs map[string]any) *EventMock {
	if attrs == nil {
		attrs = make(map[string]any)
	}
	return &EventMock{
		Symbols: boolexpr.SymbolsMap(attrs),
	}
}

// MockSQLRule is a type alias for SQLRule[*EventMock, *EventMock]
type MockSQLRule = SQLRule[*EventMock, *EventMock]

type reportCollector struct {
	errors []error
}

func newReportCollector() *reportCollector {
	return &reportCollector{}
}

func (c *reportCollector) Collect(err error) {
	// Only collect actual errors, not nil (success)
	if err != nil {
		c.errors = append(c.errors, err)
	}
}

func (c *reportCollector) Errors() []error {
	out := make([]error, len(c.errors))
	copy(out, c.errors)

	return out
}
