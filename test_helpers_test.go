package firehose

// Type aliases for convenience in tests

// EventMock is a type alias for MockAttributer
type EventMock = MockAttributer

// MockRule is a type alias for Rule[*EventMock, *EventMock]
type MockRule = Rule[*EventMock, *EventMock]
