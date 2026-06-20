package firehose

import (
	"context"
)

// EventMock is a type alias for MockAttributer for convenience in tests
// MockAttributer implements the Attributer interface with Attributes method
type EventMock = MockAttributer

// MockRule is a type alias for Rule[*EventMock, *EventMock] for convenience in tests
type MockRule = Rule[*EventMock, *EventMock]

// TestAction is a simple action implementation with ID for testing
type TestAction struct {
	MockAction[*EventMock, *EventMock]
	ID string
}

// TestDestination is a simple destination implementation with ID for testing
type TestDestination struct {
	MockDestination[*EventMock]
	ID string
}

// SourceMock wraps MockSource with additional test helpers
type SourceMock[T any] struct {
	MockSource[T]
	id         string
	cancelFunc context.CancelFunc
	started    bool
}

// newSourceMock creates a new SourceMock with the given ID
func newSourceMock[T any](id string) *SourceMock[T] {
	return &SourceMock[T]{
		MockSource: MockSource[T]{},
		id:         id,
	}
}

// Start wraps the mock Start and tracks state for testing
func (s *SourceMock[T]) Start(ctx context.Context, cb Callback[T]) (context.Context, error) {
	// Call the underlying mock
	_, err := s.MockSource.Start(ctx, cb)

	// Create a cancellable context for testing
	newCtx, cancel := context.WithCancel(ctx)
	s.cancelFunc = cancel
	s.started = true

	// Return error if mock specifies one
	if err != nil {
		return newCtx, err
	}

	return newCtx, nil
}

// isStarted checks if the source has started
func (s *SourceMock[T]) isStarted() bool {
	return s.started
}

// Stop cancels the source context
func (s *SourceMock[T]) Stop() {
	if s.cancelFunc != nil {
		s.cancelFunc()
	}
}

// mockIncompatibleSourceRegistry is used to test type assertion failures
type mockIncompatibleSourceRegistry struct{}

func (m *mockIncompatibleSourceRegistry) setNextSameSource(n sourceRegistry) {}
func (m *mockIncompatibleSourceRegistry) setPrevSameSource(p sourceRegistry) {}
func (m *mockIncompatibleSourceRegistry) getNextSameSource() sourceRegistry  { return nil }
func (m *mockIncompatibleSourceRegistry) getRegistry() Registry              { return nil }
