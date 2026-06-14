package firehose

import (
	"context"
	"sync"

	"github.com/emad-elsaid/boolexpr"
	"github.com/stretchr/testify/mock"
)

type MockRule = Rule[*EventMock, *EventMock]
type MockSource = SourceMock[*EventMock]
type MockAction = ActionMock[*EventMock, *EventMock]
type MockDestination = DestinationMock[*EventMock]

type EventMock struct {
	mock.Mock
}

func (e *EventMock) Attributes(ctx context.Context) (map[string]any, error) {
	args := e.Called(ctx)
	return args.Get(0).(map[string]any), args.Error(1)
}

type SourceMock[T any] struct {
	mock.Mock
	lck     sync.Mutex
	cb      Callback[T]
	cancel  context.CancelFunc
	id      string
	started bool
}

func newSourceMock[T any](id string) *SourceMock[T] {
	return &SourceMock[T]{
		id: id,
	}
}

func (m *SourceMock[T]) Start(ctx context.Context, cb Callback[*EventMock]) (context.Context, error) {
	args := m.Called(ctx, cb)
	sourceCtx := args.Get(0).(context.Context)
	sourceCtx, cancel := context.WithCancel(sourceCtx)
	m.lck.Lock()
	defer m.lck.Unlock()
	m.cancel = cancel
	m.started = true

	return sourceCtx, args.Error(1)
}

func (m *SourceMock[T]) isStarted() bool {
	m.lck.Lock()
	defer m.lck.Unlock()

	return m.started
}

func (m *SourceMock[T]) Stop() {
	if m.cancel != nil {
		m.cancel()

		m.lck.Lock()
		defer m.lck.Unlock()
		m.cancel = nil
	}
}

type ActionMock[In, Out any] struct {
	mock.Mock
}

func (m *ActionMock[In, Out]) Process(ctx context.Context, event In, syms boolexpr.Symbols) (Out, Report) {
	args := m.Called(ctx, event, syms)

	return args.Get(0).(Out), args.Get(1).(Report)
}

type DestinationMock[T any] struct {
	mock.Mock
}

func (m *DestinationMock[T]) Send(ctx context.Context, event T) Report {
	args := m.Called(ctx, event)

	return args.Get(0).(Report)
}

// mockIncompatibleRegistry is a registry that doesn't implement callbackable
type mockIncompatibleRegistry struct{}

func (m *mockIncompatibleRegistry) getNext() Registry                 { return nil }
func (m *mockIncompatibleRegistry) setNext(n Registry)                {}
func (m *mockIncompatibleRegistry) getPrev() Registry                 { return nil }
func (m *mockIncompatibleRegistry) setPrev(p Registry)                {}
func (m *mockIncompatibleRegistry) getSource() any                    { return nil }
func (m *mockIncompatibleRegistry) getCtx() context.Context           { return nil }
func (m *mockIncompatibleRegistry) start(ctx context.Context) error   { return nil }
func (m *mockIncompatibleRegistry) getSourceRegistry() sourceRegistry { return nil }

// mockIncompatibleSourceRegistry returns an incompatible registry
type mockIncompatibleSourceRegistry struct{}

func (m *mockIncompatibleSourceRegistry) setNextSameSource(n sourceRegistry) {}
func (m *mockIncompatibleSourceRegistry) setPrevSameSource(p sourceRegistry) {}
func (m *mockIncompatibleSourceRegistry) getNextSameSource() sourceRegistry  { return nil }
func (m *mockIncompatibleSourceRegistry) getRegistry() Registry {
	return &mockIncompatibleRegistry{}
}
