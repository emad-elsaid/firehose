package firehose

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Test helper functions

// drainErrorChannel collects all errors from a channel until it closes or timeout expires.
func drainErrorChannel(t *testing.T, errChan <-chan error, timeout time.Duration) []error {
	t.Helper()

	var errors []error
	timeoutCh := time.After(timeout)

	for {
		select {
		case err, ok := <-errChan:
			if !ok {
				return errors
			}
			errors = append(errors, err)
		case <-timeoutCh:
			t.Fatal("timeout waiting for error channel to close")
			return nil
		}
	}
}

// Mock implementations for testing

type mockSource[T any] struct {
	id       string
	startErr error
	started  bool
	done     chan struct{}
	stopped  bool
}

func newMockSource[T any](id string) *mockSource[T] {
	return &mockSource[T]{
		id:   id,
		done: make(chan struct{}),
	}
}

func (m *mockSource[T]) ID() string {
	return m.id
}

func (m *mockSource[T]) Start(ctx context.Context, cb func(context.Context, T) error) (context.Context, error) {
	m.started = true
	if m.startErr != nil {
		return nil, m.startErr
	}

	doneCtx, cancel := context.WithCancel(ctx)

	go func() {
		select {
		case <-m.done:
			cancel()
		case <-ctx.Done():
			cancel()
		}
	}()

	return doneCtx, nil
}

func (m *mockSource[T]) Stop() {
	if !m.stopped {
		m.stopped = true
		close(m.done)
	}
}

type mockAction[In, Out any] struct {
	processErr error
	processed  []In
}

func (m *mockAction[In, Out]) Process(ctx context.Context, event In) (Out, error) {
	m.processed = append(m.processed, event)
	var zero Out
	if m.processErr != nil {
		return zero, m.processErr
	}
	return zero, nil
}

type mockDestination[T any] struct {
	sendErr error
	sent    []T
}

func (m *mockDestination[T]) Send(event T) error {
	if m.sendErr != nil {
		return m.sendErr
	}
	m.sent = append(m.sent, event)
	return nil
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
func (m *mockIncompatibleSourceRegistry) getRegistry() Registry {
	return &mockIncompatibleRegistry{}
}

func TestAddRule(t *testing.T) {
	t.Run("add first rule to nil registry", func(t *testing.T) {
		rule := &Rule[int, string]{
			When: newMockSource[int]("source1"),
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		}

		result, err := AddRule(nil, rule)

		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, result, result.getNext())
		require.Equal(t, result, result.getPrev())
	})

	t.Run("add second rule to existing registry", func(t *testing.T) {
		rule1 := &Rule[int, string]{
			When: newMockSource[int]("source1"),
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		}
		registry, _ := AddRule[int, string](nil, rule1)

		rule2 := &Rule[int, string]{
			When: newMockSource[int]("source2"),
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		}

		result, err := AddRule(registry, rule2)

		require.NoError(t, err)
		require.NotNil(t, result)

		// Result should be the first rule (registry head)
		require.Equal(t, rule1, result)

		// With 2 rules in a circular list: rule1 <-> rule2
		require.Equal(t, rule2, result.getNext(), "rule1.next should point to rule2")
		require.Equal(t, rule2, result.getPrev(), "rule1.prev should point to rule2")

		// Verify rule2 points back to rule1
		require.Equal(t, rule1, rule2.getNext(), "rule2.next should point to rule1")
		require.Equal(t, rule1, rule2.getPrev(), "rule2.prev should point to rule1")
	})

	t.Run("add third rule to registry with two rules", func(t *testing.T) {
		rule1 := &Rule[int, string]{
			When: newMockSource[int]("source1"),
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		}
		registry, _ := AddRule[int, string](nil, rule1)

		rule2 := &Rule[int, string]{
			When: newMockSource[int]("source2"),
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		}
		registry, _ = AddRule[int, string](registry, rule2)

		rule3 := &Rule[int, string]{
			When: newMockSource[int]("source3"),
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		}

		result, err := AddRule(registry, rule3)

		require.NoError(t, err)
		require.NotNil(t, result)

		// Result should still be rule1 (registry head doesn't change)
		require.Equal(t, rule1, result)

		// Verify circular structure: rule1 -> rule2 -> rule3 -> rule1
		require.Equal(t, rule2, rule1.getNext(), "rule1.next should be rule2")
		require.Equal(t, rule3, rule2.getNext(), "rule2.next should be rule3")
		require.Equal(t, rule1, rule3.getNext(), "rule3.next should be rule1")

		// Verify reverse: rule1 <- rule2 <- rule3 <- rule1
		require.Equal(t, rule3, rule1.getPrev(), "rule1.prev should be rule3")
		require.Equal(t, rule1, rule2.getPrev(), "rule2.prev should be rule1")
		require.Equal(t, rule2, rule3.getPrev(), "rule3.prev should be rule2")
	})
}

func TestAddRuleSameSourceChaining(t *testing.T) {
	t.Run("first rule with a source has no same-source links", func(t *testing.T) {
		source := newMockSource[int]("source1")
		rule := &Rule[int, string]{
			When: source,
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		}

		result, err := AddRule[int, string](nil, rule)

		require.NoError(t, err)
		require.NotNil(t, result)

		ruleResult := result.(*Rule[int, string])
		require.Nil(t, ruleResult.nextSameSource, "first rule should have no next same source")
		require.Nil(t, ruleResult.prevSameSource, "first rule should have no prev same source")
	})

	t.Run("two rules with different sources have no same-source links", func(t *testing.T) {
		source1 := newMockSource[int]("source1")
		source2 := newMockSource[int]("source2")

		registry, _ := AddRule[int, string](nil, &Rule[int, string]{
			When: source1,
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		})

		rule2 := &Rule[int, string]{
			When: source2,
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		}

		result, err := AddRule(registry, rule2)

		require.NoError(t, err)

		// First rule
		rule1 := result.(*Rule[int, string])
		require.Nil(t, rule1.nextSameSource, "rule1 should have no next same source")
		require.Nil(t, rule1.prevSameSource, "rule1 should have no prev same source")

		// Second rule
		secondRule := result.getNext().(*Rule[int, string])
		require.Nil(t, secondRule.nextSameSource, "rule2 should have no next same source")
		require.Nil(t, secondRule.prevSameSource, "rule2 should have no prev same source")
	})

	t.Run("two rules with same source are linked", func(t *testing.T) {
		source := newMockSource[int]("source1")

		registry, _ := AddRule[int, string](nil, &Rule[int, string]{
			When: source,
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		})

		rule2 := &Rule[int, string]{
			When: source,
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		}

		result, err := AddRule(registry, rule2)

		require.NoError(t, err)

		// First rule
		rule1 := result.(*Rule[int, string])
		require.NotNil(t, rule1.nextSameSource, "rule1 should have next same source")
		require.Nil(t, rule1.prevSameSource, "rule1 is first, should have no prev same source")

		// Second rule
		secondRule := result.getNext().(*Rule[int, string])
		require.Nil(t, secondRule.nextSameSource, "rule2 is last, should have no next same source")
		require.NotNil(t, secondRule.prevSameSource, "rule2 should have prev same source")

		// Verify they point to each other
		require.Equal(t, secondRule, rule1.nextSameSource.getRegistry())
		require.Equal(t, rule1, secondRule.prevSameSource.getRegistry())
	})

	t.Run("three rules with same source form a chain", func(t *testing.T) {
		source := newMockSource[int]("source1")

		registry, _ := AddRule[int, string](nil, &Rule[int, string]{
			When: source,
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		})

		registry, _ = AddRule[int, string](registry, &Rule[int, string]{
			When: source,
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		})

		rule3 := &Rule[int, string]{
			When: source,
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		}

		result, err := AddRule(registry, rule3)

		require.NoError(t, err)

		// Navigate the circular list to find all three rules
		rules := make([]*Rule[int, string], 0, 3)
		current := result
		for i := 0; i < 3; i++ {
			rules = append(rules, current.(*Rule[int, string]))
			current = current.getNext()
		}

		// First rule in same-source chain
		require.Nil(t, rules[0].prevSameSource, "first rule has no prev same source")
		require.NotNil(t, rules[0].nextSameSource, "first rule has next same source")

		// Second rule in same-source chain
		require.NotNil(t, rules[1].prevSameSource, "second rule has prev same source")
		require.NotNil(t, rules[1].nextSameSource, "second rule has next same source")

		// Third rule in same-source chain
		require.NotNil(t, rules[2].prevSameSource, "third rule has prev same source")
		require.Nil(t, rules[2].nextSameSource, "third rule has no next same source")

		// Verify the chain
		require.Equal(t, rules[1], rules[0].nextSameSource.getRegistry())
		require.Equal(t, rules[0], rules[1].prevSameSource.getRegistry())
		require.Equal(t, rules[2], rules[1].nextSameSource.getRegistry())
		require.Equal(t, rules[1], rules[2].prevSameSource.getRegistry())
	})

	t.Run("mixed sources: two with sourceA, one with sourceB, one more with sourceA", func(t *testing.T) {
		sourceA := newMockSource[int]("sourceA")
		sourceB := newMockSource[int]("sourceB")

		// Add first rule with sourceA
		registry, _ := AddRule[int, string](nil, &Rule[int, string]{
			When: sourceA,
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		})

		// Add second rule with sourceA
		registry, _ = AddRule[int, string](registry, &Rule[int, string]{
			When: sourceA,
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		})

		// Add rule with sourceB
		registry, _ = AddRule[int, string](registry, &Rule[int, string]{
			When: sourceB,
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		})

		// Add third rule with sourceA
		result, err := AddRule[int, string](registry, &Rule[int, string]{
			When: sourceA,
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		})

		require.NoError(t, err)

		// Collect all rules
		rules := make([]*Rule[int, string], 0, 4)
		current := result
		for i := 0; i < 4; i++ {
			rules = append(rules, current.(*Rule[int, string]))
			current = current.getNext()
		}

		// Identify rules by source
		var sourceARules []*Rule[int, string]
		var sourceBRules []*Rule[int, string]

		for _, r := range rules {
			if r.When.(*mockSource[int]).id == "sourceA" {
				sourceARules = append(sourceARules, r)
			} else {
				sourceBRules = append(sourceBRules, r)
			}
		}

		require.Len(t, sourceARules, 3, "should have 3 rules with sourceA")
		require.Len(t, sourceBRules, 1, "should have 1 rule with sourceB")

		// Verify sourceA chain
		require.Nil(t, sourceARules[0].prevSameSource)
		require.NotNil(t, sourceARules[0].nextSameSource)
		require.Equal(t, sourceARules[1], sourceARules[0].nextSameSource.getRegistry())

		require.NotNil(t, sourceARules[1].prevSameSource)
		require.NotNil(t, sourceARules[1].nextSameSource)
		require.Equal(t, sourceARules[0], sourceARules[1].prevSameSource.getRegistry())
		require.Equal(t, sourceARules[2], sourceARules[1].nextSameSource.getRegistry())

		require.NotNil(t, sourceARules[2].prevSameSource)
		require.Nil(t, sourceARules[2].nextSameSource)
		require.Equal(t, sourceARules[1], sourceARules[2].prevSameSource.getRegistry())

		// Verify sourceB has no same-source links
		require.Nil(t, sourceBRules[0].prevSameSource)
		require.Nil(t, sourceBRules[0].nextSameSource)
	})
}

func TestStart(t *testing.T) {
	t.Run("start single rule successfully", func(t *testing.T) {
		source := newMockSource[int]("source1")
		registry, _ := AddRule[int, string](nil, &Rule[int, string]{
			When: source,
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		})

		ctx := t.Context()
		errChan := make(chan error, 10)

		go Start(ctx, registry, errChan)

		source.Stop()

		receivedErrors := drainErrorChannel(t, errChan, 1*time.Second)

		require.Len(t, receivedErrors, 1) // context.Canceled when we stop the source

		rule := registry.(*Rule[int, string])
		require.True(t, source.started)
		require.NotNil(t, rule.ctx)
	})

	t.Run("start multiple rules with different sources", func(t *testing.T) {
		registry, _ := AddRule[int, string](nil, &Rule[int, string]{
			When: newMockSource[int]("source1"),
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		})
		registry, _ = AddRule[int, string](registry, &Rule[int, string]{
			When: newMockSource[int]("source2"),
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		})

		ctx := t.Context()
		errChan := make(chan error, 10)

		go Start(ctx, registry, errChan)

		// Stop all sources
		current := registry
		for {
			if rule, ok := current.(*Rule[int, string]); ok {
				if source, ok := rule.When.(*mockSource[int]); ok {
					source.Stop()
				}
			}

			current = current.getNext()
			if current == registry {
				break
			}
		}

		receivedErrors := drainErrorChannel(t, errChan, 1*time.Second)

		require.Len(t, receivedErrors, 2) // context.Canceled for each source

		// Verify all sources started
		count := 0
		current = registry
		for {
			rule := current.(*Rule[int, string])
			source := rule.When.(*mockSource[int])
			require.True(t, source.started)
			count++

			current = current.getNext()
			if current == registry {
				break
			}
		}
		require.Equal(t, 2, count)
	})

	t.Run("start rule with source error", func(t *testing.T) {
		source := newMockSource[int]("source1")
		source.startErr = errors.New("source start error")
		registry, _ := AddRule[int, string](nil, &Rule[int, string]{
			When: source,
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		})

		ctx := t.Context()
		errChan := make(chan error, 10)

		go Start(ctx, registry, errChan)

		receivedErrors := drainErrorChannel(t, errChan, 1*time.Second)

		require.Len(t, receivedErrors, 1)
		require.True(t, source.started)
	})

	t.Run("start multiple rules with same source", func(t *testing.T) {
		source := newMockSource[int]("source1")
		registry, _ := AddRule[int, string](nil, &Rule[int, string]{
			When: source,
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		})
		registry, _ = AddRule[int, string](registry, &Rule[int, string]{
			When: source,
			Then: &mockAction[int, string]{},
			To:   &mockDestination[string]{},
		})

		ctx := t.Context()
		errChan := make(chan error, 10)

		go Start(ctx, registry, errChan)

		source.Stop()

		receivedErrors := drainErrorChannel(t, errChan, 1*time.Second)

		require.Len(t, receivedErrors, 1) // Only one context.Canceled since source is shared

		// Source should only be started once
		firstRule := registry.(*Rule[int, string])
		require.True(t, source.started)
		require.NotNil(t, firstRule.ctx)
	})
}
