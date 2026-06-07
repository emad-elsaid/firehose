package firehose

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

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

func TestAddRule(t *testing.T) {
	t.Parallel()

	t.Run("add first rule to nil registry", func(t *testing.T) {
		rule := &MockRule{
			When: newSourceMock[*EventMock]("source1"),
			Then: &MockAction{},
			To:   &MockDestination{},
		}

		result, err := AddRule(t.Context(), nil, rule, nil, nil, nil, new(EventMock), new(EventMock))

		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, result, result.getNext())
		require.Equal(t, result, result.getPrev())
	})

	t.Run("add second rule to existing registry", func(t *testing.T) {
		rule1 := &MockRule{
			When: newSourceMock[*EventMock]("source1"),
			Then: &MockAction{},
			To:   &MockDestination{},
		}
		registry, _ := AddRule(t.Context(), nil, rule1, nil, nil, nil, new(EventMock), new(EventMock))

		rule2 := &MockRule{
			When: newSourceMock[*EventMock]("source2"),
			Then: &MockAction{},
			To:   &MockDestination{},
		}

		result, err := AddRule(t.Context(), registry, rule2, nil, nil, nil, new(EventMock), new(EventMock))

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
		rule1 := &MockRule{
			When: newSourceMock[*EventMock]("source1"),
			Then: &MockAction{},
			To:   &MockDestination{},
		}
		registry, _ := AddRule(t.Context(), nil, rule1, nil, nil, nil, new(EventMock), new(EventMock))

		rule2 := &MockRule{
			When: newSourceMock[*EventMock]("source2"),
			Then: &MockAction{},
			To:   &MockDestination{},
		}
		registry, _ = AddRule(t.Context(), registry, rule2, nil, nil, nil, new(EventMock), new(EventMock))

		rule3 := &MockRule{
			When: newSourceMock[*EventMock]("source3"),
			Then: &MockAction{},
			To:   &MockDestination{},
		}

		result, err := AddRule(t.Context(), registry, rule3, nil, nil, nil, new(EventMock), new(EventMock))

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
	t.Parallel()

	t.Run("first rule with a source has no same-source links", func(t *testing.T) {
		source := newSourceMock[*EventMock]("source1")
		rule := &MockRule{
			When: source,
			Then: &MockAction{},
			To:   &MockDestination{},
		}

		result, err := AddRule(t.Context(), nil, rule, nil, nil, nil, new(EventMock), new(EventMock))

		require.NoError(t, err)
		require.NotNil(t, result)

		ruleResult := result.(*Rule[*EventMock, *EventMock])
		require.Nil(t, ruleResult.nextSameSource, "first rule should have no next same source")
		require.Nil(t, ruleResult.prevSameSource, "first rule should have no prev same source")
	})

	t.Run("two rules with different sources have no same-source links", func(t *testing.T) {
		source1 := newSourceMock[*EventMock]("source1")
		source2 := newSourceMock[*EventMock]("source2")

		registry, _ := AddRule(t.Context(), nil, &MockRule{
			When: source1,
			Then: &MockAction{},
			To:   &MockDestination{},
		}, nil, nil, nil, new(EventMock), new(EventMock))

		rule2 := &MockRule{
			When: source2,
			Then: &MockAction{},
			To:   &MockDestination{},
		}

		result, err := AddRule(t.Context(), registry, rule2, nil, nil, nil, new(EventMock), new(EventMock))

		require.NoError(t, err)

		// First rule
		rule1 := result.(*Rule[*EventMock, *EventMock])
		require.Nil(t, rule1.nextSameSource, "rule1 should have no next same source")
		require.Nil(t, rule1.prevSameSource, "rule1 should have no prev same source")

		// Second rule
		secondRule := result.getNext().(*Rule[*EventMock, *EventMock])
		require.Nil(t, secondRule.nextSameSource, "rule2 should have no next same source")
		require.Nil(t, secondRule.prevSameSource, "rule2 should have no prev same source")
	})

	t.Run("two rules with same source are linked", func(t *testing.T) {
		source := newSourceMock[*EventMock]("source1")

		registry, _ := AddRule(t.Context(), nil, &MockRule{
			When: source,
			Then: &MockAction{},
			To:   &MockDestination{},
		}, nil, nil, nil, new(EventMock), new(EventMock))

		rule2 := &MockRule{
			When: source,
			Then: &MockAction{},
			To:   &MockDestination{},
		}

		result, err := AddRule(t.Context(), registry, rule2, nil, nil, nil, new(EventMock), new(EventMock))

		require.NoError(t, err)

		// First rule
		rule1 := result.(*Rule[*EventMock, *EventMock])
		require.NotNil(t, rule1.nextSameSource, "rule1 should have next same source")
		require.Nil(t, rule1.prevSameSource, "rule1 is first, should have no prev same source")

		// Second rule
		secondRule := result.getNext().(*Rule[*EventMock, *EventMock])
		require.Nil(t, secondRule.nextSameSource, "rule2 is last, should have no next same source")
		require.NotNil(t, secondRule.prevSameSource, "rule2 should have prev same source")

		// Verify they point to each other
		require.Equal(t, secondRule, rule1.nextSameSource.getRegistry())
		require.Equal(t, rule1, secondRule.prevSameSource.getRegistry())
	})

	t.Run("three rules with same source form a chain", func(t *testing.T) {
		source := newSourceMock[*EventMock]("source1")

		registry, _ := AddRule(t.Context(), nil, &MockRule{
			When: source,
			Then: &MockAction{},
			To:   &MockDestination{},
		}, nil, nil, nil, new(EventMock), new(EventMock))

		registry, _ = AddRule(t.Context(), registry, &MockRule{
			When: source,
			Then: &MockAction{},
			To:   &MockDestination{},
		}, nil, nil, nil, new(EventMock), new(EventMock))

		rule3 := &MockRule{
			When: source,
			Then: &MockAction{},
			To:   &MockDestination{},
		}

		result, err := AddRule(t.Context(), registry, rule3, nil, nil, nil, new(EventMock), new(EventMock))

		require.NoError(t, err)

		// Navigate the circular list to find all three rules
		rules := make([]*Rule[*EventMock, *EventMock], 0, 3)
		current := result
		for i := 0; i < 3; i++ {
			rules = append(rules, current.(*Rule[*EventMock, *EventMock]))
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
		sourceA := newSourceMock[*EventMock]("sourceA")
		sourceB := newSourceMock[*EventMock]("sourceB")

		// Add first rule with sourceA
		registry, _ := AddRule(t.Context(), nil, &MockRule{
			When: sourceA,
			Then: &MockAction{},
			To:   &MockDestination{},
		}, nil, nil, nil, new(EventMock), new(EventMock))

		// Add second rule with sourceA
		registry, _ = AddRule(t.Context(), registry, &MockRule{
			When: sourceA,
			Then: &MockAction{},
			To:   &MockDestination{},
		}, nil, nil, nil, new(EventMock), new(EventMock))

		// Add rule with sourceB
		registry, _ = AddRule(t.Context(), registry, &MockRule{
			When: sourceB,
			Then: &MockAction{},
			To:   &MockDestination{},
		}, nil, nil, nil, new(EventMock), new(EventMock))

		// Add third rule with sourceA
		result, err := AddRule(t.Context(), registry, &MockRule{
			When: sourceA,
			Then: &MockAction{},
			To:   &MockDestination{},
		}, nil, nil, nil, new(EventMock), new(EventMock))

		require.NoError(t, err)

		// Collect all rules
		rules := make([]*Rule[*EventMock, *EventMock], 0, 4)
		current := result
		for i := 0; i < 4; i++ {
			rules = append(rules, current.(*Rule[*EventMock, *EventMock]))
			current = current.getNext()
		}

		// Identify rules by source
		var sourceARules []*Rule[*EventMock, *EventMock]
		var sourceBRules []*Rule[*EventMock, *EventMock]

		for _, r := range rules {
			if r.When.(*SourceMock[*EventMock]).id == "sourceA" {
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
		source := newSourceMock[*EventMock]("source1")
		defer source.AssertExpectations(t)

		registry, _ := AddRule(t.Context(), nil, &MockRule{
			When: source,
			Then: &MockAction{},
			To:   &MockDestination{},
		}, nil, nil, nil, new(EventMock), new(EventMock))

		ctx := t.Context()
		errChan := make(chan error, 10)

		source.On("Start", ctx, mock.Anything).Return(ctx, nil).Once()

		Start(ctx, registry, errChan)

		require.Eventually(t, source.isStarted, time.Second, time.Millisecond*100)

		source.Stop()
		go Wait(registry, errChan)

		receivedErrors := drainErrorChannel(t, errChan, 1*time.Second)

		require.Len(t, receivedErrors, 1) // context.Canceled when we stop the source

		rule := registry.(*Rule[*EventMock, *EventMock])
		require.NotNil(t, rule.ctx)
	})

	t.Run("start multiple rules with different sources", func(t *testing.T) {
		source1 := newSourceMock[*EventMock]("source1")
		defer source1.AssertExpectations(t)
		source2 := newSourceMock[*EventMock]("source2")
		defer source2.AssertExpectations(t)

		registry, _ := AddRule(t.Context(), nil, &MockRule{
			When: source1,
			Then: &MockAction{},
			To:   &MockDestination{},
		}, nil, nil, nil, new(EventMock), new(EventMock))
		registry, _ = AddRule(t.Context(), registry, &MockRule{
			When: source2,
			Then: &MockAction{},
			To:   &MockDestination{},
		}, nil, nil, nil, new(EventMock), new(EventMock))

		ctx := t.Context()
		errChan := make(chan error, 10)

		source1.On("Start", ctx, mock.Anything).Return(ctx, nil).Once()
		source2.On("Start", ctx, mock.Anything).Return(ctx, nil).Once()

		Start(ctx, registry, errChan)

		require.Eventually(t, source1.isStarted, time.Second, time.Millisecond*100)
		require.Eventually(t, source2.isStarted, time.Second, time.Millisecond*100)

		source1.Stop()
		source2.Stop()
		go Wait(registry, errChan)

		receivedErrors := drainErrorChannel(t, errChan, 1*time.Second)

		require.Len(t, receivedErrors, 2) // context.Canceled for each source
	})

	t.Run("start rule with source error", func(t *testing.T) {
		source := newSourceMock[*EventMock]("source1")
		defer source.AssertExpectations(t)

		registry, _ := AddRule(t.Context(), nil, &MockRule{
			When: source,
			Then: &MockAction{},
			To:   &MockDestination{},
		}, nil, nil, nil, new(EventMock), new(EventMock))

		ctx := t.Context()
		errChan := make(chan error, 10)

		source.On("Start", ctx, mock.Anything).Return(ctx, os.ErrClosed).Once()

		Start(ctx, registry, errChan)

		require.Eventually(t, source.isStarted, time.Second, time.Millisecond*100)

		go Wait(registry, errChan)

		receivedErrors := drainErrorChannel(t, errChan, 1*time.Second)

		require.Len(t, receivedErrors, 1)
	})

	t.Run("start multiple rules with same source", func(t *testing.T) {
		source := newSourceMock[*EventMock]("source1")
		defer source.AssertExpectations(t)

		registry, _ := AddRule(t.Context(), nil, &MockRule{
			When: source,
			Then: &MockAction{},
			To:   &MockDestination{},
		}, nil, nil, nil, new(EventMock), new(EventMock))
		registry, _ = AddRule(t.Context(), registry, &MockRule{
			When: source,
			Then: &MockAction{},
			To:   &MockDestination{},
		}, nil, nil, nil, new(EventMock), new(EventMock))

		ctx := t.Context()
		errChan := make(chan error, 10)

		source.On("Start", ctx, mock.Anything).Return(ctx, nil).Once()

		Start(ctx, registry, errChan)

		require.Eventually(t, source.isStarted, time.Second, time.Millisecond*100)
		source.Stop()

		go Wait(registry, errChan)

		receivedErrors := drainErrorChannel(t, errChan, 1*time.Second)

		require.Len(t, receivedErrors, 1) // Only one context.Canceled since source is shared
	})
}
