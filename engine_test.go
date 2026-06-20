package firehose

import (
	"context"
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
			ID:   "rule1",
			When: newSourceMock[*EventMock]("source1"),
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
		}

		result, err := AddRule(t.Context(), nil, rule, nil, nil, nil, new(EventMock), new(EventMock))

		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, result, result.getNext())
		require.Equal(t, result, result.getPrev())
	})

	t.Run("add second rule to existing registry", func(t *testing.T) {
		rule1 := &MockRule{
			ID:   "rule1",
			When: newSourceMock[*EventMock]("source1"),
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
		}
		registry, _ := AddRule(t.Context(), nil, rule1, nil, nil, nil, new(EventMock), new(EventMock))

		rule2 := &MockRule{
			ID:   "rule2",
			When: newSourceMock[*EventMock]("source2"),
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
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
			ID:   "rule1",
			When: newSourceMock[*EventMock]("source1"),
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
		}
		registry, _ := AddRule(t.Context(), nil, rule1, nil, nil, nil, new(EventMock), new(EventMock))

		rule2 := &MockRule{
			ID:   "rule2",
			When: newSourceMock[*EventMock]("source2"),
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
		}
		registry, _ = AddRule(t.Context(), registry, rule2, nil, nil, nil, new(EventMock), new(EventMock))

		rule3 := &MockRule{
			ID:   "rule3",
			When: newSourceMock[*EventMock]("source3"),
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
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
			ID:   "rule1",
			When: source,
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
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
			ID:   "rule1",
			When: source1,
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
		}, nil, nil, nil, new(EventMock), new(EventMock))

		rule2 := &MockRule{
			ID:   "rule2",
			When: source2,
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
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
			ID:   "rule1",
			When: source,
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
		}, nil, nil, nil, new(EventMock), new(EventMock))

		rule2 := &MockRule{
			ID:   "rule2",
			When: source,
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
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
			ID:   "rule1",
			When: source,
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
		}, nil, nil, nil, new(EventMock), new(EventMock))

		registry, _ = AddRule(t.Context(), registry, &MockRule{
			ID:   "rule2",
			When: source,
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
		}, nil, nil, nil, new(EventMock), new(EventMock))

		rule3 := &MockRule{
			ID:   "rule3",
			When: source,
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
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
			ID:   "rule1",
			When: sourceA,
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
		}, nil, nil, nil, new(EventMock), new(EventMock))

		// Add second rule with sourceA
		registry, _ = AddRule(t.Context(), registry, &MockRule{
			ID:   "rule2",
			When: sourceA,
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
		}, nil, nil, nil, new(EventMock), new(EventMock))

		// Add rule with sourceB
		registry, _ = AddRule(t.Context(), registry, &MockRule{
			ID:   "rule3",
			When: sourceB,
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
		}, nil, nil, nil, new(EventMock), new(EventMock))

		// Add third rule with sourceA
		result, err := AddRule(t.Context(), registry, &MockRule{
			ID:   "rule4",
			When: sourceA,
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
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
			ID:   "rule1",
			When: source,
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
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
			ID:   "rule1",
			When: source1,
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
		}, nil, nil, nil, new(EventMock), new(EventMock))
		registry, _ = AddRule(t.Context(), registry, &MockRule{
			ID:   "rule2",
			When: source2,
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
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
			ID:   "rule1",
			When: source,
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
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
			ID:   "rule1",
			When: source,
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
		}, nil, nil, nil, new(EventMock), new(EventMock))
		registry, _ = AddRule(t.Context(), registry, &MockRule{
			ID:   "rule2",
			When: source,
			Then: &MockAction[*EventMock, *EventMock]{},
			To:   &MockDestination[*EventMock]{},
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

func Test_inherit(t *testing.T) {
	tcs := []struct {
		name     string
		parent   *MockRule
		child    *MockRule
		expected *MockRule
	}{
		{
			name: "child inherits parent's exported fields",
			parent: &MockRule{
				ID:   "parent",
				When: newSourceMock[*EventMock]("source1"),
				If:   "parent condition",
				Then: &TestAction{ID: "parent action"},
				To:   &TestDestination{ID: "parent destination"},
			},
			child: &MockRule{},
			expected: &MockRule{
				ID:   "parent/1",
				When: newSourceMock[*EventMock]("source1"),
				If:   "parent condition",
				Then: &TestAction{ID: "parent action"},
				To:   &TestDestination{ID: "parent destination"},
			},
		},

		{
			name: "child does not override its own fields",
			parent: &MockRule{
				ID:   "parent",
				When: newSourceMock[*EventMock]("source1"),
				If:   "parent condition",
				Then: &MockAction[*EventMock, *EventMock]{},
				To:   &MockDestination[*EventMock]{},
			},
			child: &MockRule{
				ID:   "child",
				When: newSourceMock[*EventMock]("source2"),
				If:   "child condition",
				Then: &TestAction{ID: "child action"},
				To:   &TestDestination{ID: "child destination"},
			},
			expected: &MockRule{
				ID:   "parent/child",
				When: newSourceMock[*EventMock]("source2"),
				If:   "(parent condition) and (child condition)",
				Then: &TestAction{ID: "child action"},
				To:   &TestDestination{ID: "child destination"},
			},
		},

		{
			name: "child inherits only missing fields from parent",
			parent: &MockRule{
				ID:   "parent",
				When: newSourceMock[*EventMock]("source1"),
				If:   "parent condition",
				Then: &MockAction[*EventMock, *EventMock]{},
				To:   &MockDestination[*EventMock]{},
			},
			child: &MockRule{
				ID:   "child",
				When: newSourceMock[*EventMock]("source2"),
				Then: &TestAction{ID: "child action"},
				To:   &TestDestination{ID: "child destination"},
			},
			expected: &MockRule{
				ID:   "parent/child",
				When: newSourceMock[*EventMock]("source2"),
				If:   "parent condition",
				Then: &TestAction{ID: "child action"},
				To:   &TestDestination{ID: "child destination"},
			},
		},

		{
			name: "child condition should be anded with parent condition if child has its own condition",
			parent: &MockRule{
				If: "parent condition",
			},
			child: &MockRule{
				If: "child condition",
			},
			expected: &MockRule{
				ID: "1",
				If: "(parent condition) and (child condition)",
			},
		},
		{
			name: "parent ID should be used as prefix for child ID if child has its own ID",
			parent: &MockRule{
				ID: "parent",
			},
			child: &MockRule{
				ID: "child",
			},
			expected: &MockRule{
				ID: "parent/child",
			},
		},
		{
			name: "doesn't copy subrules from parent to child",
			parent: &MockRule{
				ID: "parent",
				SubRules: []MockRule{
					{
						ID: "subrule1",
					},
				},
			},
			child: &MockRule{
				ID: "child",
			},
			expected: &MockRule{
				ID: "parent/child",
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			inherit(1, tc.parent, tc.child)
			require.Equal(t, tc.expected, tc.child)
		})
	}
}

func Test_flatten(t *testing.T) {
	parentSource := newSourceMock[*EventMock]("source1")
	parentAction := &TestAction{ID: "parent action"}
	parentDest := &TestDestination{ID: "parent dest"}

	tests := []struct {
		name     string
		rule     *MockRule
		expected *MockRule
	}{
		{
			name:     "nil rule does not panic",
			rule:     nil,
			expected: nil,
		},
		{
			name: "rule with no subrules remains unchanged",
			rule: &MockRule{
				ID:   "parent",
				When: parentSource,
				If:   "condition",
			},
			expected: &MockRule{
				ID:   "parent",
				When: parentSource,
				If:   "condition",
			},
		},
		{
			name: "single subrule inherits parent fields",
			rule: &MockRule{
				ID:   "parent",
				When: parentSource,
				If:   "parent condition",
				Then: parentAction,
				To:   parentDest,
				SubRules: []MockRule{
					{
						ID: "child",
					},
				},
			},
			expected: &MockRule{
				ID:   "parent",
				When: parentSource,
				If:   "parent condition",
				Then: parentAction,
				To:   parentDest,
				SubRules: []MockRule{
					{
						ID:   "parent/child",
						When: parentSource,
						If:   "parent condition",
						Then: parentAction,
						To:   parentDest,
					},
				},
			},
		},
		{
			name: "multiple subrules inherit parent fields",
			rule: &MockRule{
				ID:   "parent",
				When: parentSource,
				If:   "parent condition",
				Then: &MockAction[*EventMock, *EventMock]{},
				To:   &MockDestination[*EventMock]{},
				SubRules: []MockRule{
					{ID: "child1"},
					{ID: "child2"},
					{ID: "child3"},
				},
			},
			expected: &MockRule{
				ID:   "parent",
				When: parentSource,
				If:   "parent condition",
				Then: &MockAction[*EventMock, *EventMock]{},
				To:   &MockDestination[*EventMock]{},
				SubRules: []MockRule{
					{
						ID:   "parent/child1",
						When: parentSource,
						If:   "parent condition",
						Then: &MockAction[*EventMock, *EventMock]{},
						To:   &MockDestination[*EventMock]{},
					},
					{
						ID:   "parent/child2",
						When: parentSource,
						If:   "parent condition",
						Then: &MockAction[*EventMock, *EventMock]{},
						To:   &MockDestination[*EventMock]{},
					},
					{
						ID:   "parent/child3",
						When: parentSource,
						If:   "parent condition",
						Then: &MockAction[*EventMock, *EventMock]{},
						To:   &MockDestination[*EventMock]{},
					},
				},
			},
		},
		{
			name: "nested subrules inherit recursively",
			rule: &MockRule{
				ID:   "grandparent",
				When: parentSource,
				If:   "grandparent condition",
				Then: &MockAction[*EventMock, *EventMock]{},
				To:   &MockDestination[*EventMock]{},
				SubRules: []MockRule{
					{
						ID: "parent",
						If: "parent condition",
						SubRules: []MockRule{
							{
								ID: "child",
							},
						},
					},
				},
			},
			expected: &MockRule{
				ID:   "grandparent",
				When: parentSource,
				If:   "grandparent condition",
				Then: &MockAction[*EventMock, *EventMock]{},
				To:   &MockDestination[*EventMock]{},
				SubRules: []MockRule{
					{
						ID:   "grandparent/parent",
						When: parentSource,
						If:   "(grandparent condition) and (parent condition)",
						Then: &MockAction[*EventMock, *EventMock]{},
						To:   &MockDestination[*EventMock]{},
						SubRules: []MockRule{
							{
								ID:   "grandparent/parent/child",
								When: parentSource,
								If:   "(grandparent condition) and (parent condition)",
								Then: &MockAction[*EventMock, *EventMock]{},
								To:   &MockDestination[*EventMock]{},
							},
						},
					},
				},
			},
		},
		{
			name: "subrule overrides do not get replaced",
			rule: &MockRule{
				ID:   "parent",
				When: parentSource,
				If:   "parent condition",
				Then: parentAction,
				To:   parentDest,
				SubRules: []MockRule{
					{
						ID:   "child",
						When: newSourceMock[*EventMock]("source2"),
						If:   "child condition",
						Then: &TestAction{ID: "child action"},
						To:   &TestDestination{ID: "child dest"},
					},
				},
			},
			expected: &MockRule{
				ID:   "parent",
				When: parentSource,
				If:   "parent condition",
				Then: parentAction,
				To:   parentDest,
				SubRules: []MockRule{
					{
						ID:   "parent/child",
						When: newSourceMock[*EventMock]("source2"),
						If:   "(parent condition) and (child condition)",
						Then: &TestAction{ID: "child action"},
						To:   &TestDestination{ID: "child dest"},
					},
				},
			},
		},
		{
			name: "empty subrules array does not cause error",
			rule: &MockRule{
				ID:       "parent",
				SubRules: []MockRule{},
			},
			expected: &MockRule{
				ID:       "parent",
				SubRules: []MockRule{},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			flatten(tc.rule)
			require.Equal(t, tc.expected, tc.rule)
		})
	}
}

func Test_addSingleRule_Errors(t *testing.T) {
	tests := []struct {
		name        string
		rule        *MockRule
		expectError bool
	}{
		{
			name: "returns error for rule missing all required fields",
			rule: &MockRule{
				ID: "invalid",
			},
			expectError: true,
		},
		{
			name: "returns error for rule missing When",
			rule: &MockRule{
				ID:   "missing-when",
				Then: &MockAction[*EventMock, *EventMock]{},
				To:   &MockDestination[*EventMock]{},
			},
			expectError: true,
		},
		{
			name: "returns error for rule missing Then",
			rule: &MockRule{
				ID:   "missing-then",
				When: newSourceMock[*EventMock]("source1"),
				To:   &MockDestination[*EventMock]{},
			},
			expectError: true,
		},
		{
			name: "returns error for rule missing To",
			rule: &MockRule{
				ID:   "missing-to",
				When: newSourceMock[*EventMock]("source1"),
				Then: &MockAction[*EventMock, *EventMock]{},
			},
			expectError: true,
		},
		{
			name: "returns error for rule missing ID",
			rule: &MockRule{
				When: newSourceMock[*EventMock]("source1"),
				Then: &MockAction[*EventMock, *EventMock]{},
				To:   &MockDestination[*EventMock]{},
			},
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := addSingleRule(
				t.Context(),
				nil,
				tc.rule,
				nil,
				nil,
				nil,
				new(EventMock),
				new(EventMock),
			)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func Test_addSingleRule_WithSubRules(t *testing.T) {
	tests := []struct {
		name        string
		rule        *MockRule
		expectError bool
		expectNil   bool
	}{
		{
			name: "adds parent with valid subrules",
			rule: &MockRule{
				ID:   "parent",
				When: newSourceMock[*EventMock]("source1"),
				Then: &MockAction[*EventMock, *EventMock]{},
				To:   &MockDestination[*EventMock]{},
				SubRules: []MockRule{
					{
						ID:   "child1",
						When: newSourceMock[*EventMock]("source2"),
						Then: &MockAction[*EventMock, *EventMock]{},
						To:   &MockDestination[*EventMock]{},
					},
				},
			},
			expectError: false,
			expectNil:   false,
		},
		{
			name: "parent not activatable but has valid subrules",
			rule: &MockRule{
				ID: "parent-not-activatable",
				SubRules: []MockRule{
					{
						ID:   "child",
						When: newSourceMock[*EventMock]("source1"),
						Then: &MockAction[*EventMock, *EventMock]{},
						To:   &MockDestination[*EventMock]{},
					},
				},
			},
			expectError: false,
			expectNil:   false,
		},
		{
			name: "returns error when parent and subrules not activatable",
			rule: &MockRule{
				ID: "parent",
				SubRules: []MockRule{
					{
						ID: "child-invalid",
					},
				},
			},
			expectError: true,
			expectNil:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			registry, err := addSingleRule(
				t.Context(),
				nil,
				tc.rule,
				nil,
				nil,
				nil,
				new(EventMock),
				new(EventMock),
			)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			if tc.expectNil {
				require.Nil(t, registry)
			} else {
				require.NotNil(t, registry)
			}
		})
	}
}

func Test_isActivatable(t *testing.T) {
	tests := []struct {
		name     string
		rule     *MockRule
		expected bool
	}{
		{
			name: "rule with all required fields is activatable",
			rule: &MockRule{
				ID:   "complete-rule",
				When: newSourceMock[*EventMock]("source1"),
				Then: &MockAction[*EventMock, *EventMock]{},
				To:   &MockDestination[*EventMock]{},
			},
			expected: true,
		},
		{
			name: "rule missing ID is not activatable",
			rule: &MockRule{
				When: newSourceMock[*EventMock]("source1"),
				Then: &MockAction[*EventMock, *EventMock]{},
				To:   &MockDestination[*EventMock]{},
			},
			expected: false,
		},
		{
			name: "rule missing When is not activatable",
			rule: &MockRule{
				ID:   "no-when",
				Then: &MockAction[*EventMock, *EventMock]{},
				To:   &MockDestination[*EventMock]{},
			},
			expected: false,
		},
		{
			name: "rule missing Then is not activatable",
			rule: &MockRule{
				ID:   "no-then",
				When: newSourceMock[*EventMock]("source1"),
				To:   &MockDestination[*EventMock]{},
			},
			expected: false,
		},
		{
			name: "rule missing To is not activatable",
			rule: &MockRule{
				ID:   "no-to",
				When: newSourceMock[*EventMock]("source1"),
				Then: &MockAction[*EventMock, *EventMock]{},
			},
			expected: false,
		},
		{
			name:     "empty rule is not activatable",
			rule:     &MockRule{},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := isActivatable(tc.rule)
			require.Equal(t, tc.expected, result)
		})
	}
}

func Test_wrapCallbackMiddlewares(t *testing.T) {
	tests := []struct {
		name        string
		setup       func() (*MockRule, func() []CallbackMiddleware[*EventMock, *EventMock])
		expectError bool
		validate    func(t *testing.T, rule *MockRule)
	}{
		{
			name: "nil middleware function does not modify callback",
			setup: func() (*MockRule, func() []CallbackMiddleware[*EventMock, *EventMock]) {
				rule := &MockRule{
					ID:   "test-rule",
					When: newSourceMock[*EventMock]("source1"),
					Then: &MockAction[*EventMock, *EventMock]{},
					To:   &MockDestination[*EventMock]{},
				}
				return rule, nil
			},
			expectError: false,
			validate: func(t *testing.T, rule *MockRule) {
				require.Nil(t, rule.wrappedCallback)
			},
		},
		{
			name: "wraps callback with single middleware",
			setup: func() (*MockRule, func() []CallbackMiddleware[*EventMock, *EventMock]) {
				rule := &MockRule{
					ID:   "test-rule",
					When: newSourceMock[*EventMock]("source1"),
					Then: &MockAction[*EventMock, *EventMock]{},
					To:   &MockDestination[*EventMock]{},
				}

				middleware := &MockCallbackMiddleware[*EventMock, *EventMock]{}
				wrappedCb := func(ctx context.Context, event *EventMock, reports chan<- Report) {}
				middleware.EXPECT().Wrap(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(ctx context.Context, rule *MockRule, callback Callback[*EventMock], in *EventMock) (Callback[*EventMock], error) {
						return wrappedCb, nil
					}).Once()

				return rule, func() []CallbackMiddleware[*EventMock, *EventMock] {
					return []CallbackMiddleware[*EventMock, *EventMock]{middleware}
				}
			},
			expectError: false,
			validate: func(t *testing.T, rule *MockRule) {
				require.NotNil(t, rule.wrappedCallback)
			},
		},
		{
			name: "wraps callback with multiple middlewares in reverse order",
			setup: func() (*MockRule, func() []CallbackMiddleware[*EventMock, *EventMock]) {
				rule := &MockRule{
					ID:   "test-rule",
					When: newSourceMock[*EventMock]("source1"),
					Then: &MockAction[*EventMock, *EventMock]{},
					To:   &MockDestination[*EventMock]{},
				}

				middleware1 := &MockCallbackMiddleware[*EventMock, *EventMock]{}
				middleware2 := &MockCallbackMiddleware[*EventMock, *EventMock]{}
				wrappedCb1 := func(ctx context.Context, event *EventMock, reports chan<- Report) {}
				wrappedCb2 := func(ctx context.Context, event *EventMock, reports chan<- Report) {}

				// Should wrap in reverse: middleware2 first, then middleware1
				middleware2.EXPECT().Wrap(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(ctx context.Context, rule *MockRule, callback Callback[*EventMock], in *EventMock) (Callback[*EventMock], error) {
						return wrappedCb1, nil
					}).Once()
				middleware1.EXPECT().Wrap(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(ctx context.Context, rule *MockRule, callback Callback[*EventMock], in *EventMock) (Callback[*EventMock], error) {
						return wrappedCb2, nil
					}).Once()

				return rule, func() []CallbackMiddleware[*EventMock, *EventMock] {
					return []CallbackMiddleware[*EventMock, *EventMock]{middleware1, middleware2}
				}
			},
			expectError: false,
			validate: func(t *testing.T, rule *MockRule) {
				require.NotNil(t, rule.wrappedCallback)
			},
		},
		{
			name: "returns error when middleware wrapping fails",
			setup: func() (*MockRule, func() []CallbackMiddleware[*EventMock, *EventMock]) {
				rule := &MockRule{
					ID:   "test-rule",
					When: newSourceMock[*EventMock]("source1"),
					Then: &MockAction[*EventMock, *EventMock]{},
					To:   &MockDestination[*EventMock]{},
				}

				middleware := &MockCallbackMiddleware[*EventMock, *EventMock]{}
				middleware.On("Wrap", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Return(nil, os.ErrClosed).Once()

				return rule, func() []CallbackMiddleware[*EventMock, *EventMock] {
					return []CallbackMiddleware[*EventMock, *EventMock]{middleware}
				}
			},
			expectError: true,
			validate:    func(t *testing.T, rule *MockRule) {},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule, middlewares := tc.setup()

			err := wrapCallbackMiddlewares(t.Context(), rule, new(EventMock), middlewares)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				tc.validate(t, rule)
			}
		})
	}
}

func Test_wrapActionMiddlewares(t *testing.T) {
	tests := []struct {
		name        string
		setup       func() (*MockRule, func() []ActionMiddleware[*EventMock, *EventMock])
		expectError bool
		validate    func(t *testing.T, rule *MockRule)
	}{
		{
			name: "nil middleware function does not modify action",
			setup: func() (*MockRule, func() []ActionMiddleware[*EventMock, *EventMock]) {
				originalAction := &MockAction[*EventMock, *EventMock]{}
				rule := &MockRule{
					ID:   "test-rule",
					When: newSourceMock[*EventMock]("source1"),
					Then: originalAction,
					To:   &MockDestination[*EventMock]{},
				}
				return rule, nil
			},
			expectError: false,
			validate: func(t *testing.T, rule *MockRule) {
				require.NotNil(t, rule.Then)
			},
		},
		{
			name: "wraps action with single middleware",
			setup: func() (*MockRule, func() []ActionMiddleware[*EventMock, *EventMock]) {
				originalAction := &MockAction[*EventMock, *EventMock]{}
				rule := &MockRule{
					ID:   "test-rule",
					When: newSourceMock[*EventMock]("source1"),
					Then: originalAction,
					To:   &MockDestination[*EventMock]{},
				}

				middleware := &MockActionMiddleware[*EventMock, *EventMock]{}
				wrappedAction := &TestAction{ID: "wrapped"}
				middleware.On("Wrap", mock.Anything, *rule, originalAction, mock.Anything).
					Return(wrappedAction, nil).Once()

				return rule, func() []ActionMiddleware[*EventMock, *EventMock] {
					return []ActionMiddleware[*EventMock, *EventMock]{middleware}
				}
			},
			expectError: false,
			validate: func(t *testing.T, rule *MockRule) {
				require.NotNil(t, rule.Then)
				require.Equal(t, "wrapped", rule.Then.(*TestAction).ID)
			},
		},
		{
			name: "returns error when middleware wrapping fails",
			setup: func() (*MockRule, func() []ActionMiddleware[*EventMock, *EventMock]) {
				originalAction := &MockAction[*EventMock, *EventMock]{}
				rule := &MockRule{
					ID:   "test-rule",
					When: newSourceMock[*EventMock]("source1"),
					Then: originalAction,
					To:   &MockDestination[*EventMock]{},
				}

				middleware := &MockActionMiddleware[*EventMock, *EventMock]{}
				middleware.On("Wrap", mock.Anything, *rule, originalAction, mock.Anything).
					Return(nil, os.ErrPermission).Once()

				return rule, func() []ActionMiddleware[*EventMock, *EventMock] {
					return []ActionMiddleware[*EventMock, *EventMock]{middleware}
				}
			},
			expectError: true,
			validate:    func(t *testing.T, rule *MockRule) {},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule, middlewares := tc.setup()

			err := wrapActionMiddlewares(t.Context(), rule, new(EventMock), middlewares)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				tc.validate(t, rule)
			}
		})
	}
}

func Test_wrapDestinationMiddlewares(t *testing.T) {
	tests := []struct {
		name        string
		setup       func() (*MockRule, func() []DestinationMiddleware[*EventMock, *EventMock])
		expectError bool
		validate    func(t *testing.T, rule *MockRule)
	}{
		{
			name: "nil middleware function does not modify destination",
			setup: func() (*MockRule, func() []DestinationMiddleware[*EventMock, *EventMock]) {
				originalDest := &MockDestination[*EventMock]{}
				rule := &MockRule{
					ID:   "test-rule",
					When: newSourceMock[*EventMock]("source1"),
					Then: &MockAction[*EventMock, *EventMock]{},
					To:   originalDest,
				}
				return rule, nil
			},
			expectError: false,
			validate: func(t *testing.T, rule *MockRule) {
				require.NotNil(t, rule.To)
			},
		},
		{
			name: "wraps destination with single middleware",
			setup: func() (*MockRule, func() []DestinationMiddleware[*EventMock, *EventMock]) {
				originalDest := &MockDestination[*EventMock]{}
				rule := &MockRule{
					ID:   "test-rule",
					When: newSourceMock[*EventMock]("source1"),
					Then: &MockAction[*EventMock, *EventMock]{},
					To:   originalDest,
				}

				middleware := &MockDestinationMiddleware[*EventMock, *EventMock]{}
				wrappedDest := &TestDestination{ID: "wrapped"}
				middleware.On("Wrap", mock.Anything, *rule, originalDest, mock.Anything).
					Return(wrappedDest, nil).Once()

				return rule, func() []DestinationMiddleware[*EventMock, *EventMock] {
					return []DestinationMiddleware[*EventMock, *EventMock]{middleware}
				}
			},
			expectError: false,
			validate: func(t *testing.T, rule *MockRule) {
				require.NotNil(t, rule.To)
				require.Equal(t, "wrapped", rule.To.(*TestDestination).ID)
			},
		},
		{
			name: "returns error when middleware wrapping fails",
			setup: func() (*MockRule, func() []DestinationMiddleware[*EventMock, *EventMock]) {
				originalDest := &MockDestination[*EventMock]{}
				rule := &MockRule{
					ID:   "test-rule",
					When: newSourceMock[*EventMock]("source1"),
					Then: &MockAction[*EventMock, *EventMock]{},
					To:   originalDest,
				}

				middleware := &MockDestinationMiddleware[*EventMock, *EventMock]{}
				middleware.On("Wrap", mock.Anything, *rule, originalDest, mock.Anything).
					Return(nil, os.ErrInvalid).Once()

				return rule, func() []DestinationMiddleware[*EventMock, *EventMock] {
					return []DestinationMiddleware[*EventMock, *EventMock]{middleware}
				}
			},
			expectError: true,
			validate:    func(t *testing.T, rule *MockRule) {},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule, middlewares := tc.setup()

			err := wrapDestinationMiddlewares(t.Context(), rule, new(EventMock), middlewares)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				tc.validate(t, rule)
			}
		})
	}
}
