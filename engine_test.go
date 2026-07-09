package firehose

import (
	"context"
	"os"
	"testing"

	"github.com/emad-elsaid/boolexpr"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// testCond is a simple test condition implementation
type testCond[I any] string

func (c testCond[I]) Evaluate(_ context.Context, _ I, _ boolexpr.Symbols) (bool, error) {
	return true, nil
}

// testConditions is a slice of conditions for testing
type testConditions[I any] []Condition[I]

func (ifs testConditions[I]) Evaluate(ctx context.Context, event I, syms boolexpr.Symbols) (bool, error) {
	for _, cond := range ifs {
		pass, err := cond.Evaluate(ctx, event, syms)
		if err != nil {
			return false, err
		}
		if !pass {
			return false, nil
		}
	}
	return true, nil
}

// assertRulesEqual compares two rules ignoring the If field structure (only checks non-nil)
func assertRulesEqual(t *testing.T, expected, actual *MockRule) {
	t.Helper()

	if expected == nil && actual == nil {
		return
	}

	require.NotNil(t, expected, "expected should not be nil")
	require.NotNil(t, actual, "actual should not be nil")

	require.Equal(t, expected.ID, actual.ID, "ID mismatch")
	require.Equal(t, expected.From, actual.From, "On mismatch")
	require.Equal(t, expected.Select, actual.Select, "Select mismatch")
	require.Equal(t, expected.Into, actual.Into, "Into mismatch")
	require.Equal(t, expected.Middlewares, actual.Middlewares, "Middlewares mismatch")

	// For If, just check nil/non-nil matches
	if expected.Where == nil {
		require.Nil(t, actual.Where, "If should be nil")
	} else {
		require.NotNil(t, actual.Where, "If should not be nil")
	}

	// Recursively check SubRules
	require.Equal(t, len(expected.SubRules), len(actual.SubRules), "SubRules length mismatch")
	for i := range expected.SubRules {
		assertRulesEqual(t, &expected.SubRules[i], &actual.SubRules[i])
	}
}

func TestAddRule(t *testing.T) {
	t.Run("add first rule to nil registry", func(t *testing.T) {
		rule := &MockRule{
			ID:     "rule1",
			From:   NewMockSource[*EventMock](t),
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		}

		result, err := AddRule(t.Context(), nil, rule)

		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, result, result.getNext())
		require.Equal(t, result, result.getPrev())
	})

	t.Run("add second rule to existing registry", func(t *testing.T) {
		rule1 := &MockRule{
			ID:     "rule1",
			From:   NewMockSource[*EventMock](t),
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		}
		registry, _ := AddRule(t.Context(), nil, rule1)

		rule2 := &MockRule{
			ID:     "rule2",
			From:   NewMockSource[*EventMock](t),
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		}

		result, err := AddRule(t.Context(), registry, rule2)

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
			ID:     "rule1",
			From:   NewMockSource[*EventMock](t),
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		}
		registry, _ := AddRule(t.Context(), nil, rule1)

		rule2 := &MockRule{
			ID:     "rule2",
			From:   NewMockSource[*EventMock](t),
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		}
		registry, _ = AddRule(t.Context(), registry, rule2)

		rule3 := &MockRule{
			ID:     "rule3",
			From:   NewMockSource[*EventMock](t),
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		}

		result, err := AddRule(t.Context(), registry, rule3)

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

	t.Run("add rule with Environment that doesn't match current ENV", func(t *testing.T) {
		t.Setenv("ENV", "staging")
		rule := &MockRule{
			ID:           "rule1",
			Environments: []string{"test"},
			From:         NewMockSource[*EventMock](t),
			Select:       &MockAction[*EventMock, *EventMock]{},
			Into:         &MockDestination[*EventMock]{},
		}

		result, err := AddRule(t.Context(), nil, rule)

		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("add rule with Environment that matches current ENV", func(t *testing.T) {
		t.Setenv("ENV", "test")
		rule := &MockRule{
			ID:           "rule1",
			Environments: []string{"test"},
			From:         NewMockSource[*EventMock](t),
			Select:       &MockAction[*EventMock, *EventMock]{},
			Into:         &MockDestination[*EventMock]{},
		}

		result, err := AddRule(t.Context(), nil, rule)

		require.NoError(t, err)
		require.NotNil(t, result)
	})
}

func TestAddRuleSameSourceChaining(t *testing.T) {
	t.Parallel()

	t.Run("first rule with a source has no same-source links", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)
		rule := &MockRule{
			ID:     "rule1",
			From:   source,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		}

		result, err := AddRule(t.Context(), nil, rule)

		require.NoError(t, err)
		require.NotNil(t, result)

		ruleResult := result.(*Rule[*EventMock, *EventMock])
		require.Nil(t, ruleResult.nextSameSource, "first rule should have no next same source")
		require.Nil(t, ruleResult.prevSameSource, "first rule should have no prev same source")
	})

	t.Run("two rules with different sources have no same-source links", func(t *testing.T) {
		source1 := NewMockSource[*EventMock](t)
		source2 := NewMockSource[*EventMock](t)

		registry, _ := AddRule(t.Context(), nil, &MockRule{
			ID:     "rule1",
			From:   source1,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		})

		rule2 := &MockRule{
			ID:     "rule2",
			From:   source2,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		}

		result, err := AddRule(t.Context(), registry, rule2)

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
		source := NewMockSource[*EventMock](t)

		registry, _ := AddRule(t.Context(), nil, &MockRule{
			ID:     "rule1",
			From:   source,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		})

		rule2 := &MockRule{
			ID:     "rule2",
			From:   source,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		}

		result, err := AddRule(t.Context(), registry, rule2)

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
		source := NewMockSource[*EventMock](t)

		registry, _ := AddRule(t.Context(), nil, &MockRule{
			ID:     "rule1",
			From:   source,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		})

		registry, _ = AddRule(t.Context(), registry, &MockRule{
			ID:     "rule2",
			From:   source,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		})

		rule3 := &MockRule{
			ID:     "rule3",
			From:   source,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		}

		result, err := AddRule(t.Context(), registry, rule3)

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
		sourceA := NewMockSource[*EventMock](t)
		sourceB := NewMockSource[*EventMock](t)

		// Add first rule with sourceA
		registry, _ := AddRule(t.Context(), nil, &MockRule{
			ID:     "rule1",
			From:   sourceA,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		})

		// Add second rule with sourceA
		registry, _ = AddRule(t.Context(), registry, &MockRule{
			ID:     "rule3",
			From:   sourceA,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		})

		// Add rule with sourceB
		registry, _ = AddRule(t.Context(), registry, &MockRule{
			ID:     "rule3",
			From:   sourceB,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		})

		// Add third rule with sourceA
		result, err := AddRule(t.Context(), registry, &MockRule{
			ID:     "rule4",
			From:   sourceA,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		})

		require.NoError(t, err)

		// Collect all rules
		rules := make([]*Rule[*EventMock, *EventMock], 0, 4)
		current := result
		for i := 0; i < 4; i++ {
			rules = append(rules, current.(*Rule[*EventMock, *EventMock]))
			current = current.getNext()
		}

		// Identify rules by source instance (pointer comparison)
		var sourceARules []*Rule[*EventMock, *EventMock]
		var sourceBRules []*Rule[*EventMock, *EventMock]

		for _, r := range rules {
			if r.From == sourceA {
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
		source := NewMockSource[*EventMock](t)

		registry, _ := AddRule(t.Context(), nil, &MockRule{
			ID:     "rule1",
			From:   source,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		})

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		var receivedErrors []error
		errorHandler := func(err error) {
			receivedErrors = append(receivedErrors, err)
		}

		source.On("Start", ctx, mock.Anything).Return(ctx, nil).Once()

		Start(ctx, registry, errorHandler)

		cancel()
		Wait(registry, errorHandler)

		require.Len(t, receivedErrors, 1) // context.Canceled when we cancel the context

		rule := registry.(*Rule[*EventMock, *EventMock])
		require.NotNil(t, rule.ctx)
	})

	t.Run("start multiple rules with different sources", func(t *testing.T) {
		source1 := NewMockSource[*EventMock](t)
		source2 := NewMockSource[*EventMock](t)

		registry, _ := AddRule(t.Context(), nil, &MockRule{
			ID:     "rule1",
			From:   source1,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		})
		registry, _ = AddRule(t.Context(), registry, &MockRule{
			ID:     "rule2",
			From:   source2,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		})

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		var receivedErrors []error
		errorHandler := func(err error) {
			receivedErrors = append(receivedErrors, err)
		}

		source1.On("Start", ctx, mock.Anything).Return(ctx, nil).Once()
		source2.On("Start", ctx, mock.Anything).Return(ctx, nil).Once()

		Start(ctx, registry, errorHandler)

		cancel()
		Wait(registry, errorHandler)

		require.Len(t, receivedErrors, 2) // context.Canceled for each source
	})

	t.Run("start rule with source error", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)

		registry, _ := AddRule(t.Context(), nil, &MockRule{
			ID:     "rule1",
			From:   source,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		})

		ctx := t.Context()

		var receivedErrors []error
		errorHandler := func(err error) {
			receivedErrors = append(receivedErrors, err)
		}

		source.On("Start", ctx, mock.Anything).Return(ctx, os.ErrClosed).Once()

		Start(ctx, registry, errorHandler)
		Wait(registry, errorHandler)

		require.Len(t, receivedErrors, 1)
	})

	t.Run("start multiple rules with same source", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)

		registry, _ := AddRule(t.Context(), nil, &MockRule{
			ID:     "rule1",
			From:   source,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		})
		registry, _ = AddRule(t.Context(), registry, &MockRule{
			ID:     "rule2",
			From:   source,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		})

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		var receivedErrors []error
		errorHandler := func(err error) {
			receivedErrors = append(receivedErrors, err)
		}

		source.On("Start", ctx, mock.Anything).Return(ctx, nil).Once()

		Start(ctx, registry, errorHandler)

		cancel()
		Wait(registry, errorHandler)

		require.Len(t, receivedErrors, 1) // Only one context.Canceled because source is shared
	})
}

func Test_inherit(t *testing.T) {
	parentSource1 := NewMockSource[*EventMock](t)
	parentAction1 := NewMockAction[*EventMock, *EventMock](t)
	parentDest1 := NewMockDestination[*EventMock](t)

	parentSource2 := NewMockSource[*EventMock](t)
	childSource2 := NewMockSource[*EventMock](t)
	childAction2 := NewMockAction[*EventMock, *EventMock](t)
	childDest2 := NewMockDestination[*EventMock](t)

	parentSource3 := NewMockSource[*EventMock](t)
	childSource3 := NewMockSource[*EventMock](t)
	childAction3 := NewMockAction[*EventMock, *EventMock](t)
	childDest3 := NewMockDestination[*EventMock](t)

	tcs := []struct {
		name     string
		parent   *MockRule
		child    *MockRule
		expected *MockRule
	}{
		{
			name: "child inherits parent's exported fields",
			parent: &MockRule{
				ID:     "parent",
				From:   parentSource1,
				Where:  testCond[*EventMock]("parent condition"),
				Select: parentAction1,
				Into:   parentDest1,
			},
			child: &MockRule{},
			expected: &MockRule{
				ID:     "parent/1",
				From:   parentSource1,
				Where:  testCond[*EventMock]("parent condition"),
				Select: parentAction1,
				Into:   parentDest1,
			},
		},
		{
			name: "child does not override its own fields",
			parent: &MockRule{
				ID:     "parent",
				From:   parentSource2,
				Where:  testCond[*EventMock]("parent condition"),
				Select: &MockAction[*EventMock, *EventMock]{},
				Into:   &MockDestination[*EventMock]{},
			},
			child: &MockRule{
				ID:     "child",
				From:   childSource2,
				Where:  testCond[*EventMock]("child condition"),
				Select: childAction2,
				Into:   childDest2,
			},
			expected: &MockRule{
				ID:     "parent/child",
				From:   childSource2,
				Where:  testCond[*EventMock](""), // non-nil placeholder
				Select: childAction2,
				Into:   childDest2,
			},
		},
		{
			name: "child with no environment inherits parent's environment",
			parent: &MockRule{
				ID:           "parent",
				Environments: []string{"test", "staging"},
				From:         parentSource2,
				Where:        testCond[*EventMock]("parent condition"),
				Select:       &MockAction[*EventMock, *EventMock]{},
				Into:         &MockDestination[*EventMock]{},
			},
			child: &MockRule{
				ID:     "child",
				From:   childSource2,
				Where:  testCond[*EventMock]("child condition"),
				Select: childAction2,
				Into:   childDest2,
			},
			expected: &MockRule{
				ID:           "parent/child",
				Environments: []string{"test", "staging"},
				From:         childSource2,
				Where:        testCond[*EventMock](""), // non-nil placeholder
				Select:       childAction2,
				Into:         childDest2,
			},
		},
		{
			name: "child with its own environment does not inherit parent's environment",
			parent: &MockRule{
				ID:           "parent",
				Environments: []string{"test"},
				From:         parentSource2,
				Where:        testCond[*EventMock]("parent condition"),
				Select:       &MockAction[*EventMock, *EventMock]{},
				Into:         &MockDestination[*EventMock]{},
			},
			child: &MockRule{
				ID:           "child",
				Environments: []string{"staging"},
				From:         childSource2,
				Where:        testCond[*EventMock]("child condition"),
				Select:       childAction2,
				Into:         childDest2,
			},
			expected: &MockRule{
				ID:           "parent/child",
				Environments: []string{"staging"},
				From:         childSource2,
				Where:        testCond[*EventMock](""), // non-nil placeholder
				Select:       childAction2,
				Into:         childDest2,
			},
		},

		{
			name: "child inherits only missing fields from parent",
			parent: &MockRule{
				ID:     "parent",
				From:   parentSource3,
				Where:  testCond[*EventMock]("parent condition"),
				Select: &MockAction[*EventMock, *EventMock]{},
				Into:   &MockDestination[*EventMock]{},
			},
			child: &MockRule{
				ID:     "child",
				From:   childSource3,
				Select: childAction3,
				Into:   childDest3,
			},
			expected: &MockRule{
				ID:     "parent/child",
				From:   childSource3,
				Where:  testCond[*EventMock]("parent condition"),
				Select: childAction3,
				Into:   childDest3,
			},
		},

		{
			name: "child condition should be prepended with parent conditions",
			parent: &MockRule{
				Where: testConditions[*EventMock]{
					testCond[*EventMock]("parent condition"),
				},
			},
			child: &MockRule{
				Where: testConditions[*EventMock]{
					testCond[*EventMock]("child condition"),
				},
			},
			expected: &MockRule{
				ID:    "1",
				Where: testCond[*EventMock](""), // non-nil placeholder
			},
		},
		{
			name: "child output condition should be prepended with parent output conditions",
			parent: &MockRule{
				Having: testConditions[*EventMock]{
					testCond[*EventMock]("parent output condition"),
				},
			},
			child: &MockRule{
				Having: testConditions[*EventMock]{
					testCond[*EventMock]("child output condition"),
				},
			},
			expected: &MockRule{
				ID:     "1",
				Having: testCond[*EventMock](""), // non-nil placeholder
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
			assertRulesEqual(t, tc.expected, tc.child)
		})
	}
}

func Test_flatten(t *testing.T) {
	parentSource := NewMockSource[*EventMock](t)
	parentAction := NewMockAction[*EventMock, *EventMock](t)
	parentDest := NewMockDestination[*EventMock](t)

	childSource := NewMockSource[*EventMock](t)
	childAction := NewMockAction[*EventMock, *EventMock](t)
	childDest := NewMockDestination[*EventMock](t)

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
				From: parentSource,
				Where: testConditions[*EventMock]{
					testCond[*EventMock]("condition"),
				},
			},
			expected: &MockRule{
				ID:   "parent",
				From: parentSource,
				Where: testConditions[*EventMock]{
					testCond[*EventMock]("condition"),
				},
			},
		},
		{
			name: "single subrule inherits parent fields",
			rule: &MockRule{
				ID:   "parent",
				From: parentSource,
				Where: testConditions[*EventMock]{
					testCond[*EventMock]("parent condition"),
				},
				Select: parentAction,
				Into:   parentDest,
				SubRules: []MockRule{
					{
						ID: "child",
					},
				},
			},
			expected: &MockRule{
				ID:   "parent",
				From: parentSource,
				Where: testConditions[*EventMock]{
					testCond[*EventMock]("parent condition"),
				},
				Select: parentAction,
				Into:   parentDest,
				SubRules: []MockRule{
					{
						ID:   "parent/child",
						From: parentSource,
						Where: testConditions[*EventMock]{
							testCond[*EventMock]("parent condition"),
						},
						Select: parentAction,
						Into:   parentDest,
					},
				},
			},
		},
		{
			name: "multiple subrules inherit parent fields",
			rule: &MockRule{
				ID:   "parent",
				From: parentSource,
				Where: testConditions[*EventMock]{
					testCond[*EventMock]("parent condition"),
				},
				Select: &MockAction[*EventMock, *EventMock]{},
				Into:   &MockDestination[*EventMock]{},
				SubRules: []MockRule{
					{ID: "child1"},
					{ID: "child2"},
					{ID: "child3"},
				},
			},
			expected: &MockRule{
				ID:   "parent",
				From: parentSource,
				Where: testConditions[*EventMock]{
					testCond[*EventMock]("parent condition"),
				},
				Select: &MockAction[*EventMock, *EventMock]{},
				Into:   &MockDestination[*EventMock]{},
				SubRules: []MockRule{
					{
						ID:   "parent/child1",
						From: parentSource,
						Where: testConditions[*EventMock]{
							testCond[*EventMock]("parent condition"),
						},
						Select: &MockAction[*EventMock, *EventMock]{},
						Into:   &MockDestination[*EventMock]{},
					},
					{
						ID:   "parent/child2",
						From: parentSource,
						Where: testConditions[*EventMock]{
							testCond[*EventMock]("parent condition"),
						},
						Select: &MockAction[*EventMock, *EventMock]{},
						Into:   &MockDestination[*EventMock]{},
					},
					{
						ID:   "parent/child3",
						From: parentSource,
						Where: testConditions[*EventMock]{
							testCond[*EventMock]("parent condition"),
						},
						Select: &MockAction[*EventMock, *EventMock]{},
						Into:   &MockDestination[*EventMock]{},
					},
				},
			},
		},
		{
			name: "nested subrules inherit recursively",
			rule: &MockRule{
				ID:   "grandparent",
				From: parentSource,
				Where: testConditions[*EventMock]{
					testCond[*EventMock]("grandparent condition"),
				},
				Select: &MockAction[*EventMock, *EventMock]{},
				Into:   &MockDestination[*EventMock]{},
				SubRules: []MockRule{
					{
						ID: "parent",
						Where: testConditions[*EventMock]{
							testCond[*EventMock]("parent condition"),
						},
						SubRules: []MockRule{
							{
								ID: "child",
							},
						},
					},
				},
			},
			expected: &MockRule{
				ID:     "grandparent",
				From:   parentSource,
				Where:  testConditions[*EventMock]{testCond[*EventMock]("")}, // non-nil placeholder
				Select: &MockAction[*EventMock, *EventMock]{},
				Into:   &MockDestination[*EventMock]{},
				SubRules: []MockRule{
					{
						ID:     "grandparent/parent",
						From:   parentSource,
						Where:  testCond[*EventMock](""), // non-nil placeholder
						Select: &MockAction[*EventMock, *EventMock]{},
						Into:   &MockDestination[*EventMock]{},
						SubRules: []MockRule{
							{
								ID:     "grandparent/parent/child",
								From:   parentSource,
								Where:  testCond[*EventMock](""), // non-nil placeholder
								Select: &MockAction[*EventMock, *EventMock]{},
								Into:   &MockDestination[*EventMock]{},
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
				From: parentSource,
				Where: testConditions[*EventMock]{
					testCond[*EventMock]("parent condition"),
				},
				Select: parentAction,
				Into:   parentDest,
				SubRules: []MockRule{
					{
						ID:   "child",
						From: childSource,
						Where: testConditions[*EventMock]{
							testCond[*EventMock]("child condition"),
						},
						Select: childAction,
						Into:   childDest,
					},
				},
			},
			expected: &MockRule{
				ID:     "parent",
				From:   parentSource,
				Where:  testConditions[*EventMock]{testCond[*EventMock]("")}, // non-nil placeholder
				Select: parentAction,
				Into:   parentDest,
				SubRules: []MockRule{
					{
						ID:     "parent/child",
						From:   childSource,
						Where:  testCond[*EventMock](""), // non-nil placeholder
						Select: childAction,
						Into:   childDest,
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
			assertRulesEqual(t, tc.expected, tc.rule)
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
				ID:     "missing-when",
				Select: &MockAction[*EventMock, *EventMock]{},
				Into:   &MockDestination[*EventMock]{},
			},
			expectError: true,
		},
		{
			name: "returns error for rule missing Select",
			rule: &MockRule{
				ID:   "missing-then",
				From: NewMockSource[*EventMock](t),
				Into: &MockDestination[*EventMock]{},
			},
			expectError: true,
		},
		{
			name: "returns error for rule missing Into",
			rule: &MockRule{
				ID:     "missing-to",
				From:   NewMockSource[*EventMock](t),
				Select: &MockAction[*EventMock, *EventMock]{},
			},
			expectError: true,
		},
		{
			name: "returns error for rule missing ID",
			rule: &MockRule{
				From:   NewMockSource[*EventMock](t),
				Select: &MockAction[*EventMock, *EventMock]{},
				Into:   &MockDestination[*EventMock]{},
			},
			expectError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := addSingleRule(
				t.Context(), nil, tc.rule,
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
				ID:     "parent",
				From:   NewMockSource[*EventMock](t),
				Select: &MockAction[*EventMock, *EventMock]{},
				Into:   &MockDestination[*EventMock]{},
				SubRules: []MockRule{
					{
						ID:     "child1",
						From:   NewMockSource[*EventMock](t),
						Select: &MockAction[*EventMock, *EventMock]{},
						Into:   &MockDestination[*EventMock]{},
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
						ID:     "child",
						From:   NewMockSource[*EventMock](t),
						Select: &MockAction[*EventMock, *EventMock]{},
						Into:   &MockDestination[*EventMock]{},
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
				t.Context(), nil, tc.rule,
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
				ID:     "complete-rule",
				From:   NewMockSource[*EventMock](t),
				Select: &MockAction[*EventMock, *EventMock]{},
				Into:   &MockDestination[*EventMock]{},
			},
			expected: true,
		},
		{
			name: "rule missing ID is not activatable",
			rule: &MockRule{
				From:   NewMockSource[*EventMock](t),
				Select: &MockAction[*EventMock, *EventMock]{},
				Into:   &MockDestination[*EventMock]{},
			},
			expected: false,
		},
		{
			name: "rule missing When is not activatable",
			rule: &MockRule{
				ID:     "no-when",
				Select: &MockAction[*EventMock, *EventMock]{},
				Into:   &MockDestination[*EventMock]{},
			},
			expected: false,
		},
		{
			name: "rule missing Select is not activatable",
			rule: &MockRule{
				ID:   "no-then",
				From: NewMockSource[*EventMock](t),
				Into: &MockDestination[*EventMock]{},
			},
			expected: false,
		},
		{
			name: "rule missing Into is not activatable",
			rule: &MockRule{
				ID:     "no-to",
				From:   NewMockSource[*EventMock](t),
				Select: &MockAction[*EventMock, *EventMock]{},
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

func Test_wrapMiddlewares(t *testing.T) {
	tests := []struct {
		name        string
		setup       func() *MockRule
		expectError bool
		validate    func(t *testing.T, rule *MockRule)
	}{
		{
			name: "nil middleware function does not modify callback",
			setup: func() *MockRule {
				rule := &MockRule{
					ID:     "test-rule",
					From:   NewMockSource[*EventMock](t),
					Select: &MockAction[*EventMock, *EventMock]{},
					Into:   &MockDestination[*EventMock]{},
				}
				return rule
			},
			expectError: false,
			validate: func(t *testing.T, rule *MockRule) {
				require.Nil(t, rule.wrappedCallback)
			},
		},
		{
			name: "wraps callback with single middleware",
			setup: func() *MockRule {
				middleware := &MockMiddleware[*EventMock, *EventMock]{}
				wrappedCb := func(ctx context.Context, event *EventMock, report ReportFunc) {}
				middleware.EXPECT().WrapCallback(mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(ctx context.Context, rule *MockRule, callback Callback[*EventMock]) (Callback[*EventMock], error) {
						return wrappedCb, nil
					}).Once()
				middleware.EXPECT().WrapAction(mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(ctx context.Context, rule *MockRule, action Action[*EventMock, *EventMock]) (Action[*EventMock, *EventMock], error) {
						return action, nil
					}).Maybe()
				middleware.EXPECT().WrapDestination(mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(ctx context.Context, rule *MockRule, dest Destination[*EventMock]) (Destination[*EventMock], error) {
						return dest, nil
					}).Maybe()

				rule := &MockRule{
					ID:          "test-rule",
					From:        NewMockSource[*EventMock](t),
					Select:      &MockAction[*EventMock, *EventMock]{},
					Into:        &MockDestination[*EventMock]{},
					Middlewares: []Middleware[*EventMock, *EventMock]{middleware},
				}

				return rule
			},
			expectError: false,
			validate: func(t *testing.T, rule *MockRule) {
				require.NotNil(t, rule.wrappedCallback)
			},
		},
		{
			name: "wraps callback with multiple middlewares in reverse order",
			setup: func() *MockRule {

				middleware1 := &MockMiddleware[*EventMock, *EventMock]{}
				middleware2 := &MockMiddleware[*EventMock, *EventMock]{}
				wrappedCb1 := func(ctx context.Context, event *EventMock, report ReportFunc) {}
				wrappedCb2 := func(ctx context.Context, event *EventMock, report ReportFunc) {}

				// Should wrap in reverse: middleware2 first, then middleware1
				middleware2.EXPECT().WrapCallback(mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(ctx context.Context, rule *MockRule, callback Callback[*EventMock]) (Callback[*EventMock], error) {
						return wrappedCb1, nil
					}).Once()
				middleware2.EXPECT().WrapAction(mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(ctx context.Context, rule *MockRule, action Action[*EventMock, *EventMock]) (Action[*EventMock, *EventMock], error) {
						return action, nil
					}).Maybe()
				middleware2.EXPECT().WrapDestination(mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(ctx context.Context, rule *MockRule, dest Destination[*EventMock]) (Destination[*EventMock], error) {
						return dest, nil
					}).Maybe()

				middleware1.EXPECT().WrapCallback(mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(ctx context.Context, rule *MockRule, callback Callback[*EventMock]) (Callback[*EventMock], error) {
						return wrappedCb2, nil
					}).Once()
				middleware1.EXPECT().WrapAction(mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(ctx context.Context, rule *MockRule, action Action[*EventMock, *EventMock]) (Action[*EventMock, *EventMock], error) {
						return action, nil
					}).Maybe()
				middleware1.EXPECT().WrapDestination(mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(ctx context.Context, rule *MockRule, dest Destination[*EventMock]) (Destination[*EventMock], error) {
						return dest, nil
					}).Maybe()

				rule := &MockRule{
					ID:          "test-rule",
					From:        NewMockSource[*EventMock](t),
					Select:      &MockAction[*EventMock, *EventMock]{},
					Into:        &MockDestination[*EventMock]{},
					Middlewares: []Middleware[*EventMock, *EventMock]{middleware1, middleware2},
				}

				return rule
			},
			expectError: false,
			validate: func(t *testing.T, rule *MockRule) {
				require.NotNil(t, rule.wrappedCallback)
			},
		},
		{
			name: "returns error when middleware wrapping fails",
			setup: func() *MockRule {
				middleware := &MockMiddleware[*EventMock, *EventMock]{}
				middleware.On("WrapCallback", mock.Anything, mock.Anything, mock.Anything).
					Return(nil, os.ErrClosed).Once()

				rule := &MockRule{
					ID:          "test-rule",
					From:        NewMockSource[*EventMock](t),
					Select:      &MockAction[*EventMock, *EventMock]{},
					Into:        &MockDestination[*EventMock]{},
					Middlewares: []Middleware[*EventMock, *EventMock]{middleware},
				}

				return rule
			},
			expectError: true,
			validate:    func(t *testing.T, rule *MockRule) {},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := tc.setup()

			err := wrapMiddlewares(t.Context(), rule)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				tc.validate(t, rule)
			}
		})
	}
}

func Test_wrapMiddlewares_action(t *testing.T) {
	tests := []struct {
		name        string
		setup       func() *MockRule
		expectError bool
		validate    func(t *testing.T, rule *MockRule)
	}{
		{
			name: "nil middleware function does not modify action",
			setup: func() *MockRule {
				originalAction := &MockAction[*EventMock, *EventMock]{}
				rule := &MockRule{
					ID:     "test-rule",
					From:   NewMockSource[*EventMock](t),
					Select: originalAction,
					Into:   &MockDestination[*EventMock]{},
				}
				return rule
			},
			expectError: false,
			validate: func(t *testing.T, rule *MockRule) {
				require.NotNil(t, rule.Select)
			},
		},
		{
			name: "wraps action with single middleware",
			setup: func() *MockRule {
				originalAction := &MockAction[*EventMock, *EventMock]{}

				middleware := &MockMiddleware[*EventMock, *EventMock]{}
				wrappedAction := NewMockAction[*EventMock, *EventMock](t)
				middleware.EXPECT().WrapCallback(mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(ctx context.Context, rule *MockRule, callback Callback[*EventMock]) (Callback[*EventMock], error) {
						return callback, nil
					}).Maybe()
				middleware.EXPECT().WrapAction(mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(ctx context.Context, rule *MockRule, action Action[*EventMock, *EventMock]) (Action[*EventMock, *EventMock], error) {
						return wrappedAction, nil
					}).Once()
				middleware.EXPECT().WrapDestination(mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(ctx context.Context, rule *MockRule, dest Destination[*EventMock]) (Destination[*EventMock], error) {
						return dest, nil
					}).Maybe()

				rule := &MockRule{
					ID:          "test-rule",
					From:        NewMockSource[*EventMock](t),
					Select:      originalAction,
					Into:        &MockDestination[*EventMock]{},
					Middlewares: []Middleware[*EventMock, *EventMock]{middleware},
				}

				return rule
			},
			expectError: false,
			validate: func(t *testing.T, rule *MockRule) {
				require.NotNil(t, rule.actionWrappers)
			},
		},
		{
			name: "returns error when middleware wrapping fails",
			setup: func() *MockRule {
				originalAction := &MockAction[*EventMock, *EventMock]{}

				middleware := &MockMiddleware[*EventMock, *EventMock]{}
				middleware.EXPECT().WrapCallback(mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(ctx context.Context, rule *MockRule, callback Callback[*EventMock]) (Callback[*EventMock], error) {
						return callback, nil
					}).Maybe()
				middleware.EXPECT().WrapAction(mock.Anything, mock.Anything, mock.Anything).
					Return(nil, os.ErrPermission).Once()

				rule := &MockRule{
					ID:          "test-rule",
					From:        NewMockSource[*EventMock](t),
					Select:      originalAction,
					Into:        &MockDestination[*EventMock]{},
					Middlewares: []Middleware[*EventMock, *EventMock]{middleware},
				}

				return rule
			},
			expectError: true,
			validate:    func(t *testing.T, rule *MockRule) {},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := tc.setup()

			err := wrapMiddlewares(t.Context(), rule)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				tc.validate(t, rule)
			}
		})
	}
}

func Test_wrapMiddlewares_destination(t *testing.T) {
	tests := []struct {
		name        string
		setup       func() *MockRule
		expectError bool
		validate    func(t *testing.T, rule *MockRule)
	}{
		{
			name: "nil middleware function does not modify destination",
			setup: func() *MockRule {
				originalDest := &MockDestination[*EventMock]{}
				rule := &MockRule{
					ID:     "test-rule",
					From:   NewMockSource[*EventMock](t),
					Select: &MockAction[*EventMock, *EventMock]{},
					Into:   originalDest,
				}
				return rule
			},
			expectError: false,
			validate: func(t *testing.T, rule *MockRule) {
				require.NotNil(t, rule.Into)
			},
		},
		{
			name: "wraps destination with single middleware",
			setup: func() *MockRule {
				originalDest := &MockDestination[*EventMock]{}

				middleware := &MockMiddleware[*EventMock, *EventMock]{}
				wrappedDest := NewMockDestination[*EventMock](t)
				middleware.EXPECT().WrapCallback(mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(ctx context.Context, rule *MockRule, callback Callback[*EventMock]) (Callback[*EventMock], error) {
						return callback, nil
					}).Maybe()
				middleware.EXPECT().WrapAction(mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(ctx context.Context, rule *MockRule, action Action[*EventMock, *EventMock]) (Action[*EventMock, *EventMock], error) {
						return action, nil
					}).Maybe()
				middleware.EXPECT().WrapDestination(mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(ctx context.Context, rule *MockRule, dest Destination[*EventMock]) (Destination[*EventMock], error) {
						return wrappedDest, nil
					}).Once()

				rule := &MockRule{
					ID:          "test-rule",
					From:        NewMockSource[*EventMock](t),
					Select:      &MockAction[*EventMock, *EventMock]{},
					Into:        originalDest,
					Middlewares: []Middleware[*EventMock, *EventMock]{middleware},
				}
				return rule
			},
			expectError: false,
			validate: func(t *testing.T, rule *MockRule) {
				require.NotNil(t, rule.destinationWrappers)
			},
		},
		{
			name: "returns error when middleware wrapping fails",
			setup: func() *MockRule {
				originalDest := &MockDestination[*EventMock]{}

				middleware := &MockMiddleware[*EventMock, *EventMock]{}
				middleware.EXPECT().WrapCallback(mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(ctx context.Context, rule *MockRule, callback Callback[*EventMock]) (Callback[*EventMock], error) {
						return callback, nil
					}).Maybe()
				middleware.EXPECT().WrapAction(mock.Anything, mock.Anything, mock.Anything).
					RunAndReturn(func(ctx context.Context, rule *MockRule, action Action[*EventMock, *EventMock]) (Action[*EventMock, *EventMock], error) {
						return action, nil
					}).Maybe()
				middleware.EXPECT().WrapDestination(mock.Anything, mock.Anything, mock.Anything).
					Return(nil, os.ErrInvalid).Once()
				rule := &MockRule{
					ID:          "test-rule",
					From:        NewMockSource[*EventMock](t),
					Select:      &MockAction[*EventMock, *EventMock]{},
					Into:        originalDest,
					Middlewares: []Middleware[*EventMock, *EventMock]{middleware},
				}

				return rule
			},
			expectError: true,
			validate:    func(t *testing.T, rule *MockRule) {},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rule := tc.setup()

			err := wrapMiddlewares(t.Context(), rule)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				tc.validate(t, rule)
			}
		})
	}
}

func TestMiddlewareActuallyExecuted(t *testing.T) {
	t.Parallel()

	t.Run("action middleware wraps and is called during rule execution", func(t *testing.T) {
		innerAction := NewMockAction[*EventMock, *EventMock](t)
		event := &EventMock{}
		expectedOutput := &EventMock{}

		innerAction.On("Process", mock.Anything, event, mock.Anything).
			Return(expectedOutput, Report{}).Once()

		middleware := &MockMiddleware[*EventMock, *EventMock]{}
		wrappedAction := NewMockAction[*EventMock, *EventMock](t)

		middleware.EXPECT().WrapCallback(mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, rule *MockRule, callback Callback[*EventMock]) (Callback[*EventMock], error) {
				return callback, nil
			}).Maybe()
		middleware.EXPECT().WrapAction(mock.Anything, mock.Anything, mock.Anything).
			Return(wrappedAction, nil).Once()
		middleware.EXPECT().WrapDestination(mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, rule *MockRule, dest Destination[*EventMock]) (Destination[*EventMock], error) {
				return dest, nil
			}).Maybe()

		wrappedAction.On("Process", mock.Anything, event, mock.Anything).
			Run(func(args mock.Arguments) {
				innerAction.Process(args.Get(0).(context.Context), event, args.Get(2).(boolexpr.Symbols))
			}).
			Return(expectedOutput, Report{}).Once()

		dest := &MockDestination[*EventMock]{}
		dest.On("Send", mock.Anything, expectedOutput).
			Return(Report{}).Once()

		rule := &MockRule{
			ID:          "test-rule",
			From:        NewMockSource[*EventMock](t),
			Select:      innerAction,
			Into:        dest,
			Middlewares: []Middleware[*EventMock, *EventMock]{middleware},
		}

		ctx := context.Background()
		registry, err := AddRule(
			ctx,
			nil,
			rule,
		)
		require.NoError(t, err)
		require.NotNil(t, registry)

		syms := boolexpr.NewCachedMap(map[string]any{})
		rule.Run(ctx, event, syms, func(Report) {})

		wrappedAction.AssertExpectations(t)
		innerAction.AssertExpectations(t)
		dest.AssertExpectations(t)
		middleware.AssertExpectations(t)
	})
}

func TestDestinationMiddlewareActuallyExecuted(t *testing.T) {
	t.Parallel()

	t.Run("destination middleware wraps and is called during rule execution", func(t *testing.T) {
		action := NewMockAction[*EventMock, *EventMock](t)
		event := &EventMock{}
		output := &EventMock{}

		action.On("Process", mock.Anything, event, mock.Anything).
			Return(output, Report{}).Once()

		innerDest := &MockDestination[*EventMock]{}
		innerDest.On("Send", mock.Anything, output).
			Return(Report{}).Once()

		middleware := &MockMiddleware[*EventMock, *EventMock]{}
		wrappedDest := &MockDestination[*EventMock]{}

		middleware.EXPECT().WrapCallback(mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, rule *MockRule, callback Callback[*EventMock]) (Callback[*EventMock], error) {
				return callback, nil
			}).Maybe()
		middleware.EXPECT().WrapAction(mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, rule *MockRule, action Action[*EventMock, *EventMock]) (Action[*EventMock, *EventMock], error) {
				return action, nil
			}).Maybe()
		middleware.EXPECT().WrapDestination(mock.Anything, mock.Anything, mock.Anything).
			Return(wrappedDest, nil).Once()

		wrappedDest.On("Send", mock.Anything, output).
			Run(func(args mock.Arguments) {
				innerDest.Send(args.Get(0).(context.Context), output)
			}).
			Return(Report{}).Once()

		rule := &MockRule{
			ID:          "test-rule",
			From:        NewMockSource[*EventMock](t),
			Select:      action,
			Into:        innerDest,
			Middlewares: []Middleware[*EventMock, *EventMock]{middleware},
		}

		ctx := context.Background()
		registry, err := AddRule(
			ctx,
			nil,
			rule,
		)
		require.NoError(t, err)
		require.NotNil(t, registry)

		syms := boolexpr.NewCachedMap(map[string]any{})
		rule.Run(ctx, event, syms, func(Report) {})

		wrappedDest.AssertExpectations(t)
		innerDest.AssertExpectations(t)
		action.AssertExpectations(t)
		middleware.AssertExpectations(t)
	})
}
