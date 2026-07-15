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
}

func TestAdd(t *testing.T) {
	t.Run("add first rule to nil registry", func(t *testing.T) {
		rule := &MockRule{
			ID:     "rule1",
			From:   NewMockSource[*EventMock](t),
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		}

		result, err := Add(t.Context(), nil, rule)

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
		registry, _ := Add(t.Context(), nil, rule1)

		rule2 := &MockRule{
			ID:     "rule2",
			From:   NewMockSource[*EventMock](t),
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		}

		result, err := Add(t.Context(), registry, rule2)

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
		registry, _ := Add(t.Context(), nil, rule1)

		rule2 := &MockRule{
			ID:     "rule2",
			From:   NewMockSource[*EventMock](t),
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		}
		registry, _ = Add(t.Context(), registry, rule2)

		rule3 := &MockRule{
			ID:     "rule3",
			From:   NewMockSource[*EventMock](t),
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		}

		result, err := Add(t.Context(), registry, rule3)

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

		result, err := Add(t.Context(), nil, rule)

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

		result, err := Add(t.Context(), nil, rule)

		require.NoError(t, err)
		require.NotNil(t, result)
	})
}

func TestAddSameSourceChaining(t *testing.T) {
	t.Parallel()

	t.Run("first rule with a source has no same-source links", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)
		rule := &MockRule{
			ID:     "rule1",
			From:   source,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		}

		result, err := Add(t.Context(), nil, rule)

		require.NoError(t, err)
		require.NotNil(t, result)

		ruleResult := result.(*Rule[*EventMock, *EventMock])
		require.Nil(t, ruleResult.nextSameSource, "first rule should have no next same source")
		require.Nil(t, ruleResult.prevSameSource, "first rule should have no prev same source")
	})

	t.Run("two rules with different sources have no same-source links", func(t *testing.T) {
		source1 := NewMockSource[*EventMock](t)
		source2 := NewMockSource[*EventMock](t)

		registry, _ := Add(t.Context(), nil, &MockRule{
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

		result, err := Add(t.Context(), registry, rule2)

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

		registry, _ := Add(t.Context(), nil, &MockRule{
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

		result, err := Add(t.Context(), registry, rule2)

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

		registry, _ := Add(t.Context(), nil, &MockRule{
			ID:     "rule1",
			From:   source,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		})

		registry, _ = Add(t.Context(), registry, &MockRule{
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

		result, err := Add(t.Context(), registry, rule3)

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
		registry, _ := Add(t.Context(), nil, &MockRule{
			ID:     "rule1",
			From:   sourceA,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		})

		// Add second rule with sourceA
		registry, _ = Add(t.Context(), registry, &MockRule{
			ID:     "rule3",
			From:   sourceA,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		})

		// Add rule with sourceB
		registry, _ = Add(t.Context(), registry, &MockRule{
			ID:     "rule3",
			From:   sourceB,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		})

		// Add third rule with sourceA
		result, err := Add(t.Context(), registry, &MockRule{
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

		registry, _ := Add(t.Context(), nil, &MockRule{
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

		done := make(chan struct{})
		source.On("Start", ctx, mock.Anything).Return(done, nil).Once()

		doneChannels := Start(ctx, registry, errorHandler)

		cancel()
		close(done)
		Wait(doneChannels)

		require.Len(t, receivedErrors, 0) // No errors expected with channels
		require.Len(t, doneChannels, 1)
	})

	t.Run("start multiple rules with different sources", func(t *testing.T) {
		source1 := NewMockSource[*EventMock](t)
		source2 := NewMockSource[*EventMock](t)

		registry, _ := Add(t.Context(), nil, &MockRule{
			ID:     "rule1",
			From:   source1,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		})
		registry, _ = Add(t.Context(), registry, &MockRule{
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

		done1 := make(chan struct{})
		done2 := make(chan struct{})
		source1.On("Start", ctx, mock.Anything).Return(done1, nil).Once()
		source2.On("Start", ctx, mock.Anything).Return(done2, nil).Once()

		doneChannels := Start(ctx, registry, errorHandler)

		cancel()
		close(done1)
		close(done2)
		Wait(doneChannels)

		require.Len(t, receivedErrors, 0) // No errors expected with channels
		require.Len(t, doneChannels, 2)
	})

	t.Run("start rule with source error", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)

		registry, _ := Add(t.Context(), nil, &MockRule{
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

		source.On("Start", ctx, mock.Anything).Return(nil, os.ErrClosed).Once()

		Start(ctx, registry, errorHandler)

		require.Len(t, receivedErrors, 1)
	})

	t.Run("start multiple rules with same source", func(t *testing.T) {
		source := NewMockSource[*EventMock](t)

		registry, _ := Add(t.Context(), nil, &MockRule{
			ID:     "rule1",
			From:   source,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		})
		registry, _ = Add(t.Context(), registry, &MockRule{
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

		done := make(chan struct{})
		source.On("Start", ctx, mock.Anything).Return(done, nil).Once()

		doneChannels := Start(ctx, registry, errorHandler)

	cancel()
	close(done)
	Wait(doneChannels)

	require.Len(t, receivedErrors, 0) // No errors expected with channels
})
}

func Test_addSingle_Errors(t *testing.T) {
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
			_, err := Add(
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


func TestRuleInit(t *testing.T) {
	tests := []struct {
		name        string
		setup       func() *MockRule
		expectError bool
		validate    func(t *testing.T, rule *MockRule)
	}{
		{
			name: "no middlewares initializes wrapped fields",
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
				require.NotNil(t, rule.wrappedCallback)
				require.NotNil(t, rule.Select)
				require.NotNil(t, rule.Into)
			},
		},
		{
			name: "wraps callback with single middleware",
			setup: func() *MockRule {
			middleware := &MockMiddleware[*EventMock, *EventMock]{}
			wrappedCb := func(ctx context.Context, event *EventMock, report ErrorHandler) {}
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
			wrappedCb1 := func(ctx context.Context, event *EventMock, report ErrorHandler) {}
			wrappedCb2 := func(ctx context.Context, event *EventMock, report ErrorHandler) {}

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

			err := rule.Init(t.Context())

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				tc.validate(t, rule)
			}
		})
	}
}

func TestRuleInit_action(t *testing.T) {
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
				require.NotNil(t, rule.Select)
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

			err := rule.Init(t.Context())

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				tc.validate(t, rule)
			}
		})
	}
}

func TestRuleInit_destination(t *testing.T) {
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
				require.NotNil(t, rule.Into)
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

			err := rule.Init(t.Context())

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
			Return(expectedOutput, nil).Once()

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
			Return(expectedOutput, nil).Once()

		dest := &MockDestination[*EventMock]{}
		dest.On("Send", mock.Anything, expectedOutput).
			Return(nil).Once()

		rule := &MockRule{
			ID:          "test-rule",
			From:        NewMockSource[*EventMock](t),
			Select:      innerAction,
			Into:        dest,
			Middlewares: []Middleware[*EventMock, *EventMock]{middleware},
		}

		ctx := context.Background()
		registry, err := Add(
			ctx,
			nil,
			rule,
		)
		require.NoError(t, err)
		require.NotNil(t, registry)

		syms := boolexpr.NewCachedMap(map[string]any{})
		_ = rule.Run(ctx, event, syms)

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
			Return(output, nil).Once()

		innerDest := &MockDestination[*EventMock]{}
		innerDest.On("Send", mock.Anything, output).
			Return(nil).Once()

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
			Return(nil).Once()

		rule := &MockRule{
			ID:          "test-rule",
			From:        NewMockSource[*EventMock](t),
			Select:      action,
			Into:        innerDest,
			Middlewares: []Middleware[*EventMock, *EventMock]{middleware},
		}

		ctx := context.Background()
		registry, err := Add(
			ctx,
			nil,
			rule,
		)
		require.NoError(t, err)
		require.NotNil(t, registry)

		syms := boolexpr.NewCachedMap(map[string]any{})
		_ = rule.Run(ctx, event, syms)

		wrappedDest.AssertExpectations(t)
		innerDest.AssertExpectations(t)
		action.AssertExpectations(t)
		middleware.AssertExpectations(t)
	})
}

func TestIsValid(t *testing.T) {
	t.Run("empty rule is invalid", func(t *testing.T) {
		rule := &MockRule{}

		require.Error(t, IsValid(rule))
	})

	t.Run("rule missing source is invalid", func(t *testing.T) {
		rule := &MockRule{
			From:   nil,
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   &MockDestination[*EventMock]{},
		}
		require.Error(t, IsValid(rule))
	})

	t.Run("rule missing action is invalid", func(t *testing.T) {
		rule := &MockRule{
			From:   NewMockSource[*EventMock](t),
			Where:  testCond[*EventMock]("Attr1 == 'value'"),
			Select: nil,
			Into:   &MockDestination[*EventMock]{},
		}
		require.Error(t, IsValid(rule))
	})

	t.Run("rule missing destination is invalid", func(t *testing.T) {
		rule := &MockRule{
			From:   NewMockSource[*EventMock](t),
			Select: &MockAction[*EventMock, *EventMock]{},
			Into:   nil,
		}
		require.Error(t, IsValid(rule))
	})
}
