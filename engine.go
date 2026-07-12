package firehose

import (
	"context"
	"errors"
	"os"
	"reflect"
	"slices"
	"strconv"

	"github.com/emad-elsaid/boolexpr"
	"github.com/go-playground/validator/v10"
)

// ErrRuleNotActivatable is returned when a rule cannot be activated because
// it is missing required properties (ID, Select, From, Into).
var ErrRuleNotActivatable = errors.New("rule is not activatable, missing required properties")

// Add registers a new processing rule in the context.
func Add[I, O any](
	ctx context.Context,
	registry Registry,
	rule *Rule[I, O],
) (Registry, error) {
	flatten(rule)

	return addSingle(
		ctx,
		registry,
		rule,
	)
}

// addSingle registers a single rule and its subrules in the registry.
func addSingle[I, O any](
	ctx context.Context,
	registry Registry,
	rule *Rule[I, O],
) (Registry, error) {
	err := validateAndCheckActivatable(rule)
	if err != nil {
		return nil, err
	}

	if shouldRegisterRule(rule) {
		updatedRegistry, registerErr := registerActivatableRule(ctx, registry, rule)
		if registerErr != nil {
			return nil, registerErr
		}

		registry = updatedRegistry
	}

	return registerSubRules(ctx, registry, rule)
}

func shouldRegisterRule[I, O any](rule *Rule[I, O]) bool {
	if !isActivatable(rule) {
		return false
	}

	return isEnvironmentEnabled(rule.Environments, os.Getenv("ENV"))
}

func isEnvironmentEnabled(environments []string, currentEnvironment string) bool {
	if len(environments) == 0 {
		return true
	}

	return slices.Contains(environments, currentEnvironment)
}

func validateAndCheckActivatable[I, O any](rule *Rule[I, O]) error {
	err := IsValid(rule)
	if err != nil {
		return err
	}

	if !isActivatable(rule) && len(rule.SubRules) == 0 {
		return ErrRuleNotActivatable
	}

	return nil
}

func registerActivatableRule[I, O any](
	ctx context.Context,
	registry Registry,
	rule *Rule[I, O],
) (Registry, error) {
	err := wrapMiddlewares(ctx, rule)
	if err != nil {
		return nil, err
	}

	return addToRegistry(registry, rule), nil
}

func registerSubRules[I, O any](
	ctx context.Context,
	registry Registry,
	rule *Rule[I, O],
) (Registry, error) {
	for i := range rule.SubRules {
		subrule := &rule.SubRules[i]

		var err error

		registry, err = addSingle(
			ctx,
			registry,
			subrule,
		)
		if err != nil {
			return nil, err
		}
	}

	return registry, nil
}

func wrapMiddlewares[I, O any](
	ctx context.Context,
	rule *Rule[I, O],
) error {
	middlewares := rule.Middlewares
	if len(middlewares) == 0 {
		return nil
	}

	rule.wrappedCallback = rule.callback
	rule.wrappedAction = rule
	rule.wrappedDestination = rule

	for _, middleware := range slices.Backward(middlewares) {
		err := wrapWithMiddleware(ctx, rule, middleware)
		if err != nil {
			return err
		}
	}

	return nil
}

func wrapWithMiddleware[I, O any](
	ctx context.Context,
	rule *Rule[I, O],
	middleware Middleware[I, O],
) error {
	wrappedCallback, err := middleware.WrapCallback(ctx, rule, rule.wrappedCallback)
	if err != nil {
		return err
	}

	rule.wrappedCallback = wrappedCallback

	wrappedAction, err := middleware.WrapAction(ctx, rule, rule.wrappedAction)
	if err != nil {
		return err
	}

	rule.wrappedAction = wrappedAction

	wrappedDestination, err := middleware.WrapDestination(ctx, rule, rule.wrappedDestination)
	if err != nil {
		return err
	}

	rule.wrappedDestination = wrappedDestination

	return nil
}

func addToRegistry[I, O any](registry Registry, rule *Rule[I, O]) Registry {
	if registry == nil {
		rule.setNext(rule)
		rule.setPrev(rule)

		return rule
	}

	tail := registry.getPrev()
	sameSourceTail := getSameSourceTail(registry, rule.From)

	linkRule(rule, registry, tail)
	linkSameSourceRule(rule, sameSourceTail)

	return registry
}

func linkRule(rule Registry, head Registry, tail Registry) {
	rule.setNext(head)
	head.setPrev(rule)

	if tail != nil {
		rule.setPrev(tail)
		tail.setNext(rule)
	}
}

func linkSameSourceRule(rule sourceRegistry, sameSourceTail sourceRegistry) {
	if sameSourceTail == nil {
		return
	}

	rule.setPrevSameSource(sameSourceTail)
	sameSourceTail.setNextSameSource(rule)
}

func getSameSourceTail(registry Registry, source any) sourceRegistry {
	tail := registry.getPrev()
	for current := tail; current != nil; {
		currentSource := current.getSource()
		if currentSource == source {
			return current.getSourceRegistry()
		}

		current = current.getPrev()
		if current == tail {
			break
		}
	}

	return nil
}

// Start activates all registered rules.
func Start(ctx context.Context, registry Registry, errFunc ErrorHandler) {
	for current := registry; current != nil; {
		err := current.start(ctx)
		reportError(errFunc, err)

		current = current.getNext()
		if current == registry {
			break
		}
	}
}

// Wait blocks until all rules have completed processing, and sends any errors
// that occurred during processing to the provided error channel.
func Wait(registry Registry, errFunc ErrorHandler) {
	for current := registry; current != nil; {
		waitForRule(current, errFunc)

		current = current.getNext()
		if current == registry {
			break
		}
	}
}

func waitForRule(rule Registry, errFunc ErrorHandler) {
	ctx := rule.getCtx()
	if ctx == nil {
		return
	}

	<-ctx.Done()

	reportError(errFunc, ctx.Err())
}

func reportError(errFunc ErrorHandler, err error) {
	if err == nil || errFunc == nil {
		return
	}

	errFunc(err)
}

// flatten recursively inherit the properties of the parent rule to its subrules.
func flatten[I, O any](rule *Rule[I, O]) {
	if rule == nil {
		return
	}

	if len(rule.SubRules) == 0 {
		return
	}

	for i := range rule.SubRules {
		subrule := &rule.SubRules[i]
		inherit(i+1, rule, subrule)
		flatten(subrule)
	}
}

func inherit[I, O any](index int, parent *Rule[I, O], child *Rule[I, O]) {
	combine(index, parent, child)

	childType := reflect.TypeFor[*Rule[I, O]]().Elem()
	childValue := reflect.ValueOf(child).Elem()
	parentValue := reflect.ValueOf(parent).Elem()

	// go over child fields and if they are not set, inherit from parent
	for _, structField := range reflect.VisibleFields(childType) {
		if !structField.IsExported() {
			continue
		}

		if structField.Name == "SubRules" {
			continue
		}

		field := childValue.FieldByName(structField.Name)
		if field.IsZero() {
			field.Set(parentValue.FieldByName(structField.Name))
		}
	}
}

func combine[I, O any](index int, parent *Rule[I, O], child *Rule[I, O]) {
	// Combine parent and child Conditions
	child.Where = combineConditions(parent.Where, child.Where)

	// Combine parent and child Having conditions
	child.Having = combineConditions(parent.Having, child.Having)

	if len(parent.Middlewares) > 0 {
		child.Middlewares = append(parent.Middlewares, child.Middlewares...)
	}

	if child.ID == "" {
		child.ID = strconv.Itoa(index)
	}

	if parent.ID != "" {
		child.ID = parent.ID + "/" + child.ID
	}
}

func isActivatable[I, O any](rule *Rule[I, O]) bool {
	return rule.ID != "" &&
		rule.From != nil &&
		rule.Select != nil &&
		rule.Into != nil
}

// IsValid validates the rule's fields.
func IsValid[I, O any](rule *Rule[I, O]) error {
	validatorInstance := validator.New(validator.WithRequiredStructEnabled())

	return validatorInstance.Struct(rule)
}

// combineConditions is a generic helper that combines two conditions into a single Condition.
// If both are nil, returns nil.
// If one is nil, returns the other.
// If both are non-nil, returns a slice-based Condition that evaluates both in sequence.
func combineConditions[T any](parent, child Condition[T]) Condition[T] {
	if parent == nil {
		return child
	}

	if child == nil {
		return parent
	}

	parentConditions := flattenCondition(parent)
	childConditions := flattenCondition(child)
	conditions := make([]Condition[T], 0, len(parentConditions)+len(childConditions))
	conditions = append(conditions, parentConditions...)
	conditions = append(conditions, childConditions...)

	return conditionSlice[T](conditions)
}

// flattenCondition extracts individual conditions from a Condition value.
// If the value is conditionSlice, it returns all elements.
// Otherwise, it returns a slice containing just the single condition.
func flattenCondition[I any](conditionVal Condition[I]) []Condition[I] {
	if conditionVal == nil {
		return nil
	}

	if v, ok := conditionVal.(conditionSlice[I]); ok {
		return []Condition[I](v)
	}

	return []Condition[I]{conditionVal}
}

// conditionSlice is a slice of conditions that implements Condition[I].
type conditionSlice[I any] []Condition[I]

func (conditions conditionSlice[I]) Evaluate(ctx context.Context, event I, syms boolexpr.Symbols) (bool, error) {
	for _, cond := range conditions {
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
