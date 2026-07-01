package firehose

import (
	"context"
	"errors"
	"os"
	"reflect"
	"slices"
	"strconv"

	"github.com/go-playground/validator/v10"
)

// ErrRuleNotActivatable is returned when a rule cannot be activated because
// it is missing required properties (Id, When, Then, To).
var ErrRuleNotActivatable = errors.New("rule is not activatable, missing required properties")

// AddRule registers a new processing rule in the context.
func AddRule[I, O any](
	ctx context.Context,
	registry Registry,
	rule *Rule[I, O],
) (Registry, error) {
	flatten(rule)

	return addSingleRule(
		ctx,
		registry,
		rule,
	)
}

// addSingleRule registers a single rule and its subrules in the registry.
func addSingleRule[I, O any](
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

	return addRuleToRegistry(registry, rule), nil
}

func registerSubRules[I, O any](
	ctx context.Context,
	registry Registry,
	rule *Rule[I, O],
) (Registry, error) {
	for i := range rule.SubRules {
		subrule := &rule.SubRules[i]

		var err error

		registry, err = addSingleRule(
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
	rule.actionWrappers = rule
	rule.destinationWrappers = rule

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

	wrappedAction, err := middleware.WrapAction(ctx, rule, rule.actionWrappers)
	if err != nil {
		return err
	}

	rule.actionWrappers = wrappedAction

	wrappedDestination, err := middleware.WrapDestination(ctx, rule, rule.destinationWrappers)
	if err != nil {
		return err
	}

	rule.destinationWrappers = wrappedDestination

	return nil
}

func addRuleToRegistry[I, O any](registry Registry, rule *Rule[I, O]) Registry {
	if registry == nil {
		rule.setNext(rule)
		rule.setPrev(rule)

		return rule
	}

	tail := registry.getPrev()
	sameSourceTail := getSameSourceTail(registry, rule.On)

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
	// Combine parent and child If conditions
	child.If = child.combineIf(parent.If, child.If)

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
		rule.On != nil &&
		rule.Then != nil &&
		rule.To != nil
}

// IsValid validates the rule's fields.
func IsValid[I, O any](rule *Rule[I, O]) error {
	validatorInstance := validator.New(validator.WithRequiredStructEnabled())

	return validatorInstance.Struct(rule)
}
