package firehose

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strconv"
)

// ErrRuleNotActivatable is returned when a rule cannot be activated because
// it is missing required properties (Id, When, Then, To).
var ErrRuleNotActivatable = errors.New("rule is not activatable, missing required properties")

// AddRule registers a new processing rule in the context.
func AddRule[I, O Event](
	ctx context.Context,
	registry Registry,
	rule *Rule[I, O],
	inInstance I,
	outInstance O,
) (Registry, error) {
	flatten(rule)

	return addSingleRule(
		ctx,
		registry,
		rule,
		inInstance,
		outInstance,
	)
}

// addSingleRule registers a single rule and its subrules in the registry.
func addSingleRule[I, O Event](
	ctx context.Context,
	registry Registry,
	rule *Rule[I, O],
	inInstance I,
	outInstance O,
) (Registry, error) {
	err := validateAndCheckActivatable(rule)
	if err != nil {
		return nil, err
	}

	if isActivatable(rule) {
		var err error

		registry, err = registerActivatableRule(ctx, registry, rule, inInstance, outInstance)
		if err != nil {
			return nil, err
		}
	}

	return registerSubRules(ctx, registry, rule, inInstance, outInstance)
}

func validateAndCheckActivatable[I, O Event](rule *Rule[I, O]) error {
	err := IsValid(rule)
	if err != nil {
		return err
	}

	if !isActivatable(rule) && len(rule.SubRules) == 0 {
		return ErrRuleNotActivatable
	}

	return nil
}

func registerActivatableRule[I, O Event](
	ctx context.Context,
	registry Registry,
	rule *Rule[I, O],
	inInstance I,
	outInstance O,
) (Registry, error) {
	err := wrapMiddlewares(ctx, rule, inInstance, outInstance)
	if err != nil {
		return nil, err
	}

	return addRuleToRegistry(registry, rule), nil
}

func registerSubRules[I, O Event](
	ctx context.Context,
	registry Registry,
	rule *Rule[I, O],
	inInstance I,
	outInstance O,
) (Registry, error) {
	for i := range rule.SubRules {
		subrule := &rule.SubRules[i]

		var err error

		registry, err = addSingleRule(
			ctx,
			registry,
			subrule,
			inInstance,
			outInstance,
		)
		if err != nil {
			return nil, err
		}
	}

	return registry, nil
}

func wrapMiddlewares[I, O Event](
	ctx context.Context,
	rule *Rule[I, O],
	inInstance I,
	outInstance O,
) error {
	middlewares := rule.Middlewares
	if len(middlewares) == 0 {
		return nil
	}

	rule.wrappedCallback = rule.callback
	rule.actionWrappers = rule
	rule.destinationWrappers = rule

	for _, mw := range slices.Backward(middlewares) {
		var err error

		rule.wrappedCallback, err = mw.WrapCallback(ctx, rule, rule.wrappedCallback, inInstance)
		if err != nil {
			return err
		}

		rule.actionWrappers, err = mw.WrapAction(ctx, rule, rule.actionWrappers, inInstance)
		if err != nil {
			return err
		}

		rule.destinationWrappers, err = mw.WrapDestination(ctx, rule, rule.destinationWrappers, outInstance)
		if err != nil {
			return err
		}
	}

	return nil
}

func addRuleToRegistry[I, O Event](registry Registry, rule *Rule[I, O]) Registry {
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
func Start(ctx context.Context, registry Registry, errChan chan<- error) {
	for current := registry; current != nil; {
		err := current.start(ctx)
		if err != nil {
			errChan <- err
		}

		current = current.getNext()
		if current == registry {
			break
		}
	}
}

// Wait blocks until all rules have completed processing, and sends any errors
// that occurred during processing to the provided error channel.
func Wait(registry Registry, errChan chan<- error) {
	for current := registry; current != nil; {
		ctx := current.getCtx()

		if ctx != nil {
			<-ctx.Done()

			err := ctx.Err()
			if err != nil {
				errChan <- err
			}
		}

		current = current.getNext()
		if current == registry {
			break
		}
	}

	close(errChan)
}

// flatten recursively inherit the properties of the parent rule to its subrules.
func flatten[I, O Event](rule *Rule[I, O]) {
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

func inherit[I, O Event](index int, parent *Rule[I, O], child *Rule[I, O]) {
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

func combine[I, O Event](index int, parent *Rule[I, O], child *Rule[I, O]) {
	// Prepend parent conditions to child conditions
	if len(parent.If) > 0 {
		child.If = append(parent.If, child.If...)
	}

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

func isActivatable[I, O Event](rule *Rule[I, O]) bool {
	return rule.ID != "" &&
		rule.On != nil &&
		rule.Then != nil &&
		rule.To != nil
}
