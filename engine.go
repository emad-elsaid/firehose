package firehose

import (
	"context"
	"os"
	"slices"

	"github.com/go-playground/validator/v10"
)

// Add registers a new processing rule in the context.
func Add[I, O any](
	ctx context.Context,
	registry Registry,
	rule *Rule[I, O],
) (Registry, error) {
	err := IsValid(rule)
	if err != nil {
		return nil, err
	}

	if !shouldRegisterRule(rule) {
		return registry, nil
	}

	err = wrapMiddlewares(ctx, rule)
	if err != nil {
		return nil, err
	}

	return addToRegistry(registry, rule), nil
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

func wrapMiddlewares[I, O any](
	ctx context.Context,
	rule *Rule[I, O],
) error {
	rule.wrappedCallback = rule.callback
	rule.wrappedAction = rule.Select
	rule.wrappedDestination = rule.Into

	middlewares := rule.Middlewares
	if len(middlewares) == 0 {
		return nil
	}

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

func waitForRule(rule Registry, _ ErrorHandler) {
	done := rule.getDone()
	if done == nil {
		return
	}

	<-done
}

func reportError(errFunc ErrorHandler, err error) {
	if err == nil || errFunc == nil {
		return
	}

	errFunc(err)
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
