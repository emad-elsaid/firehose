package firehose

import (
	"context"
)

// AddRule registers a new processing rule in the context.
func AddRule[In, Out Event](ctx context.Context, registry Registry, rule *Rule[In, Out], in In) (Registry, error) {
	err := IsValid(ctx, rule)
	if err != nil {
		return nil, err
	}

	middlewares := []Middleware[In, Out]{
		&PanicRecoveryMiddleware[In, Out]{},
		&ConditionalMiddleware[In, Out]{},
	}

	for i := len(middlewares) - 1; i >= 0; i-- {
		var err error
		rule.Then, err = middlewares[i].Wrap(ctx, *rule, rule.Then, in)
		if err != nil {
			return nil, err
		}
	}

	head := registry

	if head == nil {
		rule.setNext(rule)
		rule.setPrev(rule)

		return rule, nil
	}

	tail := head.getPrev()
	sameSourceTail := getSameSourceTail(head, rule.When)

	linkRule(rule, head, tail)
	linkSameSourceRule(rule, sameSourceTail)

	return head, nil
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
