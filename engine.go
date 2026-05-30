package firehose

import (
	"context"
)

// AddRule registers a new processing rule in the context.
func AddRule[In, Out Event](registry Registry, rule *Rule[In, Out]) (Registry, error) {
	err := rule.parseCondition()
	if err != nil {
		return nil, err
	}

	head := registry

	if head == nil {
		rule.setNext(rule)
		rule.setPrev(rule)

		return rule, nil
	}

	tail := head.getPrev()
	sameSourceTail := getSameSourceTail(head, rule.When)

	rule.setNext(head)
	head.setPrev(rule)

	if tail != nil {
		rule.setPrev(tail)
		tail.setNext(rule)
	}

	if sameSourceTail != nil {
		rule.setPrevSameSource(sameSourceTail)
		sameSourceTail.setNextSameSource(rule)
	}

	return head, nil
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

// Start activates all registered rules and waits for completion.
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

	waitForSourcesToFinish(registry, errChan)
}

func waitForSourcesToFinish(registry Registry, errChan chan<- error) {
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
