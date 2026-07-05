// Package firehose provides an event processing pipeline framework.
package firehose

import (
	"context"
	"errors"
	"fmt"

	"github.com/emad-elsaid/boolexpr"
)

// Rule defines an event processing pipeline from source to destination.
// I and O represent the input and output event types.
type Rule[I, O any] struct {
	// ID is a unique identifier for the rule, used for reporting and debugging purposes.
	ID string
	// Environments is a list of environment names where the rule is active. If
	// empty, the rule is active in all environments.
	Environments []string
	// On is the source that produces events to be processed by this rule.
	On Source[I] `validate:"required_without=SubRules"`
	// If is a condition that must evaluate to true for the rule to process the event.
	// Use ifs.Cond for string expressions, ifs.RateLimit for rate limiting,
	// ifs.Once for deduplication, or ifs.Ifs for combining multiple conditions.
	If If[I]
	// Then is the action to process the event if the On source produces an event
	Then Action[I, O] `validate:"required_without=SubRules"`
	// IfOutput is a condition that must evaluate to true for the rule to send
	// the output of the Then action to the To destination.
	IfOutput If[O]
	// To is the destination to send the output of the Then action
	To Destination[O] `validate:"required_without=SubRules"`
	// SubRules are the child rules that will inherit the parent fields if set
	SubRules []Rule[I, O]
	// Middlewares are the middlewares that will be applied to the action and
	// destination and callback of the rule. The first middleware wraps the
	// second middleware, and so on. The last middleware wraps the
	// actions/destination/callback of the rule.
	Middlewares []Middleware[I, O]

	next, prev                     Registry
	nextSameSource, prevSameSource sourceRegistry

	ctx                 context.Context
	wrappedCallback     Callback[I]
	actionWrappers      Action[I, O]
	destinationWrappers Destination[O]
}

// Process implements the Action interface. it allows using the rule as an action during the wrapping
// of the action. so that when the action field changes it calls the new action.
// When called it calls the current action without any middlewares.
func (r *Rule[I, O]) Process(ctx context.Context, event I, syms boolexpr.Symbols) (O, Report) {
	return r.Then.Process(ctx, event, syms)
}

// Send implements the Destination interface. it allows using the rule as a
// destination during the wrapping of the destination. so that when the
// destination field changes it calls the new destination.
func (r *Rule[I, O]) Send(ctx context.Context, event O) Report {
	return r.To.Send(ctx, event)
}

func (r *Rule[I, O]) start(ctx context.Context) error {
	isFirstSameSource := r.prevSameSource == nil
	if !isFirstSameSource {
		return nil
	}

	// use default callback function if not wrapped by any middleware,
	// otherwise use the wrapped callback.
	cb := r.callback
	if r.wrappedCallback != nil {
		cb = r.wrappedCallback
	}

	srcCtx, err := r.On.Start(ctx, cb)
	if err != nil {
		return fmt.Errorf("failed to start source: %w", err)
	}

	r.ctx = srcCtx

	return nil
}

func (r *Rule[I, O]) callback(ctx context.Context, event I, reportFn ReportFunc) {
	syms := EventSymbols(event)

	for current := Runnable[I](r); current != nil; current = current.NextRunnable() {
		current.Run(ctx, event, syms, reportFn)
	}
}

// Run executes the rule's action and destination for the given event.
func (r *Rule[I, O]) Run(ctx context.Context, event I, syms boolexpr.Symbols, reportFn ReportFunc) {
	conditionPassed, conditionReport := r.evaluateCondition(ctx, event, syms)
	if !conditionPassed {
		reportIfNeeded(reportFn, conditionReport)

		return
	}

	output, actionReport := r.processAction(ctx, event, syms)
	if actionReport.Err != nil {
		reportIfNeeded(reportFn, actionReport)

		return
	}

	outputSyms := EventSymbols(output)

	postConditionPassed, postConditionReport := r.evaluateOutputCondition(ctx, output, outputSyms)
	if !postConditionPassed {
		reportIfNeeded(reportFn, postConditionReport)

		return
	}

	destinationReport := r.processDestination(ctx, output)
	reportIfNeeded(reportFn, destinationReport)
}

func (r *Rule[I, O]) evaluateCondition(
	ctx context.Context,
	event I,
	syms boolexpr.Symbols,
) (bool, Report) {
	if r.If == nil {
		return true, Report{Rule: "", Err: nil}
	}

	pass, err := r.If.Evaluate(ctx, event, syms)
	if err != nil {
		return false, NewRuleReport(r.ID, ConditionError{Err: err})
	}

	if !pass {
		return false, NewRuleReport(r.ID, ErrNoMatch)
	}

	return true, Report{Rule: "", Err: nil}
}

func (r *Rule[I, O]) evaluateOutputCondition(
	ctx context.Context,
	event O,
	syms boolexpr.Symbols,
) (bool, Report) {
	if r.IfOutput == nil {
		return true, Report{Rule: "", Err: nil}
	}

	pass, err := r.IfOutput.Evaluate(ctx, event, syms)
	if err != nil {
		return false, NewRuleReport(r.ID, ConditionError{Err: err})
	}

	if !pass {
		return false, NewRuleReport(r.ID, ErrNoMatch)
	}

	return true, Report{Rule: "", Err: nil}
}

func (r *Rule[I, O]) processAction(ctx context.Context, event I, syms boolexpr.Symbols) (O, Report) {
	action := r.resolveAction()
	output, report := action.Process(ctx, event, syms)
	report.Rule = r.ID

	if report.Err != nil {
		report.Err = asActionError(report.Err)
	}

	return output, report
}

func (r *Rule[I, O]) processDestination(ctx context.Context, output O) Report {
	destination := r.resolveDestination()
	report := destination.Send(ctx, output)
	report.Rule = r.ID

	if report.Err != nil {
		report.Err = asDestinationError(report.Err)
	}

	return report
}

func (r *Rule[I, O]) resolveAction() Action[I, O] {
	if r.actionWrappers != nil {
		return r.actionWrappers
	}

	return r.Then
}

func (r *Rule[I, O]) resolveDestination() Destination[O] {
	if r.destinationWrappers != nil {
		return r.destinationWrappers
	}

	return r.To
}

func asActionError(err error) error {
	var actionErr ActionError
	if errors.As(err, &actionErr) {
		return err
	}

	return ActionError{Err: err}
}

func asDestinationError(err error) error {
	var destinationErr DestinationError
	if errors.As(err, &destinationErr) {
		return err
	}

	return DestinationError{Err: err}
}

func reportIfNeeded(reportFn ReportFunc, report Report) {
	if reportFn == nil {
		return
	}

	reportFn(report)
}

// NextRunnable returns the next runnable rule with the same source.
func (r *Rule[I, O]) NextRunnable() Runnable[I] {
	if r.nextSameSource == nil {
		return nil
	}

	// We will panic on purpose in case the next source is not a Runnable of the same type
	// As this would indicate a bug in the engine.
	//nolint:forcetypeassert // Intentional panic on type mismatch
	return r.nextSameSource.getRegistry().(Runnable[I])
}

func (r *Rule[I, O]) getNext() Registry                  { return r.next }
func (r *Rule[I, O]) setNext(n Registry)                 { r.next = n }
func (r *Rule[I, O]) getPrev() Registry                  { return r.prev }
func (r *Rule[I, O]) setPrev(p Registry)                 { r.prev = p }
func (r *Rule[I, O]) setNextSameSource(n sourceRegistry) { r.nextSameSource = n }
func (r *Rule[I, O]) getNextSameSource() sourceRegistry  { return r.nextSameSource }
func (r *Rule[I, O]) setPrevSameSource(p sourceRegistry) { r.prevSameSource = p }
func (r *Rule[I, O]) getSourceRegistry() sourceRegistry  { return r }
func (r *Rule[I, O]) getRegistry() Registry              { return r }
func (r *Rule[I, O]) getCtx() context.Context            { return r.ctx }
func (r *Rule[I, O]) getSource() any                     { return r.On }

// combineIf combines two If conditions into a single If.
// If both are nil, returns nil.
// If one is nil, returns the other.
// If both are non-nil, returns a slice-based If that evaluates both in sequence.
func (r *Rule[I, O]) combineIf(parent, child If[I]) If[I] {
	if parent == nil {
		return child
	}

	if child == nil {
		return parent
	}

	parentConditions := flattenIf(parent)
	childConditions := flattenIf(child)
	conditions := make([]If[I], 0, len(parentConditions)+len(childConditions))
	conditions = append(conditions, parentConditions...)
	conditions = append(conditions, childConditions...)

	return ifSlice[I](conditions)
}

// flattenIf extracts individual If conditions from an If value.
// If the value is ifSlice, it returns all elements.
// Otherwise, it returns a slice containing just the single condition.
func flattenIf[I any](ifVal If[I]) []If[I] {
	if ifVal == nil {
		return nil
	}

	// Check if it's our internal ifSlice type.
	if v, ok := ifVal.(ifSlice[I]); ok {
		return []If[I](v)
	}

	// Everything else is a single condition.
	return []If[I]{ifVal}
}

// ifSlice is a slice of If conditions that implements If[I].
type ifSlice[I any] []If[I]

func (ifs ifSlice[I]) Evaluate(ctx context.Context, event I, syms boolexpr.Symbols) (bool, error) {
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
