package firehose

import (
	"fmt"
	"reflect"

	"github.com/emad-elsaid/boolexpr"
)

type symbols struct {
	v Event
}

// Get returns the value of the symbol for the given key, it supports accessing
// event fields and executing functions that return values, or functions that
// return (value, error)
func (s symbols) Get(symbol string) (any, error) {
	// find the field in the event struct and return its value
	// if the field is a function, execute it and return the result
	// if the function returns (v alue, error), return the value and error
	v, ok := s.findByField(symbol)
	if ok {
		return v, nil
	}

	return s.findByMethod(symbol)
}

func (s symbols) findByField(symbol string) (any, bool) {
	val := reflect.ValueOf(s.v)
	if val.Kind() == reflect.Pointer {
		val = val.Elem()
	}

	sVal := val.FieldByName(symbol)
	if !sVal.IsValid() {
		return nil, false
	}

	return sVal.Interface(), true
}

func (s symbols) findByMethod(symbol string) (any, error) {
	val := reflect.ValueOf(s.v)
	sVal := val.MethodByName(symbol)
	if !sVal.IsValid() {
		return nil, fmt.Errorf("%w: symbol: %s", boolexpr.ErrSymbolNotFound, symbol)
	}

	return sVal.Interface(), nil
}
