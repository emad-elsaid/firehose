// Package runner provides task runner implementations for executing functions concurrently.
package runner

// Basic is a simple task runner that executes functions in separate goroutines.
type Basic struct{}

// Run executes the given function in a new goroutine.
func (Basic) Run(f func()) {
	go f()
}
