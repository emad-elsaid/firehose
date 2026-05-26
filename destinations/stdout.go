package destinations

import "fmt"

type Stdout[T any] struct{}

func (s Stdout[T]) Send(event T) error {
	_, err := fmt.Println(event)

	return err
}
