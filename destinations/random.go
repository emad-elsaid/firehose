package destinations

import (
	"context"
	cryptorand "crypto/rand"
	"math/big"

	fh "github.com/emad-elsaid/firehose"
)

// Random forwards events to a random destination.
type Random[T any] struct {
	Destinations []fh.Destination[T] `validate:"required,min=1,dive,required"`
}

// Send forwards the event to a random destination.
func (r *Random[T]) Send(ctx context.Context, event T) error {
	if len(r.Destinations) == 0 {
		return fh.DestinationError{Err: ErrNoDestinationsConfigured}
	}

	index, err := r.nextIndex(len(r.Destinations))
	if err != nil {
		return fh.DestinationError{Err: err}
	}

	return r.Destinations[index].Send(ctx, event)
}

func (r *Random[T]) nextIndex(size int) (int, error) {
	upperBound := big.NewInt(int64(size))

	index, err := cryptorand.Int(cryptorand.Reader, upperBound)
	if err != nil {
		return 0, err
	}

	return int(index.Int64()), nil
}
