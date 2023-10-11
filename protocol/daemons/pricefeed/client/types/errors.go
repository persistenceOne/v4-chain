package types

import (
	"errors"
)

var (
	ErrEmptyMarketPriceUpdate = errors.New("Market price update has length of 0")
	ErrUnableToUpdatePrices   = errors.New("Unable to update prices")
)
