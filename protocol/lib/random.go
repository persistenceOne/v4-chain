package lib

import (
	"errors"
	"fmt"
	"math/rand"

	exprand "golang.org/x/exp/rand"
	"gonum.org/v1/gonum/stat/sampleuv"
)

// WeightedRandomSample returns a random sample of n indices without replacement.
// This function returns an error if:
//   - src is nil.
//   - failed to select an item from weights because all weights are zero, or n > len(weights).
//
// Note that weights of zero will never get selected.
func WeightedRandomSample(
	weights []float64,
	n uint32,
	src exprand.Source,
) (
	selected []int,
	err error,
) {
	if src == nil {
		return nil, errors.New("src expected to be non-nil")
	}

	weighted := sampleuv.NewWeighted(weights, src)

	selected = make([]int, n)
	for i := uint32(0); i < n; i++ {
		index, ok := weighted.Take()
		if !ok {
			return nil, errors.New("failed to take item from weighted")
		}
		selected[i] = index
	}

	return selected, nil
}

func RandomBool() bool {
	return rand.Intn(2) == 0
}

// RandomBytesBetween returns a random byte slice that is in the range [start, end] when compared lexicographically.
// Nil slices for start and end will be treated as empty byte slices. Will panic if:
//   - start compares lexicographically less than end
//   - nil rand is provided
func RandomBytesBetween(start []byte, end []byte, rand *rand.Rand) []byte {
	if rand == nil {
		panic(errors.New("rand expected to be non-nil."))
	}

	minLen := len(start)
	maxLen := len(end)
	if minLen > maxLen {
		minLen, maxLen = maxLen, minLen
	}

	bytes := make([]byte, maxLen)
	i := 0

	// Copy the common bytes between the two keys.
	for ; i < minLen; i++ {
		if start[i] == end[i] {
			bytes[i] = start[i]
		} else {
			if start[i] > end[i] {
				panic(fmt.Errorf("start %x compares lexicographically greater than end %x at position %d.", start, end, i))
			}
			break
		}
	}

	// If start == end then we are done and can return bytes.
	if i == maxLen {
		return bytes
	}

	// Remember the floor and ceiling starting at the first byte that differs between the two keys.
	// Note that if floor is -1, then len(start) <= len(bytes)
	isPrefixOfStart, isPrefixOfEnd := true, true
	floor := int32(0)
	if i < len(start) {
		floor = int32(start[i])
	}
	ceiling := int32(end[i])

	// Compute a random byte length that gives each byte string an equal probability.
	// Note that [0, 255] represents the possible values and 256 represents the "unset" byte.
	targetLength := maxLen
	for j := minLen; j < maxLen && rand.Int31n(257) == 256; j++ {
		targetLength--
	}

	// Generate the remainder of the random bytes producing a value that compares lexicographically
	// between start and end.
	for ; i < targetLength; i++ {
		current := floor + rand.Int31n(ceiling-floor+1)
		bytes[i] = byte(current)

		// Ensure that if bytes is a prefix of start that the next byte in start is the new floor.
		if isPrefixOfStart && current == floor && i+1 < len(start) {
			floor = int32(start[i+1])
		} else {
			floor = 0
			isPrefixOfStart = false
		}

		// Ensure that if bytes is a prefix of end that the next byte in end is the new ceiling.
		if isPrefixOfEnd && current == ceiling {
			// If bytes == end then we must return now as we can't generate any more bytes as
			// the result would be greater than end.
			if i+1 < len(end) {
				ceiling = int32(end[i+1])
			} else {
				return bytes[:i+1]
			}
		} else {
			ceiling = 255
			isPrefixOfEnd = false
		}
	}

	return bytes[:targetLength]
}
