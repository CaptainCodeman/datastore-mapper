package mapper

import (
	"math"
	"strings"
	"time"

	"math/big"
)

// various value range splitting functions

var (
	lexDistance []*big.Int

	stringCharacters string
	maxStringLength  int
	minString        string
	maxString        string
)

// this all enables the ordinal string set to be reduced for testing purposes
func init() {
	defaultConstants()
}

func defaultConstants() {
	setupConstants("-.0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~", 32)
}

func setupConstants(alphabet string, maxLength int) {
	stringCharacters = alphabet
	maxStringLength = maxLength
	minString = ""
	maxString = strings.Repeat(alphabet[len(alphabet)-1:], maxLength)

	lexDistance = make([]*big.Int, maxLength)
	lexDistance[0] = big.NewInt(1)
	length := big.NewInt(int64(len(alphabet)))

	for i := 1; i < maxLength; i++ {
		temp := new(big.Int)
		temp.Mul(lexDistance[i-1], length)
		temp.Add(temp, big.NewInt(1))
		lexDistance[i] = temp
	}
}

// NOTE: datastore only stores values as int64 and float64 internally
// so we don't need to handle every variation of int, int32 etc...
// instead we'll upconvert to the biggest type and split on that
// Date's are converted to int64 and back to dates

func splitRangeInt64(start, end int64, splits int) []int64 {
	results := []int64{start}
	stride := (float64(end) - float64(start)) / float64(splits)
	for i := 1; i <= splits; i++ {
		result := roundInt64(float64(i)*stride + float64(start))
		if result != results[len(results)-1] {
			results = append(results, result)
		}
	}
	return results
}

func roundInt64(f float64) int64 {
	return int64(math.Floor(f + .5))
}

func splitRangeUint64(start, end uint64, splits int) []uint64 {
	results := []uint64{start}
	stride := (float64(end) - float64(start)) / float64(splits)
	for i := 1; i <= splits; i++ {
		result := roundUint64(float64(i)*stride + float64(start))
		if result != results[len(results)-1] {
			results = append(results, result)
		}
	}
	return results
}

func roundUint64(f float64) uint64 {
	return uint64(math.Floor(f + .5))
}

func splitRangeFloat64(start, end float64, splits int) []float64 {
	results := []float64{start}
	stride := (end - start) / float64(splits)
	for i := 1; i <= splits; i++ {
		result := roundFloat64(float64(i)*stride + start)
		if result != results[len(results)-1] {
			results = append(results, result)
		}
	}
	return results
}

func roundFloat64(f float64) float64 {
	return f
}

func splitRangeTime(start, end time.Time, splits int) []time.Time {
	values := splitRangeInt64(start.UnixNano(), end.UnixNano(), splits)
	results := make([]time.Time, len(values))
	for i, v := range values {
		results[i] = time.Unix(0, v).UTC()
	}
	return results
}

func splitRangeString(start, end string, splits int) []string {
	results := []string{start}
	if start == end {
		return results
	}
	if end < start {
		tmp := start
		start = end
		end = tmp
	}

	// find longest common prefix between strings
	minLen := len(start)
	if len(end) < minLen {
		minLen = len(end)
	}
	prefix := ""
	for i := 0; i < minLen; i++ {
		if start[i] == end[i] {
			prefix = start[0 : i+1]
		} else {
			break
		}
	}

	// remove prefix from strings to split
	start = start[len(prefix):]
	end = end[len(prefix):]

	ordStart := stringToOrd(start)
	ordEnd := stringToOrd(end)

	tmp := new(big.Int)
	tmp.Sub(ordEnd, ordStart)

	stride := new(big.Float)
	stride.SetInt(tmp)
	stride.Quo(stride, big.NewFloat(float64(splits)))

	for i := 1; i <= splits; i++ {
		tmp := new(big.Float)
		tmp.Mul(stride, big.NewFloat(float64(i)))
		tmp.Add(tmp, new(big.Float).SetInt(ordStart))

		result, _ := tmp.Int(new(big.Int))

		value := prefix + ordToString(result, 0)

		if value != results[len(results)-1] {
			results = append(results, value)
		}
	}

	return results
}

// Convert a string ordinal to a string
func ordToString(n *big.Int, maxLength int) string {
	if n.Int64() == 0 {
		return ""
	}

	if maxLength == 0 {
		maxLength = maxStringLength
	}
	maxLength--
	length := lexDistance[maxLength]
	tmp := new(big.Int)
	tmp.Sub(n, big.NewInt(1))
	index := new(big.Int)
	index.Div(tmp, length)
	mod := new(big.Int)
	mod.Mod(tmp, length)

	return stringCharacters[index.Int64():index.Int64()+1] + ordToString(mod, maxLength)
}

// Converts a string into an int representing its lexographic order
func stringToOrd(value string) *big.Int {
	n := new(big.Int)
	for i, c := range value {
		pos := strings.IndexRune(stringCharacters, c)
		tmp := new(big.Int)
		ld := lexDistance[maxStringLength-i-1]
		tmp.Mul(ld, big.NewInt(int64(pos)))
		n.Add(n, tmp)
		n.Add(n, big.NewInt(1))
	}
	return n
}
