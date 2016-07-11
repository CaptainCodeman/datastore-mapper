package mapper

import (
	"time"
)

const (
	// Ridiculous future UNIX epoch time, 500 years from now.
	futureTime = 1 << 34
)

var (
	// getTime is a function to return the current UTC time.
	// This makes it possible to set the time to use in tests
	getTime = getTimeDefault
)

func getTimeDefault() time.Time {
	return time.Now().UTC()
}
