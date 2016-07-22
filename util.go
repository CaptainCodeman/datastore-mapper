package mapper

import (
	"time"
)

var (
	// getTime is a function to return the current UTC time.
	// This makes it possible to set the time to use in tests
	getTime = getTimeDefault
)

func getTimeDefault() time.Time {
	return time.Now().UTC()
}
