package mapper

import (
	"fmt"
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

// getDescendingKey returns a key name lexically ordered by time descending.
// This lets us have a key name for use with Datastore entities which returns
// rows in time descending order when it is scanned in lexically ascending order,
// allowing us to bypass index building for descending indexes.
func getDescendingKey() string {
	nowDescending := futureTime - getTime().Unix()
	return fmt.Sprintf("%012d", nowDescending)
}

// TODO: set force_writes option
