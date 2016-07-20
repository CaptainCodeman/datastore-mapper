package mapper

import (
	"time"
)

type (
	// Config stores mapper configuration settings
	Config struct {
		// Path is the mount point for the server
		Path string

		// DatastorePrefix is added to the beginning of every mapreduce collection name
		DatastorePrefix string

		// Queue is the default queue to use for mapreduce tasks if not
		Queue string

		// Shards is the default number of shards to use
		Shards int

		// Oversampling is a factor to increase the number of scatter samples
		// and helps achieve more even shard distribution with 'clumpy' data
		// (clumpy is definitely a technical term)
		Oversampling int

		// LeaseDuration is how long a worker will hold a lock for
		LeaseDuration time.Duration

		// LeaseTimeout is the time considered to be a timeout
		LeaseTimeout time.Duration

		// TaskTimeout is the time to execute a task for.
		// For frontend instances the limit is 10 minutes
		TaskTimeout time.Duration

		// CursorTimeout is the time to use a cursor for before requerying
		// The default limit is 60 seconds
		CursorTimeout time.Duration

		// Retries is the maximum number of times to retry a failing task
		Retries int

		// LogVerbose controls verbose logging output
		LogVerbose bool
	}
)

// newConfig creates a new config with default values
func newConfig() *Config {
	// TODO: use config as default, allow setting some values per-job
	// and prevent config changes affecting already-running tasks
	return &Config{
		Path:            DefaultPath,
		DatastorePrefix: "MP_",
		Queue:           "",
		Shards:          8,
		Oversampling:    32,
		LeaseDuration:   time.Duration(30) * time.Second,
		LeaseTimeout:    time.Duration(10)*time.Minute + time.Duration(30)*time.Second,
		TaskTimeout:     time.Duration(10)*time.Minute - time.Duration(30)*time.Second,
		CursorTimeout:   time.Duration(50) * time.Second,
		Retries:         31,
		LogVerbose:      false,
	}
}

// DatastorePrefix sets the prefix for mapper datastore collections
func DatastorePrefix(prefix string) func(*Config) error {
	return func(c *Config) error {
		c.DatastorePrefix = prefix
		return nil
	}
}

// Queue sets the default taskqueue to use when scheduling mapper tasks
func Queue(queue string) func(*Config) error {
	return func(c *Config) error {
		c.Queue = queue
		return nil
	}
}

// Shards sets the default target number of shards to use
func Shards(shards int) func(*Config) error {
	return func(c *Config) error {
		c.Shards = shards
		return nil
	}
}

// Oversampling sets the factor to use to even out sampling
func Oversampling(factor int) func(*Config) error {
	return func(c *Config) error {
		c.Oversampling = factor
		return nil
	}
}

// LeaseDuration sets how long a worker will hold a lock for
func LeaseDuration(duration time.Duration) func(*Config) error {
	return func(c *Config) error {
		c.LeaseDuration = duration
		return nil
	}
}

// LeaseTimeout sets how long before a lock will be considered timedout
func LeaseTimeout(duration time.Duration) func(*Config) error {
	return func(c *Config) error {
		c.LeaseTimeout = duration
		return nil
	}
}

// TaskTimeout sets how long a task is allowed to execute
func TaskTimeout(duration time.Duration) func(*Config) error {
	return func(c *Config) error {
		c.TaskTimeout = duration
		return nil
	}
}

// CursorTimeout sets how long a datastore cursor is allowed to run
func CursorTimeout(duration time.Duration) func(*Config) error {
	return func(c *Config) error {
		c.CursorTimeout = duration
		return nil
	}
}

// Retries sets how many times to attempt retrying a failing task
func Retries(retries int) func(*Config) error {
	return func(c *Config) error {
		c.Retries = retries
		return nil
	}
}

// LogVerbose sets verbos logging
func LogVerbose(c *Config) error {
	c.LogVerbose = true
	return nil
}
