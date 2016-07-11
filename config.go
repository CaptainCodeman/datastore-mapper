package mapper

import (
	"time"
)

type (
	// Config stores mapper configuration settings and defaults
	Config struct {
		// DatastorePrefix is added to the beginning of every mapreduce collection name
		DatastorePrefix string

		// TaskPrefix is added to the beginning of every mapreduce task name
		TaskPrefix string

		// BasePath is the root to the mapreduce web handlers
		BasePath string

		// Queue is the default queue to use for mapreduce tasks if not
		Queue string

		// ShardCount is the default number of shards to use
		ShardCount int

		// OversamplingFactor helps achieve more event shard distribution with 'clumpy' data
		// clumpy is definitely a technical term
		OversamplingFactor int

		// LeaseDuration is how long a worker will hold a lock for
		LeaseDuration time.Duration

		// MaxLeaseDuration is the time considered to be a timeout
		MaxLeaseDuration time.Duration

		// TaskMaxAttempts is the maximum number of times to retry a failing task
		TaskMaxAttempts int

		// LogVerbose controls verbose logging output
		LogVerbose bool
	}
)

var (
	config = defaultConfig()
)

func defaultConfig() *Config {
	return &Config{
		DatastorePrefix:    "MP_",
		TaskPrefix:         "MP-",
		BasePath:           "/mapper",
		Queue:              "default",
		ShardCount:         8,
		OversamplingFactor: 32,
		LeaseDuration:      time.Duration(30) * time.Second,
		MaxLeaseDuration:   time.Duration(10)*time.Minute + time.Duration(30)*time.Second,
		TaskMaxAttempts:    31,
		LogVerbose:         true,
	}
}
