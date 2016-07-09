package mapper

import (
	"time"
)

type (
	// Config stores configuration settings and defaults
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

		// MaxShardCount is the maximum number of shards to use
		MaxShardCount int

		// OverplitFactor helps achieve more event shard distribution with 'clumpy' data
		// clumpy is definitely a technical term
		OverplitFactor int

		// MaxNamespacesForKeyShard is the maximum number of namespaces
		// that will be sharded by datastore key before switching to a
		// strategy where sharding is done lexographically by namespace.
		MaxNamespacesForKeyShard int

		// LeaseDuration is how long a worker will hold a lock for
		LeaseDuration time.Duration

		// MaxLeaseDuration is the time considered to be a timeout
		MaxLeaseDuration time.Duration

		// Delay between consecutive controller callback invocations.
		ControllerPeriod time.Duration

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
		DatastorePrefix:          "MP_",
		TaskPrefix:               "MP-",
		BasePath:                 "/mapper",
		Queue:                    "default",
		ShardCount:               8,
		MaxShardCount:            256,
		OverplitFactor:           32,
		MaxNamespacesForKeyShard: 10,
		LeaseDuration:            time.Duration(30) * time.Second,
		MaxLeaseDuration:         time.Duration(10)*time.Minute + time.Duration(30)*time.Second,
		ControllerPeriod:         time.Duration(4) * time.Second,
		TaskMaxAttempts:          31,
		LogVerbose:               true,
	}
}
