package mapper

type (
	// Counters provides a simple map of name / values for mappers
	Counters map[string]int64
)

const (
	counterMapperCalls = "mapper_calls"
	counterMapperTime  = "mapper_time"
)

func NewCounters() Counters {
	return make(map[string]int64)
}

// Increment increments the named counter by a delta
func (c Counters) Increment(name string, delta int64) {
	c[name] += delta
}

// Add other Counter values to this one
func (c Counters) Add(o Counters) {
	for name, value := range o {
		c.Increment(name, value)
	}
}
