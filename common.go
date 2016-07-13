package mapper

import (
	"strings"
	"time"

	"google.golang.org/appengine/datastore"
)

type (
	taskable interface {
		getID() string
		setID(id string)

		getQueue() string
		setQueue(queue string)
	}

	// common contains properties that are common across all
	// mapper entities (job, iterator, namespace and shard)
	common struct {
		// Counters holds the task counters map
		Counters Counters `datastore:"-"`

		// Active indicates if this task is still active
		Active bool `datastore:"active,noindex"`

		// Count is the number of records processed
		Count int64 `datastore:"count,noindex"`

		// Started is when the task began
		Started time.Time `datastore:"started"`

		// Updated is when the task was last updated
		Updated time.Time `datastore:"updated"`

		// ProcessTime is the time that the task spent executing
		ProcessTime time.Duration `datastore:"process_time,noindex"`

		// WallTime is the wall-time that the task takes
		WallTime time.Duration `datastore:"wall_time,noindex"`

		// private fields used by local instance
		id        string
		queue     string
		startTime time.Time
	}
)

func (c *common) getID() string {
	return c.id
}
func (c *common) setID(id string) {
	c.id = id
}

func (c *common) getQueue() string {
	return c.queue
}
func (c *common) setQueue(queue string) {
	c.queue = queue
}

func (c *common) start() {
	c.Active = true
	c.Counters = NewCounters()
	c.Count = 0
	c.Started = getTime()
	c.Updated = c.Started
	c.startTime = c.Started
}

func (c *common) complete() {
	c.Active = false
	c.Updated = getTime()
	c.WallTime = c.Updated.Sub(c.Started)
}

func (c *common) rollup(r common) {
	c.Count += r.Count
	c.ProcessTime += r.ProcessTime
	c.Counters.Add(r.Counters)
}

/* datastore */
func (c *common) Load(props []datastore.Property) error {
	datastore.LoadStruct(c, props)

	c.Counters = make(map[string]int64)
	for _, prop := range props {
		if strings.HasPrefix(prop.Name, "counters.") {
			key := prop.Name[9:len(prop.Name)]
			c.Counters[key] = prop.Value.(int64)
		}
	}

	c.startTime = getTime()

	return nil
}

func (c *common) Save() ([]datastore.Property, error) {
	c.ProcessTime += getTime().Sub(c.startTime)

	props, err := datastore.SaveStruct(c)
	if err != nil {
		return nil, err
	}

	for key, value := range c.Counters {
		props = append(props, datastore.Property{Name: "counters." + key, Value: value, NoIndex: true, Multiple: false})
	}

	return props, nil
}
