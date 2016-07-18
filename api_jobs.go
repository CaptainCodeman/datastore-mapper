package mapper

import (
	"net/http"
)

type (
	apiJobs struct{}
)

func init() {
	server.addResource("/jobs/", new(apiJobs))
}

func (a apiJobs) Get(w http.ResponseWriter, r *http.Request, id string) (int, interface{}, error) {
	data := jobRegistry
	return http.StatusOK, data, nil
}

// Get - list job states
// Post - start job execution - type, shards, queue
// Put/Patch? - abort job
// Delete - cleanup job
