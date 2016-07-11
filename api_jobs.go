package mapper

import (
	"net/http"
)

type (
	apiJobs struct{}
)

func init() {
	Server.AddResource(new(apiJobs), "/jobs/")
}

/*
func (a apiJobs) Get(w http.ResponseWriter, r *http.Request, id string) (int, interface{}, error) {
	c := appengine.NewContext(r)
	data, _ := listJobs(c)
	return http.StatusOK, data, nil
}
*/

func (a apiJobs) Post(w http.ResponseWriter, r *http.Request, id string) (int, interface{}, error) {
	if err := StartJob(r); err != nil {
		return http.StatusBadRequest, nil, err
	}
	data := map[string]interface{}{
		"started": true,
	}
	return http.StatusOK, data, nil
}

// Get - list job states
// Post - start job execution - type, shards, queue
// Put/Patch? - abort job
// Delete - cleanup job
