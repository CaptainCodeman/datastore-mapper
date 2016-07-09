package mapper

import (
	"fmt"
	"net/http"
	"strconv"

	"google.golang.org/appengine"
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
	values := r.URL.Query()
	name := values.Get("type")
	job, _ := CreateJobInstance(name)
	if job == nil {
		return http.StatusBadRequest, nil, fmt.Errorf("job type not found")
	}

	shards, _ := strconv.Atoi(values.Get("shards"))
	queue := values.Get("queue")
	query, _ := job.Query(r)

	c := appengine.NewContext(r)

	StartJob(c, job, query, queue, shards)
	data := map[string]interface{}{
		"started": true,
	}
	return http.StatusOK, data, nil
}

// Get - list job states
// Post - start job execution - type, shards, queue
// Put/Patch? - abort job
// Delete - cleanup job
