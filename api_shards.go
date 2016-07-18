package mapper

import (
//	"net/http"

//	"google.golang.org/appengine"
//	"google.golang.org/appengine/log"
//	"google.golang.org/cloud/datastore"
)

/*
type (
	apiShards struct{}
)

func init() {
	server.addResource("/shards/", new(apiShards))
}

func (a apiShards) Get(w http.ResponseWriter, r *http.Request, id string) (int, interface{}, error) {
	c := appengine.NewContext(r)
	data, _ := listJobs(c)
	return http.StatusOK, data, nil
}

// Kickoff shard execution for job
func (a apiShards) Post(w http.ResponseWriter, r *http.Request, id string) (int, interface{}, error) {
	values := r.URL.Query()
	job := values.Get("job")

	c := appengine.NewContext(r)

	jobState, err := GetJobState(c, job)
	log.Debugf(c, "jobState %#v", jobState)
	if err == datastore.ErrNoSuchEntity {
		log.Debugf(c, "Job State for job %s is missing. Dropping Task.", job)
		return http.StatusNotFound, nil, err
	}
	if err != nil {
		log.Errorf(c, "error %s", err.Error())
		return http.StatusInternalServerError, nil, err
	}
	if !jobState.Active {
		log.Debugf(c, "Mapreduce %s is not active. Looks like spurious task execution. Dropping Task.", job)
		return http.StatusOK, nil, nil
	}

	queue := r.Header.Get("X-AppEngine-QueueName")
	err = jobState.kickoff(c, queue)
	if err != nil {
		log.Errorf(c, "error %s", err.Error())
		return http.StatusInternalServerError, nil, err
	}

	return http.StatusOK, nil, nil
}
*/

// Get - list job states
// Post - start job execution - type, shards, queue
// Put/Patch? - abort job
// Delete - cleanup job
