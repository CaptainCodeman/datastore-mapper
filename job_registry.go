package mapper

import (
	"errors"
	"reflect"

	"encoding/gob"
)

type (
	// jobTypes maps job name to type
	jobTypes map[string]reflect.Type
)

var (
	// internal map of job names to types
	jobRegistry jobTypes = make(map[string]reflect.Type)

	// ErrJobNotFound is returned when an unregistered
	// job is requested
	ErrJobNotFound = errors.New("job not found")
)

// RegisterJob registers jobs so they can be initiated by name and
// so the Job struct can be registered with the gob serializer.
func RegisterJob(job JobSpec) error {
	jobType := reflect.TypeOf(job).Elem()
	jobName := jobType.String()
	jobRegistry[jobName] = jobType
	gob.Register(job)
	return nil
}

// CreateJobInstance creates a new JobSpec instance from the given name
func CreateJobInstance(name string) (JobSpec, error) {
	jobType, ok := jobRegistry[name]
	if !ok {
		return nil, ErrJobNotFound
	}
	return reflect.New(jobType).Interface().(JobSpec), nil
}
