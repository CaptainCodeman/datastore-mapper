package mapper

import (
	"strings"

	"net/http"

	"github.com/captaincodeman/datastore-locker"
)

const (
	// DefaultPath is a decent default path to mount the mapper mux on
	DefaultPath = "/_ah/mapper/"
)

type (
	// mapper holds the http mux we will attach our handlers to and the
	// the path it is mounted at so we can generate tasks to address it
	mapper struct {
		*http.ServeMux
		locker *locker.Locker
		config *Config
	}
)

var (
	// there can only be one instance and we need it created before init
	// so that we can attach out handlers to it. Their paths are relative
	// so it's OK that we don't yet know the prefix it will be mounted at.
	server = newMapper()
)

// NewServer configures the server and returns the handler for mounting
// within the app so it can control the endpoint to use. The server is
// actually already created but we need to know what the path prefix is.
func NewServer(path string, options ...Option) (http.Handler, error) {
	server.config.Path = strings.TrimSuffix(path, "/")
	for _, option := range options {
		if err := option(server.config); err != nil {
			return nil, err
		}
	}
	handler := http.StripPrefix(server.config.Path, server)

	// pass on locker options
	lockerOptions := []locker.Option{
		locker.LeaseDuration(server.config.LeaseDuration),
		locker.LeaseTimeout(server.config.LeaseTimeout),
		locker.DefaultQueue(server.config.DefaultQueue),
		locker.MaxRetries(server.config.Retries),
		locker.Host(server.config.Host),
	}
	if server.config.LogVerbose {
		lockerOptions = append(lockerOptions, locker.LogVerbose)
	}
	var err error
	server.locker, err = locker.NewLocker(lockerOptions...)
	if err != nil {
		return nil, err
	}

	return handler, nil
}

func newMapper() *mapper {
	return &mapper{
		ServeMux: http.NewServeMux(),
		config:   newConfig(),
	}
}
