package main

import (
	"net/http"

	"github.com/captaincodeman/datastore-mapper"
)

func init() {
	mapper.UseDatastore()
	mapperServer, _ := mapper.NewServer(mapper.DefaultPath,
		mapper.DatastorePrefix("map_"),
		mapper.Oversampling(16),
		mapper.Retries(8),
		mapper.LogVerbose,
	)
	http.Handle(mapper.DefaultPath, mapperServer)
}
