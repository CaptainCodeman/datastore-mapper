package main

import (
	"net/http"

	"github.com/captaincodeman/datastore-mapper"
)

func init() {
	mapperServer, _ := mapper.NewServer(mapper.DefaultPath)
	http.Handle(mapper.DefaultPath, mapperServer)
}
