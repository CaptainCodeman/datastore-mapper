package main

import (
	"time"
)

type (
	// Photographer who took the photo
	Photographer struct {
		ID   int64  `json:"id"   datastore:"id"`
		Name string `json:"name" datastore:"name,noindex"`
	}

	// Photo is an image in the system
	Photo struct {
		ID           int64        `json:"id"           datastore:"-"`
		Photographer Photographer `json:"photographer" datastore:"photographer"`
		Uploaded     time.Time    `json:"uploaded"     datastore:"uploaded,noindex"`
		Width        int          `json:"width"        datastore:"width,noindex"`
		Height       int          `json:"height"       datastore:"height,noindex"`
		Taken        time.Time    `json:"taken"        datastore:"taken"`
		TakenDay     time.Time    `json:"-"            datastore:"taken_day"` // to allow sharding on date with scatter
	}
)
