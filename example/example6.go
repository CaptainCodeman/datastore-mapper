package main

import (
	"io/ioutil"
	"net/http"

	"github.com/qedus/nds"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/bigquery/v2"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"

	"github.com/captaincodeman/datastore-mapper"
)

type (
	// streaming insert into BigQuery
	example6 struct {
		appID string
		bq    *bigquery.Service
	}
)

func init() {
	mapper.RegisterJob(&example6{})
}

func (x *example6) Query(r *http.Request) (*mapper.Query, error) {
	q := mapper.NewQuery("photo")
	q = q.NamespaceEmpty()
	return q, nil
}

func (x *example6) SliceStarted(c context.Context, id string, namespace string, shard, slice int) {
	x.bq, _ = bigqueryService(c)
	x.appID = appengine.AppID(c)
}

func (x *example6) SliceCompleted(c context.Context, id string, namespace string, shard, slice int) {
}

// Next processes the next item
func (x *example6) Next(c context.Context, counters mapper.Counters, key *datastore.Key) error {
	// we need to load the entity ourselves
	photo := new(Photo)
	if err := nds.Get(c, key, photo); err != nil {
		return err
	}
	photo.ID = key.IntID()

	suffix := photo.Taken.Format("20060102")
	_, err := x.bq.Tabledata.InsertAll(x.appID, "datasetName", "tableName", &bigquery.TableDataInsertAllRequest{
		TemplateSuffix: suffix,
		Rows: []*bigquery.TableDataInsertAllRequestRows{
			{
				Json: map[string]bigquery.JsonValue{
					"id":    photo.ID,
					"taken": photo.Taken,
					"photographer": map[string]bigquery.JsonValue{
						"id":   photo.Photographer.ID,
						"name": photo.Photographer.Name,
					},
				},
			},
		},
	}).Context(c).Do()
	return err
}

func bigqueryService(c context.Context) (*bigquery.Service, error) {
	var client *http.Client
	if appengine.IsDevAppServer() {
		jsonKey, err := ioutil.ReadFile("service-account.json")
		if err != nil {
			return nil, err
		}
		conf, err := google.JWTConfigFromJSON(jsonKey, bigquery.BigqueryScope)
		if err != nil {
			return nil, err
		}
		client = conf.Client(c)
	} else {
		token := google.AppEngineTokenSource(c, bigquery.BigqueryScope)
		client = oauth2.NewClient(c, token)
	}
	service, err := bigquery.New(client)
	if err != nil {
		return nil, err
	}
	return service, nil
}
