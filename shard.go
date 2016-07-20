package mapper

import (
	"io"
	"strconv"
	"strings"
	"time"

	"io/ioutil"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	apistorage "google.golang.org/api/storage/v1"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"
	"google.golang.org/cloud"
	cstorage "google.golang.org/cloud/storage"
)

type (
	// shard processes shards for a job (slices of a namespace)
	shard struct {
		common
		lock

		// Shard is the shard number within this namespace
		Shard int `datastore:"shard,noindex"`

		// Namespace is the namespace for this shard
		Namespace string `datastore:"namespace,noindex"`

		// Description is the description for this shard
		Description string `datastore:"description,noindex"`

		// Cursor is the datastore cursor to start from
		Cursor string `datastore:"cursor,noindex"`

		job *job
	}
)

const (
	shardKind = "shard"
)

func (s *shard) setJob(job *job) {
	s.job = job
}

func (s *shard) copyFrom(x shard) {
	s.common = x.common
	s.lock = x.lock
	s.Shard = x.Shard
	s.Namespace = x.Namespace
	s.Description = x.Description
	s.Query = x.Query
	s.Cursor = x.Cursor
}

func (s *shard) namespaceKey(c context.Context, config Config) *datastore.Key {
	return datastore.NewKey(c, config.DatastorePrefix+namespaceKind, s.namespaceID(), 0, nil)
}

func (s *shard) jobID() string {
	parts := strings.Split(s.id, "-")
	return parts[0] + "-" + parts[1]
}

func (s *shard) namespaceID() string {
	parts := strings.Split(s.id, "-")
	return parts[0] + "-" + parts[1] + "-" + parts[2]
}

func (s *shard) shardFilename() string {
	parts := strings.Split(s.id, "-")
	ns := parts[2]
	if ns == "" {
		ns = "~"
	}
	// mapper type / unique id / namespace / shard
	// TODO: set filename or at least extension
	return parts[0] + "-" + parts[1] + "/" + ns + "/" + parts[3] + ".json"
}

func (s *shard) sliceFilename(slice int) string {
	parts := strings.Split(s.id, "-")
	ns := parts[2]
	if ns == "" {
		ns = "~"
	}
	// mapper type / unique id / namespace / shard / slice
	// TODO: set filename or at least extension
	return parts[0] + "-" + parts[1] + "/" + ns + "/" + parts[3] + "/" + strconv.Itoa(slice) + ".json"
}

func (s *shard) iterate(c context.Context, config Config) (bool, error) {
	// switch namespace
	c, _ = appengine.Namespace(c, s.Namespace)

	taskTimeout := time.After(config.TaskTimeout)
	taskRunning := true

	jobOutput, useJobOutput := s.jobSpec.(JobOutput)
	if useJobOutput && s.job.Bucket != "" {
		w, err := s.createOutputFile(c)
		if err != nil {
			return false, err
		}
		defer w.Close()
		jobOutput.Output(w)
	}

	q := datastore.NewQuery(s.Query.kind)
	for _, f := range s.Query.filter {
		q = q.Filter(f.FieldName+" "+operatorToString[f.Op], f.Value)
	}

	var cursor *datastore.Cursor
	if s.Cursor != "" {
		newCursor, err := datastore.DecodeCursor(s.Cursor)
		if err != nil {
			log.Errorf(c, "get start cursor error %s", err.Error())
			return false, err
		}
		cursor = &newCursor
	}

	// what we'll load into if doing full entity loads (i.e. not doing KeysOnly)
	var entity interface{}

	// is full loading implemented?
	jobEntity, useJobEntity := s.jobSpec.(JobEntity)
	if useJobEntity {
		entity = jobEntity.Make()
	} else {
		q = q.KeysOnly()
	}

	// main task loop to repeat datastore query with cursor
	for taskRunning {

		// if cursor is set, start the query at that point
		if cursor != nil {
			q = q.Start(*cursor)
		}

		// limit how long the cursor can run before we requery
		cursorTimeout := time.After(config.CursorTimeout)
		// datastore cursor context needs to run for the max allowed
		cc, _ := context.WithTimeout(c, time.Duration(60)*time.Second)
		it := q.Run(cc)

		// item loop to iterate cursor
	cursorLoop:
		for {
			key, err := it.Next(entity)
			if err == datastore.Done {
				// we reached the end
				return true, nil
			}

			if err != nil {
				log.Errorf(c, "error %s", err.Error())
				return false, err
			}

			// TODO: handle task errors (fail slice?)
			s.jobSpec.Next(c, s.Counters, key)
			s.Count++

			select {
			case <-taskTimeout:
				// clearing the flag breaks us out of the task loop but also lets us update the
				// cursor first when we break from the inner cursorLoop
				taskRunning = false
				break cursorLoop
			default:
				select {
				case <-cursorTimeout:
					// this forces a new cursor and query so we don't suffer from datastore timeouts
					break cursorLoop
				default:
					// no timeout so carry on with the current cursor
					continue cursorLoop
				}
			}
		}

		// we need to get the cursor for where we are upto whether we are requerying
		// within this task or scheduling a new continuation slice
		newCursor, err := it.Cursor()
		if err != nil {
			log.Errorf(c, "get next cursor error %s", err.Error())
			return false, err
		}
		cursor = &newCursor
		s.Cursor = cursor.String()
	}

	return false, nil
}

func (s *shard) completed(c context.Context, config Config, key *datastore.Key) error {
	s.complete()
	s.Cursor = ""
	s.RequestID = ""

	// update shard status and owning namespace within a transaction
	fresh := new(shard)
	nsKey := s.namespaceKey(c, config)
	ns := new(namespace)

	return storage.RunInTransaction(c, func(tc context.Context) error {
		keys := []*datastore.Key{key, nsKey}
		vals := []interface{}{fresh, ns}
		if err := storage.GetMulti(tc, keys, vals); err != nil {
			return err
		}

		fresh.copyFrom(*s)
		ns.ShardsSuccessful++
		ns.common.rollup(s.common)

		// if all shards have completed, schedule namespace/completed to update job
		if ns.ShardsSuccessful == ns.ShardsTotal {
			t := NewLockTask(nsKey, ns, config.Path+namespaceCompleteURL, nil)
			if _, err := taskqueue.Add(tc, t, s.queue); err != nil {
				return err
			}
		}

		if _, err := storage.PutMulti(tc, keys, vals); err != nil {
			return err
		}

		return nil
	}, &datastore.TransactionOptions{XG: true, Attempts: attempts})
}

func (s *shard) createOutputFile(c context.Context) (io.WriteCloser, error) {
	// for development we can't use the appengine default credentials so
	// instead need to create our own oauth token source to access storage

	// TODO: maybe give job a chance to generate this - it could also
	// create the writer (?). The only reason we're doing it is to prevent
	// duplication and also handle the file rollup operations
	var client *cstorage.Client
	if appengine.IsDevAppServer() {
		jsonKey, err := ioutil.ReadFile("service-account.json")
		if err != nil {
			return nil, err
		}
		conf, err := google.JWTConfigFromJSON(jsonKey, cstorage.ScopeFullControl)
		if err != nil {
			return nil, err
		}
		client, err = cstorage.NewClient(c, cloud.WithTokenSource(conf.TokenSource(c)))
		if err != nil {
			return nil, err
		}
	} else {
		var err error
		client, err = cstorage.NewClient(c)
		if err != nil {
			return nil, err
		}
	}

	o := client.Bucket(s.job.Bucket).Object(s.sliceFilename(s.Sequence)).NewWriter(c)

	// TODO: wrap writer to count bytes and continue slice if we get close to 10Mb limit (?)
	return o, nil
}

// rollup shard slices into single file
func (s *shard) rollup(c context.Context) error {
	// nothing to do if no output writing or no output written
	if s.job.Bucket == "" || s.Count == 0 {
		return nil
	}

	var service *apistorage.Service
	if appengine.IsDevAppServer() {
		jsonKey, err := ioutil.ReadFile("service-account.json")
		if err != nil {
			return err
		}
		conf, err := google.JWTConfigFromJSON(jsonKey, apistorage.DevstorageReadWriteScope)
		if err != nil {
			return err
		}
		client := conf.Client(c)
		service, err = apistorage.New(client)
		if err != nil {
			return err
		}
	} else {
		var err error
		token := google.AppEngineTokenSource(c, apistorage.DevstorageReadWriteScope)
		client := oauth2.NewClient(c, token)
		service, err = apistorage.New(client)
		if err != nil {
			return err
		}
	}

	// TODO: if only 1 slice, copy / move file instead
	// TODO: check if shard file already exists and skip compose
	// TODO: logic to handle more than 32 composable files

	shardFilename := s.shardFilename()
	log.Debugf(c, "compose %s", shardFilename)
	req := &apistorage.ComposeRequest{
		Destination:   &apistorage.Object{Name: shardFilename},
		SourceObjects: make([]*apistorage.ComposeRequestSourceObjects, s.Sequence-1),
	}
	for i := 1; i < s.Sequence; i++ {
		sliceFilename := s.sliceFilename(i)
		log.Debugf(c, "source %s", sliceFilename)
		req.SourceObjects[i-1] = &apistorage.ComposeRequestSourceObjects{Name: sliceFilename}
	}

	res, err := service.Objects.Compose(s.job.Bucket, shardFilename, req).Context(c).Do()
	if err != nil {
		return err
	}
	log.Debugf(c, "created shard file %s gen %d %d bytes", res.Name, res.Generation, res.Size)

	// delete slice files
	for i := 1; i < s.Sequence; i++ {
		log.Debugf(c, "delete %s", s.sliceFilename(i))
		service.Objects.Delete(s.job.Bucket, s.sliceFilename(i)).Context(c).Do()
		// do we care about errors? provide separate cleanup option
	}

	return nil
}

// Load implements the datastore PropertyLoadSaver imterface
func (s *shard) Load(props []datastore.Property) error {
	datastore.LoadStruct(s, props)
	s.common.Load(props)
	s.lock.Load(props)
	return nil
}

// Save implements the datastore PropertyLoadSaver imterface
func (s *shard) Save() ([]datastore.Property, error) {
	props, err := datastore.SaveStruct(s)

	jprops, err := s.common.Save()
	if err != nil {
		return nil, err
	}
	props = append(props, jprops...)

	lprops, err := s.lock.Save()
	if err != nil {
		return nil, err
	}
	props = append(props, lprops...)

	return props, err
}
