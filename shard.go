package mapper

import (
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"

	"io/ioutil"

	cstorage "cloud.google.com/go/storage"
	"github.com/captaincodeman/datastore-locker"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	apistorage "google.golang.org/api/storage/v1"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"
)

type (
	// shard processes shards for a job (slices of a namespace)
	shard struct {
		locker.Lock
		common `datastore:"-"`

		// Namespace is the namespace for this shard
		Namespace string `datastore:"namespace,noindex"`

		// Shard is the shard number within this namespace
		Shard int `datastore:"shard,noindex"`

		// Description is the description for this shard
		Description string `datastore:"description,noindex"`

		// Cursor is the datastore cursor to start from
		Cursor string `datastore:"cursor,noindex"`
	}
)

const (
	shardKind = "shard"
)

func (s *shard) setJob(job *job) {
	s.job = job
}

func (s *shard) copyFrom(x shard) {
	s.Lock = x.Lock
	s.common = x.common
	s.Shard = x.Shard
	s.Namespace = x.Namespace
	s.Description = x.Description
	s.Cursor = x.Cursor
}

func (s *shard) namespaceKey(c context.Context, config Config) *datastore.Key {
	return datastore.NewKey(c, config.DatastorePrefix+namespaceKind, s.namespaceID(), 0, nil)
}

func (s *shard) jobID() string {
	parts := strings.Split(s.id, "/")
	return parts[0] + "/" + parts[1]
}

func (s *shard) namespaceID() string {
	parts := strings.Split(s.id, "/")
	return parts[0] + "/" + parts[1] + "/" + parts[2]
}

func (s *shard) shardFilename() string {
	parts := strings.Split(s.id, "/")
	ns := parts[2]
	if ns == "" {
		ns = "~"
	}
	// mapper type / unique id / namespace / shard
	// TODO: set filename or at least extension
	return parts[0] + "/" + parts[1] + "/" + ns + "/" + parts[3] + ".json"
}

func (s *shard) sliceFilename(slice int) string {
	parts := strings.Split(s.id, "/")
	ns := parts[2]
	if ns == "" {
		ns = "~"
	}
	// mapper type / unique id / namespace / shard / slice
	// TODO: set filename or at least extension
	return parts[0] + "/" + parts[1] + "/" + ns + "/" + parts[3] + "/" + strconv.Itoa(slice) + ".json"
}

func (s *shard) iterate(c context.Context, mapper *mapper) (bool, error) {
	// switch namespace
	c, _ = appengine.Namespace(c, s.Namespace)

	taskTimeout := time.After(mapper.config.TaskTimeout)
	taskRunning := true

	jobOutput, useJobOutput := s.job.JobSpec.(JobOutput)
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
		q = q.Filter(f.FieldName+" "+f.Op.String(), f.Value)
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
	var (
		entity                 interface{}
		entityElem, entityZero reflect.Value
	)

	// is full loading implemented?
	jobEntity, useJobEntity := s.job.JobSpec.(JobEntity)
	if useJobEntity {
		entity = jobEntity.Make()
		entityElem = reflect.ValueOf(entity).Elem()
		entityZero = reflect.Zero(entityElem.Type())
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
		cursorTimeout := time.After(mapper.config.CursorTimeout)
		// datastore cursor context needs to run for the max allowed
		cc, _ := context.WithTimeout(c, time.Duration(60)*time.Second)
		it := q.Run(cc)

		// item loop to iterate cursor
	cursorLoop:
		for {
			// reset entity to zero, otherwise struct slices leak data between loads
			if useJobEntity {
				entityElem.Set(entityZero)
			}

			key, err := it.Next(entity)
			if err == datastore.Done {
				// we reached the end
				return true, nil
			}

			// TODO: option to fail or continue on individual errors
			// or add error handling logic to job to give it a chance (?)
			if err != nil {
				log.Errorf(c, "key %v error %v", key, err)
				// return false, err
				continue cursorLoop
			}

			if err := s.job.JobSpec.Next(c, s.Counters, key); err != nil {
				// TODO: instead of failing the entire slice, try to figure
				// out if it's possible to continue from this point or maybe
				// the last cursor position to avoid re-processing entities.
				// NOTE: this would need to truncate any output file being
				// written so entries weren't doubled up but maybe possible.
				return false, err
			}
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

func (s *shard) completed(c context.Context, mapper *mapper, key *datastore.Key) error {
	s.complete()
	s.Cursor = ""

	// update shard status and owning namespace within a transaction
	queue, ok := locker.QueueFromContext(c)
	if !ok {
		queue = mapper.config.DefaultQueue
	}
	fresh := new(shard)
	nsKey := s.namespaceKey(c, *mapper.config)
	ns := new(namespace)

	return datastore.RunInTransaction(c, func(tc context.Context) error {
		keys := []*datastore.Key{key, nsKey}
		vals := []interface{}{fresh, ns}
		if err := datastore.GetMulti(tc, keys, vals); err != nil {
			return err
		}

		fresh.copyFrom(*s)
		fresh.Lock.Complete()

		ns.ShardsSuccessful++
		ns.common.rollup(s.common)

		// if all shards have completed, schedule namespace/completed to update job
		if ns.ShardsSuccessful == ns.ShardsTotal {
			t := mapper.locker.NewTask(nsKey, ns, mapper.config.Path+namespaceCompleteURL, nil)
			if _, err := taskqueue.Add(tc, t, queue); err != nil {
				return err
			}
		}

		if _, err := datastore.PutMulti(tc, keys, vals); err != nil {
			return err
		}

		return nil
	}, &datastore.TransactionOptions{XG: true})
}

func (s *shard) createOutputFile(c context.Context) (io.WriteCloser, error) {
	c, _ = context.WithTimeout(c, time.Duration(10)*time.Minute)
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
		conf, err := google.JWTConfigFromJSON(jsonKey, cstorage.ScopeReadWrite)
		if err != nil {
			return nil, err
		}
		client, err = cstorage.NewClient(c, option.WithTokenSource(conf.TokenSource(c)))
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
	// nothing to do if no output writing
	if s.job.Bucket == "" {
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

	target := s.shardFilename()
	sources := make([]string, s.Sequence-1)
	for i := 1; i < s.Sequence; i++ {
		sources[i-1] = s.sliceFilename(i)
	}

	return s.combineFiles(c, service, target, sources)
}

// TODO: reuse logic from namespace to avoid duplication
func (s *shard) combineFiles(c context.Context, service *apistorage.Service, target string, sources []string) error {
	if _, err := service.Objects.Get(s.job.Bucket, target).Context(c).Do(); err != nil {

		count := len(sources)

		switch {

		case count == 1:
			// copy single file
			_, err := service.Objects.Copy(s.job.Bucket, sources[0], s.job.Bucket, target, nil).Context(c).Do()
			return err

		case count <= 32:
			// combine source files to target
			log.Debugf(c, "compose %s from %d sources", target, count)
			req := &apistorage.ComposeRequest{
				Destination:   &apistorage.Object{Name: target},
				SourceObjects: make([]*apistorage.ComposeRequestSourceObjects, count),
			}
			for i := 0; i < count; i++ {
				log.Debugf(c, "source %d %s", i, sources[i])
				req.SourceObjects[i] = &apistorage.ComposeRequestSourceObjects{Name: sources[i]}
			}

			res, err := service.Objects.Compose(s.job.Bucket, target, req).Context(c).Do()
			if err != nil {
				return err
			}
			log.Debugf(c, "created file %s gen %d %d bytes", res.Name, res.Generation, res.Size)

		case count > 32:
			// rollup to intermediate files
			steps := ((count - 1) / 32) + 1
			targets := make([]string, steps)
			for i := 0; i < steps; i++ {
				targets[i] = fmt.Sprintf("%s-%d", target, i)

				start := i * 32
				stop := i*32 + 32
				if stop > count {
					stop = count
				}
				if err := s.combineFiles(c, service, targets[i], sources[start:stop]); err != nil {
					return err
				}
			}

			if err := s.combineFiles(c, service, target, targets); err != nil {
				return err
			}
		}

		// delete source files
		for i := 0; i < count; i++ {
			log.Debugf(c, "delete %s", sources[i])
			if err := service.Objects.Delete(s.job.Bucket, sources[i]).Context(c).Do(); err != nil {
				// TODO: check for 404 error and ignore but return err for failures (to ensure cleanup)
				return nil
			}
		}
	}

	return nil
}

// Load implements the datastore PropertyLoadSaver imterface
func (s *shard) Load(props []datastore.Property) error {
	datastore.LoadStruct(s, props)
	s.common.Load(props)
	return nil
}

// Save implements the datastore PropertyLoadSaver imterface
func (s *shard) Save() ([]datastore.Property, error) {
	props, err := datastore.SaveStruct(s)

	cprops, err := s.common.Save()
	if err != nil {
		return nil, err
	}
	props = append(props, cprops...)

	return props, err
}
