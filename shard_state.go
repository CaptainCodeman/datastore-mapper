package mapper

import (
	"bytes"
	"io"
	"strconv"
	"strings"
	"time"

	"encoding/gob"
	"io/ioutil"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	apistorage "google.golang.org/api/storage/v1"
	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/cloud"
	cstorage "google.golang.org/cloud/storage"
)

type (
	// shardState processes shards for a job (slices of a namespace)
	shardState struct {
		common
		lock

		// Shard is the shard number within this namespace
		Shard int `datastore:"shard,noindex"`

		// Namespace is the namespace for this shard
		Namespace string `datastore:"namespace,noindex"`

		// Description is the description for this shard
		Description string `datastore:"description,noindex"`

		// KeyRange is the keyrange for this shard
		KeyRange *KeyRange `datastore:"-"`

		// Cursor is the datastore cursor to start from
		Cursor string `datastore:"cursor,noindex"`

		job *jobState
	}
)

const (
	shardKind = "shard"
)

func (s *shardState) copyFrom(x shardState) {
	s.common = x.common
	s.lock = x.lock
	s.Shard = x.Shard
	s.Namespace = x.Namespace
	s.Description = x.Description
	s.KeyRange = x.KeyRange
	s.Cursor = x.Cursor
}

func (s *shardState) jobID() string {
	parts := strings.Split(s.id, "-")
	return parts[0] + "-" + parts[1]
}

func (s *shardState) namespaceID() string {
	parts := strings.Split(s.id, "-")
	return parts[0] + "-" + parts[1] + "-" + parts[2]
}

func (s *shardState) shardFilename() string {
	parts := strings.Split(s.id, "-")
	ns := parts[2]
	if ns == "" {
		ns = "~"
	}
	// mapper type / unique id / namespace / shard
	// TODO: set filename or at least extension
	return parts[0] + "-" + parts[1] + "/" + ns + "/" + parts[3] + ".json"
}

func (s *shardState) sliceFilename(slice int) string {
	parts := strings.Split(s.id, "-")
	ns := parts[2]
	if ns == "" {
		ns = "~"
	}
	// mapper type / unique id / namespace / shard / slice
	// TODO: set filename or at least extension
	return parts[0] + "-" + parts[1] + "/" + ns + "/" + parts[3] + "/" + strconv.Itoa(slice) + ".json"
}

func (s *shardState) createQuery(c context.Context) *datastore.Query {
	q := s.job.Query.toDatastoreQuery()
	return q
}

func (s *shardState) iterate(c context.Context) (bool, error) {
	// switch namespace
	c, _ = appengine.Namespace(c, s.Namespace)

	// use the full 10 minutes allowed (assuming front-end instance type)
	c, _ = context.WithTimeout(c, time.Duration(10)*time.Minute)

	size := int(s.job.Query.limit)
	if size == 0 {
		size = 100
	}

	q := s.createQuery(c)
	q = s.KeyRange.FilterQuery(q)
	q = q.Limit(size)

	var cursor *datastore.Cursor
	if s.Cursor != "" {
		newCursor, err := datastore.DecodeCursor(s.Cursor)
		if err != nil {
			log.Errorf(c, "get start cursor error %s", err.Error())
			return false, err
		}
		cursor = &newCursor
	}

	// we query and proecess in batches of 'size' and also create a
	// timeout signal that is checked after each batch. So, a batch has
	// to take less than 5 minutes to execute
	timeout := make(chan bool, 1)
	timer := time.AfterFunc(time.Duration(5)*time.Minute, func() {
		timeout <- true
	})
	defer timer.Stop()

	// TODO: check one of these is enabled when job is started
	single, useSingle := s.job.Job.(JobSingle)
	batch, useBatch := s.job.Job.(JobBatched)
	if useSingle && useBatch {
		useSingle = false
	}

	// if job has output defined then create writer for it
	var w io.Writer
	if s.job.Bucket == "" {
		w = ioutil.Discard
	} else {
		// for development we can't use the appengine default credentials so
		// instead need to create our own oauth token source to access storage

		// TODO: maybe give job a chance to generate this - it could also
		// create the writer (?). The only reason we're doing it is to prevent
		// duplication and also handle the file rollup operations
		var client *cstorage.Client
		if appengine.IsDevAppServer() {
			jsonKey, err := ioutil.ReadFile("service-account.json")
			if err != nil {
				return false, err
			}
			conf, err := google.JWTConfigFromJSON(jsonKey, cstorage.ScopeFullControl)
			if err != nil {
				return false, err
			}
			client, err = cstorage.NewClient(c, cloud.WithTokenSource(conf.TokenSource(c)))
			if err != nil {
				return false, err
			}
		} else {
			var err error
			client, err = cstorage.NewClient(c)
			if err != nil {
				return false, err
			}
		}
		defer client.Close()

		o := client.Bucket(s.job.Bucket).Object(s.sliceFilename(s.Sequence)).NewWriter(c)
		defer o.Close()

		// TODO: wrap writer to count bytes and continue slice if we get close to 10Mb limit
		w = o
	}

	for {
		count := 0

		if cursor != nil {
			q = q.Start(*cursor)
		}

		it := q.Run(c)
		keys := make([]*datastore.Key, 0, size)
		for {
			key, err := it.Next(nil)
			if err == datastore.Done {
				break
			}
			if err != nil {
				log.Errorf(c, "error %s", err.Error())
				return false, err
			}

			if useSingle {
				// TODO: check for return errors
				single.Single(c, w, s.Counters, key)
				s.Count++
			} else {
				keys = append(keys, key)
			}

			count++
		}

		if useBatch {
			// TODO: check for return errors
			batch.Batch(c, w, s.Counters, keys)
			s.Count += int64(len(keys))
		}

		// did we process a full batch? if not, we hit the end of the dataset
		if count < size {
			return true, nil
		}

		newCursor, err := it.Cursor()
		if err != nil {
			log.Errorf(c, "get next cursor error %s", err.Error())
			return false, err
		}
		cursor = &newCursor
		s.Cursor = cursor.String()

		// check if we've timed out
		select {
		case <-timeout:
			// timeout without completing
			return false, nil
		default:
			// continue processing in thsi request
		}
	}
}

// rollup shard slices into single file
func (s *shardState) rollup(c context.Context) error {
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

/* datastore */
func (s *shardState) Load(props []datastore.Property) error {
	datastore.LoadStruct(s, props)
	s.common.Load(props)
	s.lock.Load(props)

	for _, prop := range props {
		switch prop.Name {
		case "key_range":
			s.KeyRange = &KeyRange{}
			payload := bytes.NewBuffer(prop.Value.([]byte))
			enc := gob.NewDecoder(payload)
			if err := enc.Decode(&s.KeyRange); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *shardState) Save() ([]datastore.Property, error) {
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

	payload := new(bytes.Buffer)
	enc := gob.NewEncoder(payload)
	if err := enc.Encode(&s.KeyRange); err != nil {
		return nil, err
	}
	props = append(props, datastore.Property{Name: "key_range", Value: payload.Bytes(), NoIndex: true, Multiple: false})

	return props, err
}
