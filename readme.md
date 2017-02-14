# Datastore Mapper
This is an implementation of the appengine datastore mapper functionality of the
[appengine map-reduce framework](https://github.com/GoogleCloudPlatform/appengine-mapreduce)
developed using in [Go](https://golang.org/).

## Status
It is alpha status while I develop the API and should not be used in production yet but
it's been successfully used to export datastore entities to JSON for import into BigQuery,
streaming inserts directly into BigQuery and for schema migrations and lightweight aggregation
reporting.

More work is needed to finish the query sharding when using ancestor queries and to make
the cloud storage file output writing more robust.

## Usage
The mapper server is a regular Go `http.Handler` so can be hosted at any path. Just make
sure that the path used is also passed in to the constructor. The default path provided
is `/_ah/mapper/`.

Adding with default options:

    func init() {
        mapperServer, _ := mapper.NewServer(mapper.DefaultPath)
        http.Handle(mapper.DefaultPath, mapperServer)
    }

The default configuration can be overridden by passing additional options, for example:

    func init() {
        mapperServer, _ := mapper.NewServer(mapper.DefaultPath,
            mapper.DatastorePrefix("map_"),
            mapper.Oversampling(16),
            mapper.Retries(8),
            mapper.LogVerbose,
        )
        http.Handle(mapper.DefaultPath, mapperServer)
    }

See the `/config.go` file for all configuration options.

## Mapper Jobs
Mapper Jobs are defined by creating a Go struct that implements the `JobSpec` interface and
registering it with the mapper:

    func init() {
        mapper.RegisterJob(&mymapper{})
    }

The struct defines the `Query` function used to parse request parameters and create the datastore
query spec:

    func (m *mymapper) Query(r *http.Request) (*Query, error)

Plus, the function that will be called by the mapper as the datastore is iterated:

    func (m *mymapper) Next(c context.Context, counters Counters, key *datastore.Key) error

The datastore will use a 'keys only' query but this can be changed to fetching the full
entities by implementing the `JobEntity` interface to return the entity to load into (which
should be stored as a named field of the job):

    func (m *mymapper) Make() interface{}

To make it easy to output to Cloud Storage a job can implement `JobOutput` which provides
an `io.writer` for the slice being processed. The job can create whatever encoder it needs
to write it's output (JSON works great for BigQuery):

    func (m *mymapper) Output(w io.Writer)
 
There are additional interfaces that can be implemented to receive notification of various
lifecycle events:

* Job Started / Completed
* Namespcae Started / Completed
* Shard Started / Completed
* Slice Started / Completed

See the [/example/](/example/) folder for some examples of the various job types:

* example1: simple keysonly iteration and aggregation using counters
* example2: simple eager iteration and aggregation using counters
* example3: parse request parameters to create query or use defaults for CRON job
* example4: lifecycle notifications
* example5: export custom JSON to Cloud Storage (for batch import into BigQuery)
* example6: streaming inserts into BigQuery

### Local Development
The example application will run locally but currently needs a `service-account.json` credentials
file in order to use the cloud storage output.

Kicking off a job is done by POSTing to the `http://localhost:8080/_ah/mapper/start` endpoint
passing the name of the job spec and optionally:

* **shards**
  The number of shards to use
* **queue**
  The taskqueue to schedule tasks on
* **bucket**
  The GCS bucket to write output to

Example:

    http://localhost:8080/_ah/mapper/start?name=main.example1&shards=8&bucket=staging.[my-app].appspot.com

## Why only the mapper?
Technology moves on and Google's cloud platform now provides other services that handle
many of the things that the Map-Reduce framework was once the solution to. If you need 
any Analytics and Reporting functionality, for instance, you would now likely look at 
using [BigQuery](https://cloud.google.com/bigquery/) instead of trying to create your
own via Map-Reduce.

But the mapper is still needed: it's used to write backups of your Datastore data to 
Cloud storage (which is also the way you export your Datastore data into BigQuery). Many
times it's necessary to iterate over all the data in your system when you need to update
the schema or perform some other operational processing and the mapper provides a great
approach for doing that by enabling easy splitting and distribution of the workload.

Datastore is great as operational app storage but is weak when it comes to reporting so
a common approach is to export data to a queryable store and there is a datastore admin
tool that can provide a backup or export of the entities.

But using the datastore backup isn't always ideal - sometimes you want more control over 
the range and the format of the data being exported. For instance, you may not want or
need the full content of each entity to be exported to BigQuery in the same format is it's
used within the application.

## Why a Go version?
So why not just continue to use the existing Python or Java implementations?

They both continue to work as well as they always have but there were a few specific
reasons that I wanted a solution using Go:

### Performance
My experience of the Python version is that it tends to be rather slow and can only handle
entities in a small batch without the instance memory going over the limit and causing errors
and retries. The Java runtime is faster but also requires larger instances sizes just
to execute before you even start processing anything and, well, it's Java and it's now 2016.

I like that a Go version can run (and run _fast_) even on the smallest F1/B1 micro-instance
consuming just a few Mb of RAM but able to process thousands of entities per second.

### Runtime Cost
The slow performance and larger instance size requirementss for Python and Java both 
result in higher operational costs which I'd prefer to avoid. Each platform also tends
to have it's own memcache implementation to improve Datastore performance and bypassing
any already-populated application cache can also add to the cost due to unecessary datastore
operations. If I'm exporting a days worth of recent 'hot' data for instance, there is a good
chance that a sizeable proportion of that will already be in memcache and if I access it
with the same system I can save some costs. 

### Development Cost
I already have my datastore models developed as part of my Go app and don't want to have
to re-develop and maintain a Python or Java version as well (and sometimes it's not 
straightforward to switch between them with different serialization methods etc...).
The cost of including a whole other language in your solution goes beyond just the code,
it's also the development machine maintenance and all the upgrading, IDE tooling and skills
maintenance that go along with it before you get to things like the mental fatigue of context
switching.

## Performance
Initial testing shows it can process over 100k datastore entities in about a minute
using a single F1 micro instance. This works out to about 1.6k entities per second.

Of course the work can often be divided into smaller pieces so it's very likely that
increasing the number of shards so that requests are run on multiple instances will
result in even greater performance and faster throughput.

## Implementation
I originally started with the idea of strictly following the existing implementations
down to the format of the entities for controlling the tasks so that the UI could
be re-used and the Go version could be a drop-in replacement but this would have also
needed the pipeline library to be implemented and while I got quite a long way with it,
I ultimately decided to abandon the idea once I realized I would only ever be using the
mapper part.

This then meant I was free to re-imagine how some of the mapping could work with the 
same fundamental goals and many of the same ideas but some different design choices
to suit my own app requirements and issues I'd had with the existing framework.

The biggest change is conceptually how the mapper splits up the datastore work to be
distributed between shards. The Python and Java versions both use the `__scatter__`
property (added to about 0.78% or 1 in 128 entities) to get a random distribution of
the data that could be split for processing between shards. This splitting can only
be done within a single namespace though and if the number of namespaces used in an
app was above a certain limit (default 10), they switched instead to sharding on the
namespaces with each shard iterating over a range of namespaces and data within them.

If you are using the namespacing for multi-tenancy and each tenant has very different
volumes of data it can easily result in one shard becoming completely overloaded which
completey destroys the benefit of sharding and distribution which is why the mapper
framework is being used in the first place.

This framework instead creates a producer task to iterate over the namespaces and do
the shard splitting on each of them, assigning separate shards within each namespace
to ensure the work can be split evenly but with some heuristics so that the number of
shards used for a namespace that doesn't contain much data is automatically reduced.

So, instead of the number of shards being fixed, it becomes more of a 'minimum target'
for splitting the data as long as it makes sense to do so. Ultimately, we want the
work split into sensible but meaninful chunks so that it can be parallelized and it
doesn't matter if there are 8 shards with 10 slices each or 800 shards of 1 slice, the
task queue processing rate and concurrency limits are what manage how fast it will
all be scheduled and executed.

With each namespace-shard we still use the same slice approach to process the range
of data allocated to it and allow for the app-engine request timeout limitations and
retry / restart ability with some simplified locking / leasing to prevent duplicate
task execution. 

## Workflow
Here is the basic execution workflow:

Define a Job (can be a simple struct) that implements the basic Job interface used to
define the Query, and the Next method for processing the entities. A number of additional
job lifecycle interfaces can be implemented if required to receive notifications when a job,
namespace, shard or slice starts and ends. Each job must be registered with the mapper
framework.

Initiate Job execution by POSTing to the jobs endpoint. This can be done manually or 
could be setup to be kicked off from a cron task. Your job Query function will
be passed the request to extract any parameters it requires to create the query object
for filtering the datastore entitites to process. e.g. you could pass the date range of
items to process, a cron task could default to processing the previous days entries.

Job execution will begin with the scheduling of a namespace iterator that will iterate
over all the namespaces specified in the query. Usually, this will be 'all' if none
have been defined but if only certain namespaces should be included they can be set.

For each namespace, the system will attempt to use the `__scatter__` index to split the
range of keys between shards, falling back to an ordinal splitting of property values if
the index does not exist.Depending on the number of shards configured and an estimate
of the minimum dataset size (based on the number of random keys returned) the system
will choose and appropriate number of shards and split the key range between them to
provide potential for parallelization.

Each namespace-shard will be schedule to execute and will iterate over it's range of
keys in slices to limit the impact of any failure and to cope with the request time
limits of the appengine runtime. Each slice will continue where the previous left off
by using a datastore cursor, thus eventally iterating over the entire dataset assigned
to that shard and, when all the shards complete, the entire dataset for the namespace.

Your job functions will be called at the appropriate points to perform whatever work
they need to.

## Output
For simple aggregation operations the inbuilt counter object can be used but be aware
that it is serialised and stored in the datastore entities of the mapper so the number
of entries should be limited. The counters are rolled up from slice, shard, namespace
and eventually to the job.

Although I have no plans to build in any shuffle or reduce steps, I wanted to provide
an easy way to write data to cloud storage (a primary use-case will be exporting data
in JSON format for BigQuery). Cloud Storage provides the ability to write each file in
chunks and then combine them which simplifies the file-writing operation substantially
(probably not available when the original libraries were written). Each slice can write
to it's own file, overwriting on retry, and those slices can then be quickly rolled up
into a single shard file and then into a single namespace file (eventually to a single job
file). This is working but needs to be optimized and made more robust (e.g. to cope with
the limits on how many files can be combined).

Another option is to stream inserts directly into BigQuery which saves the intermediate
output writing and subsequent import from cloud storage (once streamed inserting into
partitioned tables is an option, this will make this a better option). The lifecycle
functions can be used to check for and create the BigQuery datasets and tables as part
of the operation.

### Example
Here's an example of a mapper job outputting to cloud storage. The files written by each
slice are rolled up into a single shard file and are fairly evenly distributed (there
are around 55-60,000 JSON entries in each file):

![Shard Files Example](https://cloud.githubusercontent.com/assets/304910/16933919/1185e24c-4d0f-11e6-84dc-c6e10e07be46.png)

The shard files are then rolled up into a single file for the namespace ready for
importing into BigQuery (which could be automated on namespace or job completion):

![Namespace File Example](https://cloud.githubusercontent.com/assets/304910/16934010/b3658efa-4d0f-11e6-88bf-5f9463ad7f81.png)