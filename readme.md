# Datastore Mapper
This is an implementation of the mapper part of the
[appengine map-reduce framework](https://github.com/GoogleCloudPlatform/appengine-mapreduce)
developed using in [Go](https://golang.org/).

It is alpha status while I develop the API and should not be used in production.

## Why only the mapper?
Technology moves on and Google's cloud platform now provides other services that handle
many of the things that the Map-Reduce framework was once the solution to. If you need 
any Analytics and Reporting functionality, for instamce, you would now likely look at 
using [BigQuery](https://cloud.google.com/bigquery/) instead of trying to create your
own via Map-Reduce.

But the mapper is still needed: it's used to write backups of your Datastore data to 
Cloud storage (which is also the way you export your Datastore data into BigQuery). Many
times it's necessary to iterate over all the data in your system when you need to update
the schema or perform some other operational processing and the mapper provides a great
approach for doing that.

## Why a Go version?
So why not just continue to use the existing Python or Java implementations?

They both continue to work as well as they always have but there were a few specific
reasons that I wanted a pure Go-based solution:

### Performance
The Python version tends to be rather slow and can only process a small number of
entities in a batch without the instance memory going over the limit and causing errors
and retries. The Java runtime is faster but also requires larger instances sizes just
to execute before you even start processing anything.

I like that a Go version can run (and run _fast_) on the smallest F1/B1 micro-instance
consuming just 10Mb of RAM but able to process thousands of entities per second.

### Runtime Cost
The slow performance and larger instance size requirementss for Python and Java both 
result in higher cost which I'd prefer to avoid if possible. Each platform also tends
to have it's own memcache implementation to improve Datastore performance and bypassing
the already populated cache can also add to the cost due to unecessary datastore
operations. 

### Development Cost
I already have my datastore models developed as part of my Go app and don't want to have
to re-develop and maintain a Python or Java version as well (and sometimes it's not 
straightforward to switch between them). The cost of including a whole other language
in your solution goes beyond just the code - it's the development machine maintenance
and upgrading, IDE tooling and skills.

## Performance
Initial testing shows it can process over 100k datastore entities in about a minute
using a single F1 micro instance. This works out to about 1.6k entities per second.

Of course the work can often be divided into smaller pieces so it's very likely that
increasing the number of shards so that requests are run on multiple instances will
result in much higher performance / faster throughput.

## Implementation
I originally started with the idea of strictly following the existing implementations
down to the format of the entities for controlling the tasks so that the UI could
be re-used and the Go version could be a drop-in replacement but this would have also
needed the pipline library to be implemented and while I got quite a long way with it,
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
completey destroys the benefit of sharding and distribution.

Instead, this framework creates a producer task to iterate over the namespaces and do
the shard splitting on each of them, assigning separate shards within each namespace
to ensure the work can be split evenly but with some heuristics so that the number of
shards used for a namespace that doesn't contain much data is automatically reduced.

So, instead of the number of shards being fixed, it becomes more of a 'minimum target'
for splitting the data as long as it makes sense to do so. Ultimately, we want the
work split into sensible but meaninful chunks so that it can be parallelized and it
doesn't matter if there are 8 shards with 10 slices or 800 shards with 1 slice, the
task queue processing rate and concurrency limits are what manage how fast it will
all be scheduled.

With each namespace-shard we still use the same slice approach to process the range
of data allocated to it and allow for the app-engine request timeout limitations and
retry / restart ability with some simplified locking / leasing to prevent duplicate
task execution. 

## Workflow
Here is the basic execution workflow:

Define a Job (can be a simple struct) that implements the basic Job interface used to
define the Query, either the Single or Batch interface for processing the entitied and
additional job lifecycle interfaces if required to receive notifications when a job,
shard or slice starts and ends. Each job must be registered with the mapper framework.

Initiate Job execution by POSTing to the jobs endpoint. Your job Query function will
be passed the request to extract any parameters it requires to create the query object
for filtering the datastore entitites to process.

Job execution will begin with the scheduling of a namespace iterator that will iterate
over all the namespaces specified in the query. Usually, this will be 'all' if none
have been defined but if only certain namespaces should be processed they can be set.

For each namespace, the system will attempt to use the `__scatter__` index to split the
range of keys between shards, falling back to an ordinal splitting of property values if
the index does not exist. Depending on the number of shards configured and an estimate
of the minimum dataset size (based on the number of random keys returned) the system
will choose and appropriate number of shards and split the key range between them to
provide potential for parallelization.

Each namespace-shard will be schedule to execute and will iterate over it's range of
keys in slices to limit the impact of any failure and to cope with the request time
limits of the appengine runtime. Each slice will continue where the previous left off
by using a datastore cursor, eventally iterating over the entire dataset.

Your job functions will be called at the appropriate points to perform whatever work
they need to.

## Future Plans
The basic mapper processing works and needs some tidy up / testing for robustness which
I'll be doing by using it within a real-world app with over 6 million entities to work
against.

Although I have no plans to build in any shuffle or reduce steps, I do want to provide
an easy way to write data to cloud storage (a primary use-case will be exporting data
in JSON format for BigQuery). Cloud Storage provides the sbility to write each file in
chunks and then combine them which I think will simplyfy the file-writing operation
substantially (probably not available when the original libraries were written). Each
slice can write to it's own file and those slices then rolled up into a single shard 
file and possibly, the shard files then rolled up into a single job file.
