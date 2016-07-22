package mapper

import (
	"fmt"

	"golang.org/x/net/context"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
)

const (
	jobURL               = "/job"
	jobCompleteURL       = jobURL + "/complete"
	iteratorURL          = "/iterate"
	iteratorCompleteURL  = iteratorURL + "/complete"
	namespaceURL         = "/namespace"
	namespaceCompleteURL = namespaceURL + "/complete"
	shardURL             = "/shard"
	shardCompleteURL     = shardURL + "/complete"
)

func init() {
	server.Handle(jobURL, server.handlerAdapter(server.jobHandler, jobFactory))
	server.Handle(jobCompleteURL, server.handlerAdapter(server.jobCompleteHandler, jobFactory))
	server.Handle(iteratorURL, server.handlerAdapter(server.iteratorHandler, iteratorFactory))
	server.Handle(iteratorCompleteURL, server.handlerAdapter(server.iteratorCompleteHandler, iteratorFactory))
	server.Handle(namespaceURL, server.handlerAdapter(server.namespaceHandler, namespaceFactory))
	server.Handle(namespaceCompleteURL, server.handlerAdapter(server.namespaceCompleteHandler, namespaceFactory))
	server.Handle(shardURL, server.handlerAdapter(server.shardHandler, shardFactory))
	server.Handle(shardCompleteURL, server.handlerAdapter(server.shardCompleteHandler, shardFactory))
}

func (m *mapper) jobHandler(c context.Context, config Config, key *datastore.Key, entity taskEntity) error {
	if config.LogVerbose {
		log.Infof(c, "jobHandler")
	}
	j, ok := entity.(*job)
	if !ok {
		return fmt.Errorf("expected job")
	}

	jobLifecycle, useJobLifecycle := j.jobSpec.(JobLifecycle)
	if useJobLifecycle && j.Sequence == 1 {
		jobLifecycle.JobStarted(c, j.id)
	}

	if err := j.start(c, m); err != nil {
		return err
	}

	return nil
}

func (m *mapper) jobCompleteHandler(c context.Context, config Config, key *datastore.Key, entity taskEntity) error {
	if config.LogVerbose {
		log.Infof(c, "jobHandler")
	}
	j, ok := entity.(*job)
	if !ok {
		return fmt.Errorf("expected job")
	}

	if err := j.completed(c, m, key); err != nil {
		return err
	}

	jobLifecycle, useJobLifecycle := j.jobSpec.(JobLifecycle)
	if useJobLifecycle {
		jobLifecycle.JobCompleted(c, j.id)
	}

	return nil
}

func (m *mapper) iteratorHandler(c context.Context, config Config, key *datastore.Key, entity taskEntity) error {
	if config.LogVerbose {
		log.Infof(c, "iteratorHandler")
	}
	it, ok := entity.(*iterator)
	if !ok {
		return fmt.Errorf("expected iterator")
	}

	completed, err := it.iterate(c, m)
	if err != nil {
		return err
	}

	// schedule continuation or completion
	var url string
	if completed {
		url = m.config.Path + iteratorCompleteURL
	} else {
		url = m.config.Path + iteratorURL
	}

	return m.locker.Schedule(c, key, it, url, nil)
}

func (m *mapper) iteratorCompleteHandler(c context.Context, config Config, key *datastore.Key, entity taskEntity) error {
	if config.LogVerbose {
		log.Infof(c, "iteratorCompleteHandler")
	}
	it, ok := entity.(*iterator)
	if !ok {
		return fmt.Errorf("expected iterator")
	}

	return it.completed(c, m, key)
}

func (m *mapper) namespaceHandler(c context.Context, config Config, key *datastore.Key, entity taskEntity) error {
	if config.LogVerbose {
		log.Infof(c, "namespaceHandler")
	}
	ns, ok := entity.(*namespace)
	if !ok {
		return fmt.Errorf("expected namespace")
	}

	namespaceLifecycle, useNamespaceLifecycle := ns.jobSpec.(NamespaceLifecycle)
	if useNamespaceLifecycle && ns.Sequence == 1 {
		namespaceLifecycle.NamespaceStarted(c, ns.jobID(), ns.Namespace)
	}

	err := ns.split(c, m)
	if err != nil {
		return err
	}

	// if there are no shards for this namespace, short-circuit 'complete'
	if ns.ShardsTotal == 0 {
		return ns.completed(c, m, key)
	} else {
		return ns.update(c, m, key)
	}
}

func (m *mapper) namespaceCompleteHandler(c context.Context, config Config, key *datastore.Key, entity taskEntity) error {
	if config.LogVerbose {
		log.Infof(c, "namespaceCompleteHandler")
	}
	ns, ok := entity.(*namespace)
	if !ok {
		return fmt.Errorf("expected namespace")
	}

	if err := ns.rollup(c); err != nil {
		return err
	}

	if err := ns.completed(c, m, key); err != nil {
		return err
	}

	namespaceLifecycle, useNamespaceLifecycle := ns.jobSpec.(NamespaceLifecycle)
	if useNamespaceLifecycle {
		namespaceLifecycle.NamespaceCompleted(c, ns.jobID(), ns.Namespace)
	}

	return nil
}

func (m *mapper) shardHandler(c context.Context, config Config, key *datastore.Key, entity taskEntity) error {
	if config.LogVerbose {
		log.Infof(c, "shardHandler")
	}
	s, ok := entity.(*shard)
	if !ok {
		return fmt.Errorf("expected shard")
	}

	shardLifecycle, useShardLifecycle := s.jobSpec.(ShardLifecycle)
	if useShardLifecycle && s.Sequence == 1 {
		shardLifecycle.ShardStarted(c, s.jobID(), s.Namespace, s.Shard)
	}

	sliceLifecycle, useSliceLifecycle := s.jobSpec.(SliceLifecycle)
	if useSliceLifecycle {
		sliceLifecycle.SliceStarted(c, s.jobID(), s.Namespace, s.Shard, s.Sequence)
	}

	completed, err := s.iterate(c, m)
	if err != nil {
		return err
	}

	if useSliceLifecycle {
		sliceLifecycle.SliceCompleted(c, s.jobID(), s.Namespace, s.Shard, s.Sequence)
	}

	// schedule continuation or completion
	var url string
	if completed {
		s.complete()
		url = m.config.Path + shardCompleteURL
	} else {
		url = m.config.Path + shardURL
	}
	return m.locker.Schedule(c, key, s, url, nil)
}

func (m *mapper) shardCompleteHandler(c context.Context, config Config, key *datastore.Key, entity taskEntity) error {
	if config.LogVerbose {
		log.Infof(c, "shardCompleteHandler")
	}
	s, ok := entity.(*shard)
	if !ok {
		return fmt.Errorf("expected shard")
	}

	if err := s.rollup(c); err != nil {
		return err
	}

	if err := s.completed(c, m, key); err != nil {
		return err
	}

	shardLifecycle, useShardLifecycle := s.jobSpec.(ShardLifecycle)
	if useShardLifecycle {
		shardLifecycle.ShardCompleted(c, s.jobID(), s.Namespace, s.Shard)
	}

	return nil
}
