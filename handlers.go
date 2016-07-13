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
	server.handleTask(jobURL, jobKind, server.jobHandler)
	server.handleTask(jobCompleteURL, jobKind, server.jobCompleteHandler)
	server.handleTask(iteratorURL, iteratorKind, server.iteratorHandler)
	server.handleTask(iteratorCompleteURL, iteratorKind, server.iteratorCompleteHandler)
	server.handleTask(namespaceURL, namespaceKind, server.namespaceHandler)
	server.handleTask(namespaceCompleteURL, namespaceKind, server.namespaceCompleteHandler)
	server.handleTask(shardURL, shardKind, server.shardHandler)
	server.handleTask(shardCompleteURL, shardKind, server.shardCompleteHandler)
}

func (m *mapper) jobHandler(c context.Context, config Config, key *datastore.Key, entity taskEntity) error {
	if config.LogVerbose {
		log.Infof(c, "jobHandler")
	}
	j, ok := entity.(*job)
	if !ok {
		return fmt.Errorf("expected job")
	}

	jobLifecycle, useJobLifecycle := j.Job.(JobLifecycle)
	if useJobLifecycle && j.Sequence == 1 {
		jobLifecycle.JobStarted(c, "id")
	}

	if err := j.start(c, *m.config); err != nil {
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

	if err := j.completed(c, *m.config, key); err != nil {
		return err
	}

	jobLifecycle, useJobLifecycle := j.Job.(JobLifecycle)
	if useJobLifecycle {
		jobLifecycle.JobCompleted(c, "id")
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

	completed, err := it.iterate(c, *m.config)
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

	return ScheduleLock(c, key, it, url, nil, it.queue)
}

func (m *mapper) iteratorCompleteHandler(c context.Context, config Config, key *datastore.Key, entity taskEntity) error {
	if config.LogVerbose {
		log.Infof(c, "iteratorCompleteHandler")
	}
	it, ok := entity.(*iterator)
	if !ok {
		return fmt.Errorf("expected iterator")
	}

	return it.completed(c, *m.config, key)
}

func (m *mapper) namespaceHandler(c context.Context, config Config, key *datastore.Key, entity taskEntity) error {
	if config.LogVerbose {
		log.Infof(c, "namespaceHandler")
	}
	ns, ok := entity.(*namespace)
	if !ok {
		return fmt.Errorf("expected namespace")
	}

	namespaceLifecycle, useNamespaceLifecycle := ns.job.Job.(NamespaceLifecycle)
	if useNamespaceLifecycle && ns.Sequence == 1 {
		namespaceLifecycle.NamespaceStarted(c, "id", "namespace")
	}

	err := ns.split(c, *m.config)
	if err != nil {
		return err
	}

	// if there are no shards for this namespace, short-circuit 'complete'
	if ns.ShardsTotal == 0 {
		return ns.completed(c, *m.config, key)
	} else {
		return ns.update(c, *m.config, key)
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

	if err := ns.completed(c, *m.config, key); err != nil {
		return err
	}

	namespaceLifecycle, useNamespaceLifecycle := ns.job.Job.(NamespaceLifecycle)
	if useNamespaceLifecycle {
		namespaceLifecycle.NamespaceCompleted(c, "id", "namespace")
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

	shardLifecycle, useShardLifecycle := s.job.Job.(ShardLifecycle)
	if useShardLifecycle && s.Sequence == 1 {
		shardLifecycle.ShardStarted(c, "id", "namespace", s.Shard)
	}

	sliceLifecycle, useSliceLifecycle := s.job.Job.(SliceLifecycle)
	if useSliceLifecycle {
		sliceLifecycle.SliceStarted(c, "id", "namespace", s.Shard, s.Sequence)
	}

	completed, err := s.iterate(c)
	if err != nil {
		return err
	}

	if useSliceLifecycle {
		sliceLifecycle.SliceCompleted(c, "id", "namespace", s.Shard, s.Sequence)
	}

	// schedule continuation or completion
	var url string
	if completed {
		s.complete()
		url = m.config.Path + shardCompleteURL
	} else {
		url = m.config.Path + shardURL
	}
	return ScheduleLock(c, key, s, url, nil, s.queue)
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

	if err := s.completed(c, *m.config, key); err != nil {
		return err
	}

	shardLifecycle, useShardLifecycle := s.job.Job.(ShardLifecycle)
	if useShardLifecycle {
		shardLifecycle.ShardCompleted(c, "id", "namespace", s.Shard)
	}

	return nil
}
