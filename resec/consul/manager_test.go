package consul

import (
	"context"
	"errors"
	"testing"

	"github.com/aireuropa/resec/resec/state"
	consulapi "github.com/hashicorp/consul/api"
	"github.com/jpillora/backoff"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// mockClient implements consulClient for tests
type mockClient struct {
	consulClient
	kvPair *consulapi.KVPair
	kvErr  error
	kvOpts *consulapi.QueryOptions

	services  []*consulapi.ServiceEntry
	lastIndex uint64
}

func (c *mockClient) ServiceHealth(service, tag string, passingOnly bool, q *consulapi.QueryOptions) ([]*consulapi.ServiceEntry, *consulapi.QueryMeta, error) {
	return c.services, &consulapi.QueryMeta{LastIndex: c.lastIndex}, nil
}

func (c *mockClient) KVGet(key string, q *consulapi.QueryOptions) (*consulapi.KVPair, *consulapi.QueryMeta, error) {
	c.kvOpts = q
	return c.kvPair, nil, c.kvErr
}

func newTestManager(client consulClient) *Manager {
	return &Manager{
		backoff: &backoff.Backoff{},
		client:  client,
		config:  &config{lockKey: "resec/.lock"},
		logger:  log.WithField("testing", true),
		stateCh: make(chan state.Consul, 10),
		state:   &state.Consul{Healthy: true, MasterAddr: "10.0.0.1", MasterPort: 6379},
	}
}

func TestIsConsulDown(t *testing.T) {
	assert.False(t, isConsulDown(nil))
	assert.True(t, isConsulDown(errors.New("Unexpected response code: 500 (No cluster leader)")))
	assert.True(t, isConsulDown(errors.New("Get http://127.0.0.1:8500: dial tcp 127.0.0.1:8500: connect: connection refused")))
	assert.False(t, isConsulDown(errors.New("Unexpected response code: 403 (Permission denied)")))
}

func TestHandleConsulError_NoLeader(t *testing.T) {
	m := newTestManager(&mockClient{})

	m.handleConsulError(errors.New("Unexpected response code: 500 (No cluster leader)"))
	s := <-m.stateCh
	assert.False(t, s.Healthy)
	assert.Equal(t, "", s.MasterAddr)
	assert.Equal(t, 0, s.MasterPort)

	// agent-local calls succeed without a leader, they must not mark Consul healthy
	m.handleConsulError(nil)
	_, n := drain(m.stateCh)
	assert.Equal(t, 0, n)
	assert.False(t, m.snapshot().Healthy)
}

func TestMarkConsulHealthy(t *testing.T) {
	m := newTestManager(&mockClient{})
	m.state.Healthy = false

	m.markConsulHealthy()
	s, n := drain(m.stateCh)
	assert.Equal(t, 1, n)
	assert.True(t, s.Healthy)

	// already healthy, no new state
	m.markConsulHealthy()
	_, n = drain(m.stateCh)
	assert.Equal(t, 0, n)
}

func TestLockKeyHealthy_MarksHealthy(t *testing.T) {
	client := &mockClient{kvPair: &consulapi.KVPair{Key: "resec/.lock"}}
	m := newTestManager(client)
	m.state.Healthy = false

	assert.NoError(t, m.LockKeyHealthy(context.Background()))
	assert.True(t, m.snapshot().Healthy)
}

func TestLockKeyHealthy(t *testing.T) {
	client := &mockClient{kvPair: &consulapi.KVPair{Key: "resec/.lock"}}
	m := newTestManager(client)

	assert.NoError(t, m.LockKeyHealthy(context.Background()))
	assert.True(t, client.kvOpts.RequireConsistent)

	client.kvPair = nil
	assert.Error(t, m.LockKeyHealthy(context.Background()))

	client.kvErr = errors.New("Unexpected response code: 500 (No cluster leader)")
	assert.Error(t, m.LockKeyHealthy(context.Background()))
}

// drain returns the last state on the channel
func drain(ch chan state.Consul) (last state.Consul, n int) {
	for {
		select {
		case s := <-ch:
			last, n = s, n+1
		default:
			return last, n
		}
	}
}

func TestPollMasterService_RecoversAfterConsulError(t *testing.T) {
	client := &mockClient{
		lastIndex: 42,
		services: []*consulapi.ServiceEntry{{
			Node:    &consulapi.Node{Node: "node1", Address: "10.0.0.2"},
			Service: &consulapi.AgentService{ID: "redis@:6379", Port: 6380},
		}},
	}
	m := newTestManager(client)
	q := &consulapi.QueryOptions{WaitIndex: 1}

	assert.NoError(t, m.pollMasterService("redis-master", "", q))
	s, _ := drain(m.stateCh)
	assert.Equal(t, "10.0.0.2", s.MasterAddr)

	// another go-routine sees an error and clears the master address
	m.handleConsulError(errors.New("Unexpected response code: 500 (No cluster leader)"))
	s, _ = drain(m.stateCh)
	assert.Equal(t, "", s.MasterAddr)

	// same LastIndex, the watch must still emit the master address
	assert.NoError(t, m.pollMasterService("redis-master", "", q))
	s, n := drain(m.stateCh)
	assert.True(t, n > 0)
	// the catalog read can come from the agent cache, it must not mark Consul healthy
	assert.False(t, s.Healthy)
	assert.Equal(t, "10.0.0.2", s.MasterAddr)
	assert.Equal(t, 6380, s.MasterPort)
}
