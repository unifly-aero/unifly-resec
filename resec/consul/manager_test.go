package consul

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/aireuropa/resec/resec/state"
	consulapi "github.com/hashicorp/consul/api"
	"github.com/jpillora/backoff"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

type ttlCall struct {
	checkID string
	output  string
	status  string
}

// mockClient implements consulClient for tests. Every call is recorded in
// calls (in order) and the fields below are protected by mu.
type mockClient struct {
	mu sync.Mutex

	calls []string

	kvPair *consulapi.KVPair
	kvErr  error
	kvOpts *consulapi.QueryOptions

	services   []*consulapi.ServiceEntry
	lastIndex  uint64
	serviceErr error

	registered   []*consulapi.AgentServiceRegistration
	registerErr  error
	checks       []*consulapi.AgentCheckRegistration
	checkErr     error
	deregistered []string
	deregErr     error
	catalogDereg [][2]string // node, serviceID
	catalogErr   error
	nodeName     string
	nodeNameErr  error
	ttlCalls     []ttlCall
	ttlErr       error
}

func (c *mockClient) record(name string) { c.calls = append(c.calls, name) }

func (c *mockClient) count(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, call := range c.calls {
		if call == name {
			n++
		}
	}
	return n
}

func (c *mockClient) LockOpts(opts *consulapi.LockOptions) (*consulapi.Lock, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.record("LockOpts")
	return nil, errors.New("not supported in tests")
}

func (c *mockClient) ServiceRegister(s *consulapi.AgentServiceRegistration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.record("ServiceRegister")
	c.registered = append(c.registered, s)
	return c.registerErr
}

func (c *mockClient) CheckRegister(check *consulapi.AgentCheckRegistration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.record("CheckRegister")
	c.checks = append(c.checks, check)
	return c.checkErr
}

func (c *mockClient) ServiceDeregister(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.record("ServiceDeregister")
	c.deregistered = append(c.deregistered, id)
	return c.deregErr
}

func (c *mockClient) CatalogDeregister(node, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.record("CatalogDeregister")
	c.catalogDereg = append(c.catalogDereg, [2]string{node, id})
	return c.catalogErr
}

func (c *mockClient) NodeName() (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.record("NodeName")
	return c.nodeName, c.nodeNameErr
}

func (c *mockClient) UpdateTTL(checkID, output, status string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.record("UpdateTTL")
	c.ttlCalls = append(c.ttlCalls, ttlCall{checkID, output, status})
	return c.ttlErr
}

func (c *mockClient) ServiceHealth(service, tag string, passingOnly bool, q *consulapi.QueryOptions) ([]*consulapi.ServiceEntry, *consulapi.QueryMeta, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.record("ServiceHealth")
	if c.serviceErr != nil {
		return nil, nil, c.serviceErr
	}
	return c.services, &consulapi.QueryMeta{LastIndex: c.lastIndex}, nil
}

func (c *mockClient) KVGet(key string, q *consulapi.QueryOptions) (*consulapi.KVPair, *consulapi.QueryMeta, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.record("KVGet")
	c.kvOpts = q
	return c.kvPair, nil, c.kvErr
}

func newTestManager(client consulClient) *Manager {
	return &Manager{
		backoff: &backoff.Backoff{},
		client:  client,
		config: &config{
			lockKey:                "resec/.lock",
			announceAddr:           ":6379",
			announcePort:           6379,
			serviceNamePrefix:      "redis",
			serviceTTL:             6 * time.Second,
			deregisterServiceAfter: 72 * time.Hour,
			serviceTagsByRole: map[string][]string{
				"master": {},
				"slave":  {},
			},
		},
		lockCh:  make(chan struct{}),
		logger:  log.WithField("testing", true),
		stateCh: make(chan state.Consul, 10),
		stopCh:  make(chan interface{}, 1),
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

func TestIsConsulDown_Extra(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"wrapped OpError", fmt.Errorf("wrap: %w", &net.OpError{Op: "dial", Err: errors.New("boom")}), true},
		{"DNSError", &net.DNSError{Err: "no such host", Name: "consul"}, true},
		{"rpc error", errors.New("rpc error making call: EOF-less failure"), true},
		{"502", errors.New("Unexpected response code: 502"), true},
		{"503", errors.New("Unexpected response code: 503"), true},
		{"reset", errors.New("read: connection reset by peer"), true},
		{"EOF", errors.New("EOF"), true},
		{"deadline", errors.New("context deadline exceeded"), true},
		{"404", errors.New("Unexpected response code: 404"), false},
		{"400", errors.New("Unexpected response code: 400 (Bad request)"), false},
		{"no TTL", errors.New("CheckID does not have associated TTL"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isConsulDown(tt.err))
		})
	}
}

var errNoLeader = errors.New("Unexpected response code: 500 (No cluster leader)")

func TestHandleConsulError_NonTransportKeepsHealthy(t *testing.T) {
	m := newTestManager(&mockClient{})

	m.handleConsulError(errors.New("Unexpected response code: 403 (Permission denied)"))

	_, n := drain(m.stateCh)
	assert.Equal(t, 0, n)
	s := m.snapshot()
	assert.True(t, s.Healthy)
	assert.Equal(t, "10.0.0.1", s.MasterAddr)
}

func TestHandleConsulError_AlreadyUnhealthyNoReemit(t *testing.T) {
	m := newTestManager(&mockClient{})
	m.state.Healthy = false

	m.handleConsulError(errNoLeader)

	_, n := drain(m.stateCh)
	assert.Equal(t, 0, n)
}

func TestHandleConsulError_ClearsMultiMaster(t *testing.T) {
	m := newTestManager(&mockClient{})
	m.state.MasterServices = []state.ConsulService{{ServiceID: "a"}, {ServiceID: "b"}}

	m.handleConsulError(errNoLeader)

	s, n := drain(m.stateCh)
	assert.Equal(t, 1, n)
	assert.Nil(t, s.MasterServices)
	assert.False(t, s.Healthy)
}

func entry(node, nodeAddr, svcAddr string, port int, id string) *consulapi.ServiceEntry {
	return &consulapi.ServiceEntry{
		Node:    &consulapi.Node{Node: node, Address: nodeAddr},
		Service: &consulapi.AgentService{ID: id, Address: svcAddr, Port: port},
	}
}

func TestPollMasterService_NoServices(t *testing.T) {
	m := newTestManager(&mockClient{services: []*consulapi.ServiceEntry{}, lastIndex: 5})
	q := &consulapi.QueryOptions{WaitIndex: 1}

	assert.NoError(t, m.pollMasterService("redis-master", "", q))

	_, n := drain(m.stateCh)
	assert.Equal(t, 0, n)
	assert.Equal(t, uint64(5), q.WaitIndex)
}

func TestPollMasterService_Single(t *testing.T) {
	tests := []struct {
		name    string
		svcAddr string
		want    string
	}{
		{"service address", "10.0.0.2", "10.0.0.2"},
		{"port only falls back to node address", "", "10.0.0.9"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockClient{
				lastIndex: 5,
				services:  []*consulapi.ServiceEntry{entry("n1", "10.0.0.9", tt.svcAddr, 6379, "redis@x")},
			}
			m := newTestManager(client)
			m.state.MasterServices = []state.ConsulService{{}, {}}
			q := &consulapi.QueryOptions{WaitIndex: 1}

			assert.NoError(t, m.pollMasterService("redis-master", "", q))

			s, n := drain(m.stateCh)
			assert.Equal(t, 1, n)
			assert.Equal(t, tt.want, s.MasterAddr)
			assert.Equal(t, 6379, s.MasterPort)
			assert.Nil(t, s.MasterServices)
		})
	}
}

func TestPollMasterService_UnchangedIndexAndAddress(t *testing.T) {
	client := &mockClient{
		lastIndex: 5,
		services:  []*consulapi.ServiceEntry{entry("n1", "10.0.0.9", "10.0.0.2", 6379, "a")},
	}
	m := newTestManager(client)
	q := &consulapi.QueryOptions{WaitIndex: 1}

	assert.NoError(t, m.pollMasterService("redis-master", "", q))
	_, n := drain(m.stateCh)
	assert.Equal(t, 1, n)

	assert.NoError(t, m.pollMasterService("redis-master", "", q))
	_, n = drain(m.stateCh)
	assert.Equal(t, 0, n)
}

func TestPollMasterService_TwoServices(t *testing.T) {
	client := &mockClient{
		lastIndex: 5,
		services: []*consulapi.ServiceEntry{
			entry("node1", "10.0.0.8", "10.0.0.1", 6379, "redis@10.0.0.1:6379"),
			entry("node2", "10.0.0.9", "", 6380, "redis@:6380"),
		},
	}
	m := newTestManager(client)
	q := &consulapi.QueryOptions{WaitIndex: 1}

	assert.NoError(t, m.pollMasterService("redis-master", "", q))

	s, n := drain(m.stateCh)
	assert.Equal(t, 1, n)
	assert.Equal(t, []state.ConsulService{
		{Address: "10.0.0.1", Port: 6379, ServiceID: "redis@10.0.0.1:6379", Node: "node1"},
		{Address: "10.0.0.9", Port: 6380, ServiceID: "redis@:6380", Node: "node2"},
	}, s.MasterServices)
	assert.Equal(t, "10.0.0.1", s.MasterAddr, "MasterAddr unchanged")
	assert.Equal(t, 6379, s.MasterPort)
}

func TestPollMasterService_MultiToSingle(t *testing.T) {
	client := &mockClient{
		lastIndex: 5,
		services: []*consulapi.ServiceEntry{
			entry("node1", "10.0.0.8", "10.0.0.1", 6379, "a"),
			entry("node2", "10.0.0.9", "10.0.0.2", 6379, "b"),
		},
	}
	m := newTestManager(client)
	q := &consulapi.QueryOptions{WaitIndex: 1}
	assert.NoError(t, m.pollMasterService("redis-master", "", q))
	drain(m.stateCh)

	// Apply the emitted state like the reconciler would see it.
	client.lastIndex = 6
	client.services = []*consulapi.ServiceEntry{entry("node2", "10.0.0.9", "10.0.0.2", 6379, "b")}
	assert.NoError(t, m.pollMasterService("redis-master", "", q))

	s, n := drain(m.stateCh)
	assert.Equal(t, 1, n)
	assert.Nil(t, s.MasterServices)
	assert.Equal(t, "10.0.0.2", s.MasterAddr)
}

func TestPollMasterService_TransportError(t *testing.T) {
	m := newTestManager(&mockClient{serviceErr: errors.New("dial tcp: connection refused")})
	q := &consulapi.QueryOptions{WaitIndex: 7}

	assert.Error(t, m.pollMasterService("redis-master", "", q))

	assert.Equal(t, uint64(1), q.WaitIndex)
	assert.False(t, m.snapshot().Healthy)
}

func TestPollMasterService_NonTransportError(t *testing.T) {
	m := newTestManager(&mockClient{serviceErr: errors.New("Unexpected response code: 403 (Permission denied)")})
	q := &consulapi.QueryOptions{WaitIndex: 7}

	assert.Error(t, m.pollMasterService("redis-master", "", q))

	assert.Equal(t, uint64(1), q.WaitIndex)
	assert.True(t, m.snapshot().Healthy)
}

func TestManager_GetMasterServices(t *testing.T) {
	m := newTestManager(&mockClient{})
	got := m.GetMasterServices([]*consulapi.ServiceEntry{
		entry("n1", "10.0.0.8", "10.0.0.1", 1, "a"),
		entry("n2", "10.0.0.9", "", 2, "b"),
	})
	assert.Equal(t, []state.ConsulService{
		{Address: "10.0.0.1", Port: 1, ServiceID: "a", Node: "n1"},
		{Address: "10.0.0.9", Port: 2, ServiceID: "b", Node: "n2"},
	}, got)
}

func TestManager_cleanup(t *testing.T) {
	client := &mockClient{}
	m := newTestManager(client)
	m.state.Master = true
	m.config.voluntarilyReleaseLockCh = make(chan interface{})
	releaseCh := m.config.voluntarilyReleaseLockCh
	m.config.serviceID = "redis@:6379"

	m.cleanup()

	select {
	case <-releaseCh:
	default:
		t.Error("release channel not closed")
	}
	assert.Equal(t, []string{"redis@:6379"}, client.deregistered)
	select {
	case <-m.lockCh:
	default:
		t.Error("lockCh not closed")
	}
	select {
	case _, ok := <-m.stopCh:
		assert.False(t, ok)
	default:
		t.Error("stopCh not closed")
	}
	s, _ := drain(m.stateCh)
	assert.True(t, s.Stopped)
	assert.NotPanics(t, func() { m.lockChOnce.Do(func() { close(m.lockCh) }) })
}

func TestManager_start(t *testing.T) {
	m := newTestManager(&mockClient{})
	defer close(m.stopCh)

	m.start()

	select {
	case s := <-m.stateCh:
		assert.True(t, s.Ready)
	case <-time.After(time.Second):
		t.Fatal("no state emitted")
	}
	// The mock LockOpts returns an error, so the lock is never held.
	time.Sleep(50 * time.Millisecond)
	assert.False(t, m.snapshot().Master)
}

func TestManager_recoveryCheck_Healthy(t *testing.T) {
	client := &mockClient{kvPair: &consulapi.KVPair{}}
	m := newTestManager(client)

	m.recoveryCheck()

	assert.Equal(t, 0, client.count("KVGet"))
}

func TestManager_recoveryCheck_Recovers(t *testing.T) {
	client := &mockClient{kvPair: &consulapi.KVPair{Key: "resec/.lock"}}
	m := newTestManager(client)
	m.state.Healthy = false

	m.recoveryCheck()

	assert.Equal(t, 1, client.count("KVGet"))
	assert.True(t, m.snapshot().Healthy)
}

func TestManager_recoveryCheck_StaysUnhealthy(t *testing.T) {
	client := &mockClient{kvErr: errNoLeader}
	m := newTestManager(client)
	m.state.Healthy = false

	m.recoveryCheck()

	assert.Equal(t, 1, client.count("KVGet"))
	assert.False(t, m.snapshot().Healthy)
}

func TestManager_watchConsulRecovery_StopsOnStopCh(t *testing.T) {
	m := newTestManager(&mockClient{})
	done := make(chan struct{})
	go func() {
		m.watchConsulRecovery()
		close(done)
	}()
	close(m.stopCh)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watchConsulRecovery did not return")
	}
}

func TestManager_copyState_NoSharedMemory(t *testing.T) {
	m := newTestManager(&mockClient{})
	m.state.MasterServices = []state.ConsulService{{ServiceID: "a"}, {ServiceID: "b"}}

	m.mu.Lock()
	cp := m.copyState()
	m.mu.Unlock()
	cp.MasterServices[0].ServiceID = "changed"
	assert.Equal(t, "a", m.state.MasterServices[0].ServiceID)

	snap := m.snapshot()
	snap.MasterServices[1].ServiceID = "changed"
	assert.Equal(t, "b", m.state.MasterServices[1].ServiceID)

	// nil stays nil
	m.state.MasterServices = nil
	assert.Nil(t, m.snapshot().MasterServices)
}

func TestManager_Concurrency(t *testing.T) {
	client := &mockClient{
		lastIndex: 5,
		services:  []*consulapi.ServiceEntry{entry("n1", "10.0.0.9", "10.0.0.2", 6379, "a")},
	}
	m := newTestManager(client)

	stop := make(chan struct{})
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for {
			select {
			case <-m.stateCh:
			case <-stop:
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for g := 0; g < 20; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			q := &consulapi.QueryOptions{WaitIndex: 1}
			for i := 0; i < 100; i++ {
				switch (g + i) % 4 {
				case 0:
					m.handleConsulError(errNoLeader)
				case 1:
					m.markConsulHealthy()
				case 2:
					_ = m.snapshot()
				case 3:
					q.WaitIndex = uint64(i % 3)
					_ = m.pollMasterService("redis-master", "", q)
				}
			}
		}(g)
	}
	wg.Wait()
	close(stop)
	<-drained
}

func TestManager_Close(t *testing.T) {
	m := newTestManager(&mockClient{})

	m.Close()
	assert.NotPanics(t, m.Close)
	assert.NotPanics(t, m.cleanup) // cleanup after Close must not close twice

	select {
	case <-m.lockCh:
	default:
		t.Error("lockCh not closed")
	}
	select {
	case _, ok := <-m.stopCh:
		assert.False(t, ok)
	default:
		t.Error("stopCh not closed")
	}
}
