package consul

import (
	"errors"
	"testing"

	"github.com/aireuropa/resec/resec/state"
	"github.com/stretchr/testify/assert"
)

func TestManager_ServiceNamesAndIDs(t *testing.T) {
	tests := []struct {
		name     string
		svcName  string
		master   bool
		wantName string
		wantID   string
	}{
		{"prefix slave", "", false, "redis-slave", "redis"},
		{"prefix master", "", true, "redis-master", "redis"},
		{"named", "cache", false, "cache", "cache"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newTestManager(&mockClient{})
			m.config.serviceName = tt.svcName
			m.state.Master = tt.master
			assert.Equal(t, tt.wantName, m.getConsulServiceName())
			assert.Equal(t, tt.wantID, m.getConsulServiceID())
		})
	}
}

func TestManager_getReplicationStatus(t *testing.T) {
	m := newTestManager(&mockClient{})
	m.state.Master = true
	assert.Equal(t, "master", m.getReplicationStatus())
	m.state.Master = false
	assert.Equal(t, "slave", m.getReplicationStatus())
}

func TestManager_getConsulMasterDetails(t *testing.T) {
	m := newTestManager(&mockClient{})
	name, tag := m.getConsulMasterDetails()
	assert.Equal(t, "redis-master", name)
	assert.Equal(t, "", tag)

	m.config.serviceName = "cache"
	m.config.serviceTagsByRole["master"] = []string{"master", "x"}
	name, tag = m.getConsulMasterDetails()
	assert.Equal(t, "cache", name)
	assert.Equal(t, "master", tag)
}

func TestManager_registerService_WithHost(t *testing.T) {
	client := &mockClient{}
	m := newTestManager(client)
	m.config.announceAddr = "10.0.0.5:6379"
	m.config.announceHost = "10.0.0.5"
	m.config.announcePort = 6379
	m.config.serviceTagsByRole["slave"] = []string{"s1"}

	m.registerService(state.Redis{})

	if assert.Len(t, client.registered, 1) && assert.Len(t, client.checks, 1) {
		r := client.registered[0]
		assert.Equal(t, "redis@10.0.0.5:6379", r.ID)
		assert.Equal(t, "10.0.0.5", r.Address)
		assert.Equal(t, 6379, r.Port)
		assert.Equal(t, "redis-slave", r.Name)
		assert.Equal(t, []string{"s1"}, r.Tags)

		c := client.checks[0]
		assert.Equal(t, "redis@10.0.0.5:6379:replication-status-check", c.ID)
		assert.Equal(t, r.ID, c.ServiceID)
		assert.Equal(t, m.config.serviceTTL.String(), c.TTL)
		assert.Equal(t, "warning", c.Status)
		assert.Equal(t, m.config.deregisterServiceAfter.String(), c.DeregisterCriticalServiceAfter)
	}
	assert.Equal(t, "redis@10.0.0.5:6379", m.config.serviceID)
	assert.Equal(t, "redis@10.0.0.5:6379:replication-status-check", m.config.checkID)
}

func TestManager_registerService_PortOnly(t *testing.T) {
	client := &mockClient{}
	m := newTestManager(client)
	m.config.announceAddr = ":6379"
	m.config.announceHost = ""

	m.registerService(state.Redis{})

	if assert.Len(t, client.registered, 1) {
		assert.Equal(t, "", client.registered[0].Address)
		assert.Equal(t, "redis@:6379", client.registered[0].ID)
	}
}

func TestManager_registerService_MasterTags(t *testing.T) {
	client := &mockClient{}
	m := newTestManager(client)
	m.state.Master = true
	m.config.serviceTagsByRole["master"] = []string{"master", "v2"}

	m.registerService(state.Redis{})

	if assert.Len(t, client.registered, 1) {
		assert.Equal(t, []string{"master", "v2"}, client.registered[0].Tags)
		assert.Equal(t, "redis-master", client.registered[0].Name)
	}
}

func TestManager_registerService_RegisterFails(t *testing.T) {
	client := &mockClient{registerErr: errNoLeader}
	m := newTestManager(client)

	m.registerService(state.Redis{})

	assert.Empty(t, client.checks)
	assert.False(t, m.snapshot().Healthy)
	assert.Empty(t, m.config.serviceID)
	assert.Empty(t, m.config.checkID)
}

func TestManager_registerService_CheckFails(t *testing.T) {
	client := &mockClient{checkErr: errNoLeader}
	m := newTestManager(client)

	m.registerService(state.Redis{})

	assert.Len(t, client.checks, 1)
	assert.False(t, m.snapshot().Healthy)
	assert.Equal(t, []string{"redis@:6379"}, client.deregistered)
	assert.Empty(t, m.config.serviceID)
	assert.Empty(t, m.config.checkID)
}

func TestManager_deregisterService(t *testing.T) {
	tests := []struct {
		name      string
		healthy   bool
		serviceID string
		deregErr  error
		wantCalls int
		wantHealt bool
	}{
		{"healthy with ID", true, "redis@:6379", nil, 1, true},
		{"unhealthy", false, "redis@:6379", nil, 0, false},
		{"no ID", true, "", nil, 0, true},
		{"error", true, "redis@:6379", errNoLeader, 1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &mockClient{deregErr: tt.deregErr}
			m := newTestManager(client)
			m.state.Healthy = tt.healthy
			m.config.serviceID = tt.serviceID
			m.config.checkID = tt.serviceID + ":check"

			m.deregisterService()

			assert.Equal(t, tt.wantCalls, client.count("ServiceDeregister"))
			if tt.wantCalls == 1 {
				assert.Equal(t, []string{"redis@:6379"}, client.deregistered)
			}
			assert.Equal(t, "", m.config.serviceID)
			assert.Equal(t, "", m.config.checkID)
			assert.Equal(t, tt.wantHealt, m.snapshot().Healthy)
		})
	}
}

func TestManager_setConsulCheckStatus_RegistersFirst(t *testing.T) {
	client := &mockClient{}
	m := newTestManager(client)

	m.setConsulCheckStatus(state.Redis{InfoString: "info"})

	assert.Equal(t, []string{"ServiceRegister", "CheckRegister", "UpdateTTL"}, client.calls)
	assert.Equal(t, []ttlCall{{"redis@:6379:replication-status-check", "info", "passing"}}, client.ttlCalls)
}

func TestManager_setConsulCheckStatus_NormalUpdate(t *testing.T) {
	client := &mockClient{}
	m := newTestManager(client)
	m.config.checkID = "chk"

	m.setConsulCheckStatus(state.Redis{InfoString: "role:master"})

	assert.Equal(t, []string{"UpdateTTL"}, client.calls)
	assert.Equal(t, []ttlCall{{"chk", "role:master", "passing"}}, client.ttlCalls)
}

func TestManager_setConsulCheckStatus_TTLGone(t *testing.T) {
	client := &mockClient{ttlErr: errors.New("Unexpected response code: 500 (CheckID \"chk\" does not have associated TTL)")}
	m := newTestManager(client)
	m.config.checkID = "chk"
	m.config.serviceID = "redis@:6379"

	m.setConsulCheckStatus(state.Redis{})

	assert.Equal(t, 1, client.count("ServiceDeregister"))
	assert.Equal(t, "", m.config.serviceID)
	assert.Equal(t, "", m.config.checkID)
	assert.True(t, m.snapshot().Healthy)
}

func TestManager_setConsulCheckStatus_TransportError(t *testing.T) {
	client := &mockClient{ttlErr: errors.New("dial tcp: connection refused")}
	m := newTestManager(client)
	m.config.checkID = "chk"
	m.config.serviceID = "redis@:6379"

	m.setConsulCheckStatus(state.Redis{})

	assert.False(t, m.snapshot().Healthy)
	assert.Equal(t, "chk", m.config.checkID)
	assert.Equal(t, "redis@:6379", m.config.serviceID)
}

func TestManager_localNodeName(t *testing.T) {
	t.Run("first and second call", func(t *testing.T) {
		client := &mockClient{nodeName: "n1"}
		m := newTestManager(client)

		name, err := m.localNodeName()
		assert.NoError(t, err)
		assert.Equal(t, "n1", name)
		assert.Equal(t, "n1", m.config.nodeName)
		assert.Equal(t, 1, client.count("NodeName"))

		name, err = m.localNodeName()
		assert.NoError(t, err)
		assert.Equal(t, "n1", name)
		assert.Equal(t, 1, client.count("NodeName"))
	})

	t.Run("error", func(t *testing.T) {
		client := &mockClient{nodeNameErr: errors.New("boom")}
		m := newTestManager(client)

		_, err := m.localNodeName()
		assert.Error(t, err)
		assert.Equal(t, "", m.config.nodeName)
	})

	t.Run("preset", func(t *testing.T) {
		client := &mockClient{}
		m := newTestManager(client)
		m.config.nodeName = "n1"

		name, err := m.localNodeName()
		assert.NoError(t, err)
		assert.Equal(t, "n1", name)
		assert.Equal(t, 0, client.count("NodeName"))
	})
}

func multiMasterManager(client *mockClient) *Manager {
	m := newTestManager(client)
	m.state.MasterServices = []state.ConsulService{
		{Node: "node1", ServiceID: "redis@10.0.0.1:6379"},
		{Node: "node2", ServiceID: "redis@10.0.0.2:6379"},
	}
	return m
}

func TestManager_deregisterMultiMasterService_NotLockHolder(t *testing.T) {
	client := &mockClient{}
	m := multiMasterManager(client)
	m.state.Master = false
	m.config.serviceID = "redis@10.0.0.2:6379"

	m.deregisterMultiMasterService(state.Redis{})

	assert.Equal(t, []string{"redis@10.0.0.2:6379"}, client.deregistered)
	assert.Empty(t, client.catalogDereg)
}

func TestManager_deregisterMultiMasterService_LockHolder(t *testing.T) {
	client := &mockClient{}
	m := multiMasterManager(client)
	m.state.Master = true
	m.config.nodeName = "node1"
	m.config.serviceID = "redis@10.0.0.1:6379"

	m.deregisterMultiMasterService(state.Redis{})

	assert.Equal(t, [][2]string{{"node2", "redis@10.0.0.2:6379"}}, client.catalogDereg)
	assert.Empty(t, client.deregistered, "own service kept")
}

func TestManager_deregisterMultiMasterService_NodeNameError(t *testing.T) {
	client := &mockClient{nodeNameErr: errNoLeader}
	m := multiMasterManager(client)
	m.state.Master = true

	m.deregisterMultiMasterService(state.Redis{})

	assert.Empty(t, client.catalogDereg)
	assert.False(t, m.snapshot().Healthy)
}

func TestManager_deregisterMultiMasterService_SameNodeOtherID(t *testing.T) {
	client := &mockClient{}
	m := newTestManager(client)
	m.state.MasterServices = []state.ConsulService{
		{Node: "node1", ServiceID: "redis@10.0.0.1:6379"},
		{Node: "node1", ServiceID: "redis@10.0.0.1:6380"},
	}
	m.state.Master = true
	m.config.nodeName = "node1"
	m.config.serviceID = "redis@10.0.0.1:6379"

	m.deregisterMultiMasterService(state.Redis{})

	assert.Equal(t, [][2]string{{"node1", "redis@10.0.0.1:6380"}}, client.catalogDereg)
}

func TestManager_deregisterMultiMasterService_CatalogError(t *testing.T) {
	client := &mockClient{catalogErr: errors.New("Unexpected response code: 403 (Permission denied)")}
	m := multiMasterManager(client)
	m.state.Master = true
	m.config.nodeName = "node1"
	m.config.serviceID = "redis@10.0.0.1:6379"

	m.deregisterMultiMasterService(state.Redis{})

	assert.Len(t, client.catalogDereg, 1)
	assert.True(t, m.snapshot().Healthy)
}

func TestManager_releaseConsulLock(t *testing.T) {
	t.Run("not master", func(t *testing.T) {
		m := newTestManager(&mockClient{})
		ch := make(chan interface{})
		m.config.voluntarilyReleaseLockCh = ch
		assert.NotPanics(t, m.releaseConsulLock)
		select {
		case <-ch:
			t.Error("channel must stay open")
		default:
		}
	})

	t.Run("master without channel", func(t *testing.T) {
		m := newTestManager(&mockClient{})
		m.state.Master = true
		assert.NotPanics(t, m.releaseConsulLock)
	})

	t.Run("master with channel, twice", func(t *testing.T) {
		m := newTestManager(&mockClient{})
		m.state.Master = true
		ch := make(chan interface{})
		m.config.voluntarilyReleaseLockCh = ch

		m.releaseConsulLock()
		select {
		case <-ch:
		default:
			t.Error("channel not closed")
		}
		assert.Nil(t, m.config.voluntarilyReleaseLockCh)
		assert.NotPanics(t, m.releaseConsulLock)
	})
}

func TestManager_CommandRunner_Dispatch(t *testing.T) {
	client := &mockClient{}
	m := newTestManager(client)
	m.commandCh = make(chan Command, 10)
	m.state.Master = true
	m.state.MasterServices = []state.ConsulService{{Node: "n", ServiceID: "redis@:6379"}, {Node: "m", ServiceID: "y"}}
	m.config.nodeName = "n"
	m.config.serviceID = "redis@:6379"
	done := make(chan struct{})
	go func() {
		m.CommandRunner()
		close(done)
	}()

	m.commandCh <- NewCommand(RegisterServiceCommand, state.Redis{})
	m.commandCh <- NewCommand(UpdateServiceCommand, state.Redis{InfoString: "i"})
	m.commandCh <- NewCommand(DeregisterMultiMasterCommand, state.Redis{})
	m.commandCh <- NewCommand(DeregisterServiceCommand, state.Redis{})
	m.commandCh <- NewCommand(ReleaseLockCommand, state.Redis{})
	m.commandCh <- NewCommand(StopConsulCommand, state.Redis{})

	<-done
	assert.Equal(t, "register_service", string(RegisterServiceCommand))
	assert.Equal(t, 1, client.count("ServiceRegister"))
	assert.Equal(t, 1, client.count("UpdateTTL"))
	assert.Equal(t, 1, client.count("CatalogDeregister"))
}

func TestManager_Accessors(t *testing.T) {
	m := newTestManager(&mockClient{})
	m.config.announceHost = "h"
	m.config.announcePort = 7
	host, port := m.AnnounceAddr()
	assert.Equal(t, "h", host)
	assert.Equal(t, 7, port)
	assert.NotNil(t, m.GetStateReader())
	m.commandCh = make(chan Command, 1)
	assert.NotNil(t, m.GetCommandWriter())

	c := NewCommand(StartCommand, state.Redis{})
	assert.Equal(t, StartCommand, c.Name())
	assert.Equal(t, "start", c.String())
}
