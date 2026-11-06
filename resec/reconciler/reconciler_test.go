// +test

package reconciler

import (
	"testing"
	"time"

	"github.com/aireuropa/resec/resec/consul"
	"github.com/aireuropa/resec/resec/redis"
	"github.com/aireuropa/resec/resec/state"
	"github.com/stretchr/testify/assert"
)

func TestReconciler_RunBecomeMaster(t *testing.T) {
	helper := newTestReconciler(t)
	helper.consume()
	defer helper.stop()

	// initial state, reconciler can't do any work because it lacks state
	helper.
		eval(ResultMissingState)

	// consul updated state, but do not have master lock
	helper.
		withConsulState(state.Consul{
			Ready:   true,
			Healthy: true,
			Master:  false,
		}).
		eval(ResultMissingState)

	// redis updated state, connected but no known replication status
	helper.
		withRedisState(state.Redis{
			Ready:   true,
			Healthy: true,
		}).
		eval(ResultNoMasterElected)

	// consul updated state, we know hold the master lock, so configure redis to become master
	helper.
		withConsulState(state.Consul{
			Ready:   true,
			Healthy: true,
			Master:  true,
		}).
		expectConsulCommands(
			consul.RegisterServiceCommand,
		).
		expectRedisCommands(
			redis.RunAsMasterCommand,
		).
		eval(ResultRunAsMaster)

	// redis updated state, its now running as a master node, so all we need to do is update the service
	helper.
		withRedisState(state.Redis{
			Ready:   true,
			Healthy: true,
			Info: state.RedisStatus{
				Role: "master",
			},
		}).
		expectConsulCommands(
			consul.UpdateServiceCommand,
		).
		eval(ResultUpdateService)

	// no state change, so all we need to do is update the service
	helper.
		expectConsulCommands(
			consul.UpdateServiceCommand,
		).
		eval(ResultUpdateService)
}

func TestReconciler_UnhealthyConsul(t *testing.T) {
	helper := newTestReconciler(t)
	helper.consume()
	defer helper.stop()

	// with local Redis healthy, and local Consul unhealthy,
	// the reconciler should not do any work at all
	helper.
		withRedisState(state.Redis{
			Ready:   true,
			Healthy: true,
		}).
		withConsulState(state.Consul{
			Ready:   true,
			Healthy: false,
		}).
		eval(ResultConsulNotHealthy)
}

func TestReconciler_UnhealthyRedis(t *testing.T) {
	helper := newTestReconciler(t)
	helper.consume()
	defer helper.stop()

	// with local Consul healthy, and local Redis unhealthy,
	// the reconciler should give up the consul lock (if held) and deregister the service
	helper.
		withConsulState(state.Consul{
			Ready:   true,
			Healthy: true,
		}).
		withRedisState(state.Redis{
			Ready:   true,
			Healthy: false,
		}).
		expectConsulCommands(
			consul.ReleaseLockCommand,
			consul.DeregisterServiceCommand,
		).
		eval(ResultRedisNotHealthy)
}

func TestReconciler_SlaveNoMasterElected(t *testing.T) {
	helper := newTestReconciler(t)
	helper.consume()
	defer helper.stop()

	// with local Consul and local Redis healthy, but no cluster elected Consul master
	// the reconciler should do no work
	helper.
		withConsulState(state.Consul{
			Ready:   true,
			Healthy: true,
		}).
		withRedisState(state.Redis{
			Ready:   true,
			Healthy: true,
		}).
		eval(ResultNoMasterElected)
}

func TestReconciler_SlaveMasterElected(t *testing.T) {
	helper := newTestReconciler(t)
	helper.consume()
	defer helper.stop()

	// with local Consul and Redis healthy but not elected Consul master
	// and a remote Consul master, local redis should be enslaved to the remote master
	helper.
		withConsulState(state.Consul{
			Ready:      true,
			Healthy:    true,
			MasterAddr: "127.0.0.1",
			MasterPort: 6379,
		}).
		withRedisState(state.Redis{
			Ready:   true,
			Healthy: true,
		}).
		expectRedisCommands(
			redis.RunAsSlaveCommand,
		).
		eval(ResultRunAsSlave)
}

func TestReconciler_SlaveMasterElectedAlready(t *testing.T) {
	helper := newTestReconciler(t)
	helper.consume()
	defer helper.stop()

	// with local Consul and Redis healthy, but not elected Consul master
	// and a remote Consul master, which Redis is already enslaved to
	// the reconciler should only update the Consul service
	helper.
		withConsulState(state.Consul{
			Ready:      true,
			Healthy:    true,
			MasterAddr: "127.0.0.1",
			MasterPort: 6379,
		}).
		withRedisState(state.Redis{
			Ready:   true,
			Healthy: true,
			Info: state.RedisStatus{
				MasterHost:   "127.0.0.1",
				MasterPort:   6379,
				MasterLinkUp: true,
			},
		}).
		expectConsulCommands(
			consul.UpdateServiceCommand,
		).
		eval(ResultUpdateService)
}

func TestReconciler_SlaveMasterSyncInProgress(t *testing.T) {
	helper := newTestReconciler(t)
	helper.consume()
	defer helper.stop()

	// with local Consul and Redis healthy, but not elected Consul master
	// and a remote Consul master, which Redis is already enslaved to
	// but currently syncing data from master,
	// the reconciler should deregister service and wait for sync to complete
	helper.
		withConsulState(state.Consul{
			Ready:      true,
			Healthy:    true,
			MasterAddr: "127.0.0.1",
			MasterPort: 6379,
		}).
		withRedisState(state.Redis{
			Ready:   true,
			Healthy: true,
			Info: state.RedisStatus{
				MasterHost:           "127.0.0.1",
				MasterPort:           6379,
				MasterSyncInProgress: true,
				MasterLinkUp:         true,
			},
		}).
		expectConsulCommands(
			consul.DeregisterServiceCommand,
		).
		eval(ResultMasterSyncInProgress)
}
func TestReconciler_SlaveMasterLinkDownToolong(t *testing.T) {
	helper := newTestReconciler(t)
	helper.consume()
	defer helper.stop()

	// with local Consul and Redis healthy, but not elected Consul master
	// and a remote Consul master, which Redis is already enslaved,
	// but the replication link has been down for too long
	helper.
		withConsulState(state.Consul{
			Ready:      true,
			Healthy:    true,
			MasterAddr: "127.0.0.1",
			MasterPort: 6379,
		}).
		withRedisState(state.Redis{
			Ready:   true,
			Healthy: true,
			Info: state.RedisStatus{
				MasterHost:          "127.0.0.1",
				MasterPort:          6379,
				MasterLinkUp:        false,
				MasterLinkDownSince: 15 * time.Second,
			},
		}).
		expectConsulCommands(
			consul.DeregisterServiceCommand,
		).
		eval(ResultMasterLinkDown)
}
func TestReconciler_SlaveMasterLinkDownWithinReason(t *testing.T) {
	helper := newTestReconciler(t)
	helper.consume()
	defer helper.stop()

	// with local Consul and Redis healthy, but not elected Consul master
	// and a remote Consul master, which Redis is already enslaved,
	// but the replication link has been down within the limit configured
	helper.
		withConsulState(state.Consul{
			Ready:      true,
			Healthy:    true,
			MasterAddr: "127.0.0.1",
			MasterPort: 6379,
		}).
		withRedisState(state.Redis{
			Ready:   true,
			Healthy: true,
			Info: state.RedisStatus{
				MasterHost:          "127.0.0.1",
				MasterPort:          6379,
				MasterLinkUp:        false,
				MasterLinkDownSince: 5 * time.Second,
			},
		}).
		expectConsulCommands(
			consul.UpdateServiceCommand,
		).
		eval(ResultUpdateService)
}

func TestReconciler_SlaveMasterIsSelf(t *testing.T) {
	helper := newTestReconciler(t)
	helper.reconciler.announceHost = "redis-resec-2"
	helper.reconciler.announcePort = 6379
	helper.consume()
	defer helper.stop()

	// the lock is lost but the catalog still lists our own service as master,
	// Redis must not be enslaved to its own address
	helper.
		withConsulState(state.Consul{
			Ready:      true,
			Healthy:    true,
			Master:     false,
			MasterAddr: "redis-resec-2",
			MasterPort: 6379,
		}).
		withRedisState(state.Redis{
			Ready:   true,
			Healthy: true,
			Info:    state.RedisStatus{Role: "master"},
		}).
		eval(ResultNoMasterElected)
}

func TestReconciler_SlaveMasterIsSelf_PortOnly(t *testing.T) {
	helper := newTestReconciler(t)
	helper.reconciler.announcePort = 6379
	helper.reconciler.redisHost = "127.0.0.1"
	helper.consume()
	defer helper.stop()

	helper.
		withConsulState(state.Consul{
			Ready:      true,
			Healthy:    true,
			MasterAddr: "127.0.0.1",
			MasterPort: 6379,
		}).
		withRedisState(state.Redis{
			Ready:   true,
			Healthy: true,
		}).
		eval(ResultNoMasterElected)
}

func healthyConsul() state.Consul { return state.Consul{Ready: true, Healthy: true} }
func healthyRedis() state.Redis   { return state.Redis{Ready: true, Healthy: true} }

func TestReconciler_EvaluateExtra(t *testing.T) {
	twoMasters := []state.ConsulService{{ServiceID: "a"}, {ServiceID: "b"}}
	slaveOf := func(host string, port int, up bool, down time.Duration, sync bool) state.Redis {
		r := healthyRedis()
		r.Info = state.RedisStatus{
			Role: "slave", MasterHost: host, MasterPort: port,
			MasterLinkUp: up, MasterLinkDownSince: down, MasterSyncInProgress: sync,
		}
		return r
	}
	slaveConsul := state.Consul{Ready: true, Healthy: true, MasterAddr: "10.0.0.2", MasterPort: 6379}

	tests := []struct {
		name       string
		consul     state.Consul
		redis      state.Redis
		want       resultType
		consulCmds []consul.CommandName
		redisCmds  []redis.CommandName
	}{
		{
			name:       "multi-master",
			consul:     state.Consul{Ready: true, Healthy: true, Master: true, MasterServices: twoMasters},
			redis:      state.Redis{Ready: true, Healthy: true, Info: state.RedisStatus{Role: "master"}},
			want:       ResultMultiMaster,
			consulCmds: []consul.CommandName{consul.DeregisterMultiMasterCommand},
		},
		{
			name:       "multi-master beats slave path",
			consul:     state.Consul{Ready: true, Healthy: true, Master: false, MasterAddr: "10.0.0.2", MasterPort: 6379, MasterServices: twoMasters},
			redis:      healthyRedis(),
			want:       ResultMultiMaster,
			consulCmds: []consul.CommandName{consul.DeregisterMultiMasterCommand},
		},
		{
			name:   "consul not healthy beats redis not healthy",
			consul: state.Consul{Ready: true, Healthy: false},
			redis:  state.Redis{Ready: true, Healthy: false},
			want:   ResultConsulNotHealthy,
		},
		{
			name:   "missing redis state only",
			consul: healthyConsul(),
			redis:  state.Redis{Ready: false},
			want:   ResultMissingState,
		},
		{
			name:   "missing consul state only",
			consul: state.Consul{Ready: false},
			redis:  healthyRedis(),
			want:   ResultMissingState,
		},
		{
			name:       "master already master",
			consul:     state.Consul{Ready: true, Healthy: true, Master: true},
			redis:      state.Redis{Ready: true, Healthy: true, Info: state.RedisStatus{Role: "master"}},
			want:       ResultUpdateService,
			consulCmds: []consul.CommandName{consul.UpdateServiceCommand},
		},
		{
			name:       "master, redis is slave",
			consul:     state.Consul{Ready: true, Healthy: true, Master: true},
			redis:      slaveOf("10.0.0.3", 6379, true, 0, false),
			want:       ResultRunAsMaster,
			consulCmds: []consul.CommandName{consul.RegisterServiceCommand},
			redisCmds:  []redis.CommandName{redis.RunAsMasterCommand},
		},
		{
			name:      "slave, wrong master host",
			consul:    slaveConsul,
			redis:     slaveOf("10.0.0.3", 6379, true, 0, false),
			want:      ResultRunAsSlave,
			redisCmds: []redis.CommandName{redis.RunAsSlaveCommand},
		},
		{
			name:      "slave, wrong master port",
			consul:    slaveConsul,
			redis:     slaveOf("10.0.0.2", 6380, true, 0, false),
			want:      ResultRunAsSlave,
			redisCmds: []redis.CommandName{redis.RunAsSlaveCommand},
		},
		{
			name:      "slave, redis thinks master",
			consul:    slaveConsul,
			redis:     state.Redis{Ready: true, Healthy: true, Info: state.RedisStatus{Role: "master"}},
			want:      ResultRunAsSlave,
			redisCmds: []redis.CommandName{redis.RunAsSlaveCommand},
		},
		{
			name:       "link down exactly 10s",
			consul:     slaveConsul,
			redis:      slaveOf("10.0.0.2", 6379, false, 10*time.Second, false),
			want:       ResultUpdateService,
			consulCmds: []consul.CommandName{consul.UpdateServiceCommand},
		},
		{
			name:       "link down 11s",
			consul:     slaveConsul,
			redis:      slaveOf("10.0.0.2", 6379, false, 11*time.Second, false),
			want:       ResultMasterLinkDown,
			consulCmds: []consul.CommandName{consul.DeregisterServiceCommand},
		},
		{
			name:       "sync beats link down",
			consul:     slaveConsul,
			redis:      slaveOf("10.0.0.2", 6379, false, 20*time.Second, true),
			want:       ResultMasterSyncInProgress,
			consulCmds: []consul.CommandName{consul.DeregisterServiceCommand},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestReconciler(t)
			h.consume()
			defer h.stop()

			h.withConsulState(tt.consul).withRedisState(tt.redis)
			if tt.consulCmds != nil {
				h.expectConsulCommands(tt.consulCmds...)
			}
			if tt.redisCmds != nil {
				h.expectRedisCommands(tt.redisCmds...)
			}
			h.eval(tt.want)
		})
	}
}

func TestReconciler_RunAsSlavePayload(t *testing.T) {
	h := newTestReconciler(t)
	h.consume()
	defer h.stop()

	h.withConsulState(state.Consul{Ready: true, Healthy: true, MasterAddr: "10.0.0.2", MasterPort: 6379}).
		withRedisState(state.Redis{Ready: true, Healthy: true, Info: state.RedisStatus{Role: "master"}}).
		expectRedisCommands(redis.RunAsSlaveCommand).
		eval(ResultRunAsSlave)

	cmds := h.redisCommands()
	if assert.Len(t, cmds, 1) {
		cs := cmds[0].ConsulState()
		assert.Equal(t, "10.0.0.2", cs.MasterAddr)
		assert.Equal(t, 6379, cs.MasterPort)
	}
}

func TestReconciler_ConsulCommandCarriesRedisState(t *testing.T) {
	h := newTestReconciler(t)
	h.consume()
	defer h.stop()

	h.withConsulState(state.Consul{Ready: true, Healthy: true, Master: true}).
		withRedisState(state.Redis{Ready: true, Healthy: true, InfoString: "role:master", Info: state.RedisStatus{Role: "master"}}).
		expectConsulCommands(consul.UpdateServiceCommand).
		eval(ResultUpdateService)

	cmds := h.consulCommands()
	if assert.Len(t, cmds, 1) {
		assert.Equal(t, "role:master", cmds[0].RedisState().InfoString)
	}
}

func TestReconciler_ApplyNoCommands(t *testing.T) {
	for _, res := range []resultType{"bogus", ResultNoMasterElected, ResultConsulNotHealthy, ResultMissingState, ResultUnknown} {
		t.Run(string(res), func(t *testing.T) {
			h := newTestReconciler(t)
			h.consume()
			defer h.stop()

			assert.NotPanics(t, func() { h.reconciler.apply(res) })
			time.Sleep(5 * time.Millisecond)
			assert.Empty(t, h.seenConsul())
			assert.Empty(t, h.seenRedis())
		})
	}
}

func TestReconciler_masterIsSelf(t *testing.T) {
	tests := []struct {
		name         string
		announceHost string
		announcePort int
		redisHost    string
		masterAddr   string
		masterPort   int
		want         bool
	}{
		{"nothing announced", "", 0, "", "10.0.0.1", 6379, false},
		{"announced match", "10.0.0.1", 6379, "", "10.0.0.1", 6379, true},
		{"port differs", "10.0.0.1", 6379, "", "10.0.0.1", 6380, false},
		{"host differs", "10.0.0.1", 6379, "", "10.0.0.2", 6379, false},
		{"port only, redis host match", "", 6379, "127.0.0.1", "127.0.0.1", 6379, true},
		{"port only, no hosts", "", 6379, "", "", 6379, false},
		{"announceHost wins", "h1", 6379, "h2", "h2", 6379, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestReconciler(t).reconciler
			r.announceHost, r.announcePort, r.redisHost = tt.announceHost, tt.announcePort, tt.redisHost
			r.consulState = state.Consul{MasterAddr: tt.masterAddr, MasterPort: tt.masterPort}
			assert.Equal(t, tt.want, r.masterIsSelf())
		})
	}
}

func TestReconciler_notSlaveOfCurrentMaster(t *testing.T) {
	tests := []struct {
		name   string
		redis  state.RedisStatus
		consul state.Consul
		want   bool
	}{
		{"redis master", state.RedisStatus{Role: "master"}, state.Consul{}, true},
		{"match", state.RedisStatus{Role: "slave", MasterHost: "a", MasterPort: 1}, state.Consul{MasterAddr: "a", MasterPort: 1}, false},
		{"host differs", state.RedisStatus{Role: "slave", MasterHost: "a", MasterPort: 1}, state.Consul{MasterAddr: "b", MasterPort: 1}, true},
		{"port differs", state.RedisStatus{Role: "slave", MasterHost: "a", MasterPort: 1}, state.Consul{MasterAddr: "a", MasterPort: 2}, true},
		// Edge: an empty slave with no host and no consul master counts as a
		// slave of the current master. evaluate() never gets here because
		// NoMasterElected is checked first.
		{"empty slave", state.RedisStatus{Role: "slave"}, state.Consul{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := newTestReconciler(t).reconciler
			r.redisState = state.Redis{Info: tt.redis}
			r.consulState = tt.consul
			assert.Equal(t, tt.want, r.notSlaveOfCurrentMaster())
		})
	}
}

func TestReconciler_missingInitialState(t *testing.T) {
	for _, redisReady := range []bool{false, true} {
		for _, consulReady := range []bool{false, true} {
			r := newTestReconciler(t).reconciler
			r.redisState.Ready = redisReady
			r.consulState.Ready = consulReady
			assert.Equal(t, !(redisReady && consulReady), r.missingInitialState(), "redis=%v consul=%v", redisReady, consulReady)
		}
	}
}
