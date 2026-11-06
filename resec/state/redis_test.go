package state

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRedis_IsRedisMaster(t *testing.T) {
	tests := []struct {
		role string
		want bool
	}{
		{"master", true},
		{"slave", false},
		{"", false},
	}
	for _, tt := range tests {
		t.Run("role="+tt.role, func(t *testing.T) {
			r := &Redis{Info: RedisStatus{Role: tt.role}}
			assert.Equal(t, tt.want, r.IsRedisMaster())
		})
	}
}

func TestRedis_IsUnhealthy(t *testing.T) {
	assert.True(t, (&Redis{Healthy: false}).IsUnhealthy())
	assert.False(t, (&Redis{Healthy: true}).IsUnhealthy())
}

func TestRedis_IsReadyToServe(t *testing.T) {
	tests := []struct {
		name string
		r    Redis
		want bool
	}{
		{"master", Redis{Healthy: true, Ready: true, Info: RedisStatus{Role: "master"}}, true},
		{"slave synced", Redis{Healthy: true, Ready: true, Info: RedisStatus{Role: "slave", MasterLinkUp: true}}, true},
		{"slave link down", Redis{Healthy: true, Ready: true, Info: RedisStatus{Role: "slave", MasterLinkUp: false}}, false},
		{"slave syncing", Redis{Healthy: true, Ready: true, Info: RedisStatus{Role: "slave", MasterLinkUp: true, MasterSyncInProgress: true}}, false},
		{"loading", Redis{Healthy: true, Ready: true, Info: RedisStatus{Role: "master", Loading: true}}, false},
		{"not ready", Redis{Healthy: true, Ready: false, Info: RedisStatus{Role: "master"}}, false},
		{"not healthy", Redis{Healthy: false, Ready: true, Info: RedisStatus{Role: "master"}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.r.IsReadyToServe())
		})
	}
}

func TestRedisStatus_Changed(t *testing.T) {
	base := RedisStatus{
		Role:       "slave",
		MasterHost: "a",
		MasterPort: 1,
	}

	tests := []struct {
		name   string
		mutate func(s *RedisStatus)
		want   bool
	}{
		{"equal", func(s *RedisStatus) {}, false},
		{"role", func(s *RedisStatus) { s.Role = "master" }, true},
		{"master host", func(s *RedisStatus) { s.MasterHost = "b" }, true},
		{"master port", func(s *RedisStatus) { s.MasterPort = 2 }, true},
		{"link up", func(s *RedisStatus) { s.MasterLinkUp = true }, true},
		{"sync in progress", func(s *RedisStatus) { s.MasterSyncInProgress = true }, true},
		{"loading", func(s *RedisStatus) { s.Loading = true }, true},
		// MasterLinkDownSince is not compared on purpose: it grows every
		// second while the link is down and would force a reconcile each time.
		{"link down since", func(s *RedisStatus) { s.MasterLinkDownSince = 30 * time.Second }, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			next := base
			tt.mutate(&next)
			assert.Equal(t, tt.want, base.Changed(next))
		})
	}
}
