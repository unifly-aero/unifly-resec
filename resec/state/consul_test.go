package state

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConsul_NoMasterElected(t *testing.T) {
	tests := []struct {
		name string
		c    Consul
		want bool
	}{
		{"empty", Consul{}, true},
		{"addr only", Consul{MasterAddr: "10.0.0.1"}, false},
		{"port only", Consul{MasterPort: 6379}, false},
		{"addr and port", Consul{MasterAddr: "10.0.0.1", MasterPort: 6379}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.c.NoMasterElected())
		})
	}
}

func TestConsul_IsUnhealhy(t *testing.T) {
	assert.True(t, (&Consul{Healthy: false}).IsUnhealhy())
	assert.False(t, (&Consul{Healthy: true}).IsUnhealhy())
}

func TestConsul_IsMasterIsSlave(t *testing.T) {
	m := &Consul{Master: true}
	assert.True(t, m.IsMaster())
	assert.False(t, m.IsSlave())

	s := &Consul{Master: false}
	assert.False(t, s.IsMaster())
	assert.True(t, s.IsSlave())
}

func TestConsul_IsMultiMaster(t *testing.T) {
	one := ConsulService{ServiceID: "a"}
	two := ConsulService{ServiceID: "b"}
	tests := []struct {
		name string
		svc  []ConsulService
		want bool
	}{
		{"nil", nil, false},
		{"one", []ConsulService{one}, false},
		{"two", []ConsulService{one, two}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Consul{MasterServices: tt.svc}
			assert.Equal(t, tt.want, c.IsMultiMaster())
		})
	}
}
