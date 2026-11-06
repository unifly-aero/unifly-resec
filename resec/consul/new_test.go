package consul

import (
	"testing"
	"time"

	"github.com/aireuropa/resec/resec/internal/clitest"
	"github.com/aireuropa/resec/resec/redis"
	"github.com/stretchr/testify/assert"
)

func newConn(t *testing.T, redisAddr string, args ...string) (*Manager, error) {
	t.Helper()
	c := clitest.NewContext(t, args...)
	return NewConnection(c, redis.Config{Address: redisAddr})
}

func TestNewConnection_Defaults(t *testing.T) {
	m, err := newConn(t, "127.0.0.1:6379")
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, ":6379", m.config.announceAddr)
	assert.Equal(t, "", m.config.announceHost)
	assert.Equal(t, 6379, m.config.announcePort)
	assert.Equal(t, 6*time.Second, m.config.serviceTTL)
	assert.True(t, m.state.Healthy)
	assert.NotNil(t, m.client)
}

func TestNewConnection_Localhost(t *testing.T) {
	m, err := newConn(t, "localhost:6379")
	assert.NoError(t, err)
	assert.Equal(t, ":6379", m.config.announceAddr)
}

func TestNewConnection_IPv6Loopback(t *testing.T) {
	m, err := newConn(t, "[::1]:6379")
	if assert.NoError(t, err) {
		assert.Equal(t, ":6379", m.config.announceAddr)
		assert.Equal(t, "", m.config.announceHost)
		assert.Equal(t, 6379, m.config.announcePort)
	}
}

func TestNewConnection_IPv6Remote(t *testing.T) {
	m, err := newConn(t, "[fd00::1]:6379")
	if assert.NoError(t, err) {
		assert.Equal(t, "[fd00::1]:6379", m.config.announceAddr)
		assert.Equal(t, "fd00::1", m.config.announceHost)
		assert.Equal(t, 6379, m.config.announcePort)
	}
}

func TestNewConnection_RedisAddrWithoutPort(t *testing.T) {
	for _, addr := range []string{"localhost", "nonsense"} {
		_, err := newConn(t, addr)
		assert.Error(t, err, addr)
	}
}

func TestNewConnection_RemoteRedis(t *testing.T) {
	m, err := newConn(t, "10.0.0.5:6379")
	if assert.NoError(t, err) {
		assert.Equal(t, "10.0.0.5:6379", m.config.announceAddr)
		assert.Equal(t, "10.0.0.5", m.config.announceHost)
	}
}

func TestNewConnection_AnnounceAddr(t *testing.T) {
	m, err := newConn(t, "127.0.0.1:6379", "--announce-addr", "10.1.1.1:7000")
	if assert.NoError(t, err) {
		assert.Equal(t, "10.1.1.1", m.config.announceHost)
		assert.Equal(t, 7000, m.config.announcePort)
	}
	host, port := m.AnnounceAddr()
	assert.Equal(t, "10.1.1.1", host)
	assert.Equal(t, 7000, port)
}

func TestNewConnection_Errors(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string // empty: any error
	}{
		{"bad announce addr", []string{"--announce-addr", "nonsense"}, ""},
		{"bad port", []string{"--announce-addr", "10.1.1.1:abc"}, ""},
		{"lock TTL too low", []string{"--consul-lock-ttl", "10s"}, "15s"},
		{"monitor window too large", []string{"--consul-lock-monitor-retries", "100", "--consul-lock-monitor-retry-interval", "1s", "--consul-lock-ttl", "15s"}, ""},
		{"service name without master tags", []string{"--consul-service-name", "cache"}, "MASTER_TAGS"},
		{"same first tag", []string{"--consul-service-name", "cache", "--consul-master-tags", "a,b", "--consul-slave-tags", "a,c"}, "unique"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := newConn(t, "127.0.0.1:6379", tt.args...)
			if assert.Error(t, err) && tt.wantErr != "" {
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

func TestNewConnection_MonitorWindowAtLimit(t *testing.T) {
	_, err := newConn(t, "127.0.0.1:6379",
		"--consul-lock-monitor-retries", "30",
		"--consul-lock-monitor-retry-interval", "1s",
		"--consul-lock-ttl", "15s")
	assert.NoError(t, err)
}

func TestNewConnection_TagsParsed(t *testing.T) {
	m, err := newConn(t, "127.0.0.1:6379", "--consul-master-tags", "m1,m2", "--consul-slave-tags", "s1")
	if assert.NoError(t, err) {
		assert.Equal(t, []string{"m1", "m2"}, m.config.serviceTagsByRole["master"])
		assert.Equal(t, []string{"s1"}, m.config.serviceTagsByRole["slave"])
	}
}

func TestNewConnection_NoTagsPrefixMode(t *testing.T) {
	m, err := newConn(t, "127.0.0.1:6379")
	if assert.NoError(t, err) {
		for _, role := range []string{"master", "slave"} {
			tags := m.config.serviceTagsByRole[role]
			assert.NotNil(t, tags, role)
			assert.Empty(t, tags, role)
		}
	}
}

func TestNewConnection_ServiceNameMode(t *testing.T) {
	m, err := newConn(t, "127.0.0.1:6379", "--consul-service-name", "cache", "--consul-master-tags", "m", "--consul-slave-tags", "s")
	if assert.NoError(t, err) {
		assert.Equal(t, "cache", m.config.serviceName)
	}
}
