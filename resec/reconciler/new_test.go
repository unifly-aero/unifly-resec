package reconciler

import (
	"testing"
	"time"

	"github.com/aireuropa/resec/resec/internal/clitest"
	"github.com/stretchr/testify/assert"
)

// newReconciler builds a Reconciler from CLI args.
//
// NewReconciler starts the CommandRunner goroutines of both managers. The
// cleanup calls Close, which stops them and the signal delivery.
func newReconciler(t *testing.T, args ...string) (*Reconciler, error) {
	t.Helper()
	r, err := NewReconciler(clitest.NewContext(t, args...))
	if r != nil {
		t.Cleanup(r.Close)
	}
	return r, err
}

func TestNewReconciler_Defaults(t *testing.T) {
	r, err := newReconciler(t)
	if !assert.NoError(t, err) {
		return
	}
	assert.Equal(t, "127.0.0.1", r.redisHost)
	assert.Equal(t, "", r.announceHost)
	assert.Equal(t, 6379, r.announcePort)
	assert.Equal(t, 1, r.consulHealthFailures)
	assert.Equal(t, 2*time.Second, r.consulHealthTimeout)
	assert.Equal(t, 2*time.Second, r.forceReconcileInterval)
	assert.NotNil(t, r.consulHealth)
	assert.False(t, r.stateServerOn)
	assert.NotNil(t, r.consulCommandCh)
	assert.NotNil(t, r.redisCommandCh)
}

func TestNewReconciler_FailuresBelowOne(t *testing.T) {
	r, err := newReconciler(t, "--health-consul-failures", "0")
	if assert.NoError(t, err) {
		assert.Equal(t, 1, r.consulHealthFailures)
	}
}

func TestNewReconciler_Failures5(t *testing.T) {
	r, err := newReconciler(t, "--health-consul-failures", "5")
	if assert.NoError(t, err) {
		assert.Equal(t, 5, r.consulHealthFailures)
	}
}

func TestNewReconciler_AnnounceAddr(t *testing.T) {
	r, err := newReconciler(t, "--announce-addr", "10.0.0.9:6400")
	if assert.NoError(t, err) {
		assert.Equal(t, "10.0.0.9", r.announceHost)
		assert.Equal(t, 6400, r.announcePort)
	}
}

func TestNewReconciler_StateServer(t *testing.T) {
	r, err := newReconciler(t, "--state-server", "--state-listen-addr", "127.0.0.1:0")
	if assert.NoError(t, err) {
		assert.True(t, r.stateServerOn)
		assert.Equal(t, "127.0.0.1:0", r.stateListenAddress)
	}
}

func TestNewReconciler_BadConfig(t *testing.T) {
	_, err := newReconciler(t, "--consul-lock-ttl", "1s")
	assert.Error(t, err)
}

func TestNewReconciler_IntervalTooLow(t *testing.T) {
	r, err := newReconciler(t, "--healthcheck-timeout", "100ms")
	assert.Nil(t, r)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "healthcheck-timeout")
		assert.Contains(t, err.Error(), "500ms")
	}
}

func TestReconciler_Close(t *testing.T) {
	r, err := NewReconciler(clitest.NewContext(t))
	if !assert.NoError(t, err) {
		return
	}

	assert.NotNil(t, r.consulManager)
	assert.NotNil(t, r.redisManager)

	r.Close()
	assert.NotPanics(t, r.Close) // safe to call twice
}
