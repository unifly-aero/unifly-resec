package redis

import (
	"testing"
	"time"

	"github.com/aireuropa/resec/resec/state"
	"github.com/stretchr/testify/assert"
)

// watchStatus has a hard-coded 1s first tick. It returns when stopCh is
// closed. Most tests below do not close stopCh, so their watcher goroutine
// lives until the test binary exits. Only
// TestManager_watchStatus_StopsOnStopCh waits for the return.

const watchTimeout = 4 * time.Second

func nextState(t *testing.T, m *Manager) state.Redis {
	t.Helper()
	select {
	case s := <-m.stateCh:
		return s
	case <-time.After(watchTimeout):
		t.Fatal("no state emitted")
		return state.Redis{}
	}
}

func TestManager_watchStatus_FirstInfo(t *testing.T) {
	f := newFakeRedis(t)
	raw := "# Replication\r\nrole:master\r\n"
	f.SetInfo(raw)
	m := newTestManager(t, f.Addr())

	go m.watchStatus()

	s := nextState(t, m)
	assert.True(t, s.Ready)
	assert.True(t, s.Healthy)
	assert.Equal(t, "master", s.Info.Role)
	assert.Equal(t, raw, s.InfoString)
}

func TestManager_watchStatus_Loading(t *testing.T) {
	f := newFakeRedis(t)
	f.SetInfo("loading:1\r\n")
	m := newTestManager(t, f.Addr())

	go m.watchStatus()

	s := nextState(t, m)
	assert.False(t, s.Healthy)
	assert.True(t, s.Ready)
	assert.True(t, s.Info.Loading)
}

func TestManager_watchStatus_ConnectionRefused(t *testing.T) {
	f := newFakeRedis(t)
	addr := f.Addr()
	f.Close()
	m := newTestManager(t, addr)

	go m.watchStatus()

	for i := 0; i < 2; i++ {
		s := nextState(t, m)
		assert.False(t, s.Healthy)
		assert.False(t, s.Ready)
	}
	assert.Greater(t, m.backoff.Attempt(), float64(0))
}

func TestManager_watchStatus_Recovery(t *testing.T) {
	f := newFakeRedis(t)
	addr := f.Addr()
	f.Close()
	m := newTestManager(t, addr)

	go m.watchStatus()

	s := nextState(t, m)
	assert.False(t, s.Healthy)

	f2 := restartFakeRedis(t, addr)
	f2.SetInfo("role:master\r\n")

	deadline := time.After(watchTimeout)
	for {
		select {
		case s = <-m.stateCh:
			if s.Healthy {
				assert.True(t, s.Ready)
				assert.Equal(t, float64(0), m.backoff.Attempt())
				return
			}
		case <-deadline:
			t.Fatal("never became healthy")
		}
	}
}

func TestManager_watchStatus_DoubleStart(t *testing.T) {
	f := newFakeRedis(t)
	f.SetInfo("role:master\r\n")
	m := newTestManager(t, f.Addr())

	go m.watchStatus()
	// The first emit happens inside the loop, so the flag is set by then.
	nextState(t, m)

	returned := make(chan struct{})
	go func() {
		m.watchStatus()
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("second watchStatus did not return at once")
	}
	// The first watcher only clears the flag on exit, which does not happen.
	m.mu.Lock()
	assert.True(t, m.watcherRunning)
	m.mu.Unlock()
}

func TestManager_CommandRunner_StartCommand(t *testing.T) {
	f := newFakeRedis(t)
	f.SetInfo("role:master\r\n")
	m := newTestManager(t, f.Addr())
	done := startRunner(m)
	defer stopRunner(m, done)

	m.commandCh <- NewCommand(StartCommand, state.Consul{})
	s := nextState(t, m)
	assert.True(t, s.Ready)
}

func TestManager_watchStatus_StopsOnStopCh(t *testing.T) {
	f := newFakeRedis(t)
	f.SetInfo("role:master\r\n")
	m := newTestManager(t, f.Addr())

	returned := make(chan struct{})
	go func() {
		m.watchStatus()
		close(returned)
	}()

	close(m.stopCh)

	select {
	case <-returned:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("watchStatus did not return after stopCh was closed")
	}
}
