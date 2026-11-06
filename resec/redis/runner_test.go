package redis

import (
	"slices"
	"testing"
	"time"

	"github.com/aireuropa/resec/resec/state"
	"github.com/jpillora/backoff"
	goredis "github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func newTestManager(t *testing.T, addr string) *Manager {
	t.Helper()
	client := goredis.NewClient(&goredis.Options{
		Addr:            addr,
		DialTimeout:     100 * time.Millisecond,
		ReadTimeout:     100 * time.Millisecond,
		WriteTimeout:    100 * time.Millisecond,
		MaxRetries:      -1,
		DisableIdentity: true,
	})
	t.Cleanup(func() { client.Close() })

	logger := log.New()
	logger.SetLevel(log.PanicLevel)

	return &Manager{
		client:    client,
		config:    &Config{Address: addr},
		logger:    log.NewEntry(logger),
		state:     &state.Redis{},
		stateCh:   make(chan state.Redis, 10),
		commandCh: make(chan Command, 10),
		stopCh:    make(chan interface{}, 1),
		backoff:   &backoff.Backoff{Min: time.Millisecond, Max: 5 * time.Millisecond, Factor: 1.5},
	}
}

// startRunner runs CommandRunner and returns a channel closed when it returns.
func startRunner(m *Manager) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.CommandRunner()
	}()
	return done
}

func waitCommands(t *testing.T, f *fakeRedis, want ...string) {
	t.Helper()
	assert.Eventually(t, func() bool {
		return assert.ObjectsAreEqual(want, f.Commands())
	}, time.Second, 5*time.Millisecond, "commands: %v", f.Commands())
}

func TestManager_CommandRunner_RunAsMasterNotReady(t *testing.T) {
	f := newFakeRedis(t)
	m := newTestManager(t, f.Addr())
	m.state.Ready = false
	done := startRunner(m)
	defer stopRunner(m, done)

	m.commandCh <- NewCommand(RunAsMasterCommand, state.Consul{})
	// The channel is buffered, wait until the runner has taken the command.
	assert.Eventually(t, func() bool { return len(m.commandCh) == 0 }, time.Second, 5*time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	assert.Empty(t, f.Commands())
}

func TestManager_CommandRunner_RunAsMasterReady(t *testing.T) {
	f := newFakeRedis(t)
	m := newTestManager(t, f.Addr())
	m.state.Ready = true
	done := startRunner(m)
	defer stopRunner(m, done)

	m.commandCh <- NewCommand(RunAsMasterCommand, state.Consul{})
	waitCommands(t, f, "REPLICAOF NO ONE")
}

func TestManager_CommandRunner_RunAsSlaveNotReady(t *testing.T) {
	f := newFakeRedis(t)
	m := newTestManager(t, f.Addr())
	m.state.Ready = false
	done := startRunner(m)
	defer stopRunner(m, done)

	m.commandCh <- NewCommand(RunAsSlaveCommand, state.Consul{MasterAddr: "10.0.0.2", MasterPort: 6380})
	assert.Eventually(t, func() bool { return len(m.commandCh) == 0 }, time.Second, 5*time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	assert.Empty(t, f.Commands())
}

func TestManager_CommandRunner_RunAsSlaveReady(t *testing.T) {
	f := newFakeRedis(t)
	m := newTestManager(t, f.Addr())
	m.state.Ready = true
	done := startRunner(m)
	defer stopRunner(m, done)

	m.commandCh <- NewCommand(RunAsSlaveCommand, state.Consul{MasterAddr: "10.0.0.2", MasterPort: 6380})
	waitCommands(t, f, "REPLICAOF 10.0.0.2 6380", "CLIENT KILL TYPE normal")
}

func TestManager_CommandRunner_RunAsSlaveFails(t *testing.T) {
	f := newFakeRedis(t)
	f.SetFailReplicaOf("ERR nope")
	m := newTestManager(t, f.Addr())
	m.state.Ready = true
	done := startRunner(m)
	defer stopRunner(m, done)

	m.commandCh <- NewCommand(RunAsSlaveCommand, state.Consul{MasterAddr: "10.0.0.2", MasterPort: 6380})
	waitCommands(t, f, "REPLICAOF 10.0.0.2 6380")
	time.Sleep(30 * time.Millisecond)
	assert.Equal(t, []string{"REPLICAOF 10.0.0.2 6380"}, f.Commands(), "no CLIENT KILL after failure")
}

func TestManager_CommandRunner_Stop(t *testing.T) {
	f := newFakeRedis(t)
	m := newTestManager(t, f.Addr())
	done := startRunner(m)

	m.commandCh <- NewCommand(StopCommand, state.Consul{})

	select {
	case s := <-m.stateCh:
		assert.True(t, s.Stopped)
	case <-time.After(time.Second):
		t.Fatal("no state emitted")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runner did not return")
	}
	select {
	case _, ok := <-m.stopCh:
		assert.False(t, ok, "stopCh must be closed")
	default:
		t.Fatal("stopCh is not closed")
	}
}

func TestManager_CommandRunner_StopTwice(t *testing.T) {
	// The first StopCommand closes stopCh. The second one can reach the
	// select at the same time. cleanup() must not close stopCh twice.
	f := newFakeRedis(t)
	m := newTestManager(t, f.Addr())
	m.stateCh = make(chan state.Redis, 10)
	done := startRunner(m)

	m.commandCh <- NewCommand(StopCommand, state.Consul{})
	m.commandCh <- NewCommand(StopCommand, state.Consul{})

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runner did not return")
	}
}

func TestManager_CommandRunner_StopChannelClosedExternally(t *testing.T) {
	f := newFakeRedis(t)
	m := newTestManager(t, f.Addr())
	done := startRunner(m)
	close(m.stopCh)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runner did not return")
	}
}

func TestManager_Accessors(t *testing.T) {
	m := newTestManager(t, "127.0.0.1:1")
	assert.Equal(t, "127.0.0.1:1", m.Config().Address)
	assert.NotNil(t, m.StateChReader())
	assert.NotNil(t, m.CommandChWriter())

	c := NewCommand(RunAsSlaveCommand, state.Consul{})
	assert.Equal(t, RunAsSlaveCommand, c.Name())
	assert.Equal(t, "run_as_slave", c.String())
}

func TestManager_runAsMaster_Error(t *testing.T) {
	f := newFakeRedis(t)
	f.SetFailAll("ERR boom")
	m := newTestManager(t, f.Addr())
	assert.Error(t, m.runAsMaster())
	assert.Error(t, m.runAsSlave("10.0.0.1", 1))
	assert.Error(t, m.disconnectUsers())
}

func stopRunner(m *Manager, done <-chan struct{}) {
	close(m.stopCh)
	<-done
}

func TestManager_Close(t *testing.T) {
	f := newFakeRedis(t)
	m := newTestManager(t, f.Addr())
	done := startRunner(m)

	m.Close()
	assert.NotPanics(t, m.Close)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runner did not return")
	}
}

func TestManager_RunAsMaster_WithWatcher(t *testing.T) {
	// CommandRunner changes the role while watchStatus logs. A second
	// watchStatus call reads m.logger at once. Run this test with -race.
	f := newFakeRedis(t)
	f.SetInfo("role:slave\r\n")
	m := newTestManager(t, f.Addr())
	done := startRunner(m)
	defer stopRunner(m, done)

	go m.watchStatus()
	assert.True(t, nextState(t, m).Ready)

	m.commandCh <- NewCommand(RunAsMasterCommand, state.Consul{})
	// Keep reading m.logger until the runner has sent REPLICAOF.
	deadline := time.Now().Add(time.Second)
	for !slices.Contains(f.Commands(), "REPLICAOF NO ONE") && time.Now().Before(deadline) {
		m.watchStatus() // returns at once, logs "already running"
	}
	assert.Contains(t, f.Commands(), "REPLICAOF NO ONE")
}
