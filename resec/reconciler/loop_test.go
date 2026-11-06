package reconciler

import (
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/aireuropa/resec/resec/consul"
	"github.com/aireuropa/resec/resec/redis"
	"github.com/aireuropa/resec/resec/state"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

// runStateReader starts stateReader and returns a channel closed on return.
func runStateReader(r *Reconciler) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.stateReader()
	}()
	return done
}

func waitDone(t *testing.T, done <-chan struct{}, d time.Duration) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatal("did not return in time")
	}
}

func reconcileCount(r *Reconciler) int { return len(r.reconcileCh) }

// settle sends a sentinel Consul state and waits for its reconcile signal.
// The stateReader is one goroutine, so everything sent before is processed.
func settle(t *testing.T, h *helper, sentinel state.Consul) {
	t.Helper()
	h.consulStateCh <- sentinel
	assert.Eventually(t, func() bool { return reconcileCount(h.reconciler) > 0 }, time.Second, time.Millisecond)
}

func TestReconciler_stateReader_RedisChangeTriggersReconcile(t *testing.T) {
	h := newTestReconciler(t)
	done := runStateReader(h.reconciler)
	defer func() { close(h.reconciler.stopCh); waitDone(t, done, time.Second) }()

	h.redisStateCh <- state.Redis{Ready: true}

	select {
	case <-h.reconciler.reconcileCh:
	case <-time.After(time.Second):
		t.Fatal("no reconcile")
	}
	h.reconciler.Lock()
	assert.True(t, h.reconciler.redisState.Ready)
	h.reconciler.Unlock()
}

func TestReconciler_stateReader_SameRedisStateNoReconcile(t *testing.T) {
	h := newTestReconciler(t)
	done := runStateReader(h.reconciler)
	defer func() { close(h.reconciler.stopCh); waitDone(t, done, time.Second) }()

	h.redisStateCh <- state.Redis{Ready: true}
	h.redisStateCh <- state.Redis{Ready: true}
	settle(t, h, state.Consul{Ready: true})

	// one for the redis change, one for the sentinel
	assert.Equal(t, 2, reconcileCount(h.reconciler))
}

func TestReconciler_stateReader_InfoStringIgnored(t *testing.T) {
	h := newTestReconciler(t)
	done := runStateReader(h.reconciler)
	defer func() { close(h.reconciler.stopCh); waitDone(t, done, time.Second) }()

	h.redisStateCh <- state.Redis{Ready: true, InfoString: "a"}
	h.redisStateCh <- state.Redis{Ready: true, InfoString: "b"}
	settle(t, h, state.Consul{Ready: true})

	assert.Equal(t, 2, reconcileCount(h.reconciler))
}

func TestReconciler_stateReader_ConsulChange(t *testing.T) {
	h := newTestReconciler(t)
	done := runStateReader(h.reconciler)
	defer func() { close(h.reconciler.stopCh); waitDone(t, done, time.Second) }()

	h.consulStateCh <- state.Consul{Ready: true}

	select {
	case <-h.reconciler.reconcileCh:
	case <-time.After(time.Second):
		t.Fatal("no reconcile")
	}
	h.reconciler.Lock()
	assert.True(t, h.reconciler.consulState.Ready)
	h.reconciler.Unlock()
}

func TestReconciler_stateReader_Returns(t *testing.T) {
	t.Run("redis channel closed", func(t *testing.T) {
		h := newTestReconciler(t)
		done := runStateReader(h.reconciler)
		close(h.redisStateCh)
		waitDone(t, done, 100*time.Millisecond)
	})
	t.Run("consul channel closed", func(t *testing.T) {
		h := newTestReconciler(t)
		done := runStateReader(h.reconciler)
		close(h.consulStateCh)
		waitDone(t, done, 100*time.Millisecond)
	})
	t.Run("stop", func(t *testing.T) {
		h := newTestReconciler(t)
		done := runStateReader(h.reconciler)
		close(h.reconciler.stopCh)
		waitDone(t, done, 100*time.Millisecond)
	})
}

// startRun runs Run with a short reconcile interval and returns a channel
// closed when Run returns.
func startRun(h *helper, interval time.Duration) <-chan struct{} {
	h.reconciler.forceReconcileInterval = interval
	h.consume()
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.reconciler.Run()
	}()
	return done
}

func countConsul(h *helper, name consul.CommandName) int {
	n := 0
	for _, c := range h.seenConsul() {
		if c == name {
			n++
		}
	}
	return n
}

func TestReconciler_Run_StartCommands(t *testing.T) {
	h := newTestReconciler(t)
	startRun(h, time.Hour)
	defer close(h.reconciler.stopCh)

	assert.Eventually(t, func() bool {
		return len(h.seenConsul()) > 0 && len(h.seenRedis()) > 0
	}, time.Second, time.Millisecond)
	assert.Equal(t, consul.StartCommand, h.seenConsul()[0])
	assert.Equal(t, redis.StartCommand, h.seenRedis()[0])
}

func TestReconciler_Run_PeriodicReconcile(t *testing.T) {
	// Known limit: Run debounces reconciles with a trailing 100 ms timer. A
	// forceReconcileInterval below 100 ms keeps resetting the timer and the
	// reconcile never runs. The work order asks for 10 ms. This test uses
	// 120 ms and a longer wait to avoid starvation.
	h := newTestReconciler(t)
	h.reconciler.consulState = state.Consul{Ready: true, Healthy: true, Master: true}
	h.reconciler.redisState = state.Redis{Ready: true, Healthy: true, Info: state.RedisStatus{Role: "master"}}
	startRun(h, 120*time.Millisecond)
	defer close(h.reconciler.stopCh)

	assert.Eventually(t, func() bool {
		return countConsul(h, consul.UpdateServiceCommand) >= 2
	}, 3*time.Second, 10*time.Millisecond)
}

func TestReconciler_Run_PeriodicReconcileStarvesBelowDebounce(t *testing.T) {
	// Documents the starvation described above, with the 10 ms interval from
	// the work order.
	h := newTestReconciler(t)
	h.reconciler.consulState = state.Consul{Ready: true, Healthy: true, Master: true}
	h.reconciler.redisState = state.Redis{Ready: true, Healthy: true, Info: state.RedisStatus{Role: "master"}}
	startRun(h, 10*time.Millisecond)
	defer close(h.reconciler.stopCh)

	time.Sleep(50 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 0, countConsul(h, consul.UpdateServiceCommand))
}

func TestReconciler_Run_Debounce(t *testing.T) {
	h := newTestReconciler(t)
	h.reconciler.consulState = state.Consul{Ready: true, Healthy: true, Master: true}
	h.reconciler.redisState = state.Redis{Ready: true, Healthy: true, Info: state.RedisStatus{Role: "master"}}
	startRun(h, time.Hour)
	defer close(h.reconciler.stopCh)

	for i := 0; i < 10; i++ {
		h.reconciler.reconcileCh <- true
	}

	assert.Eventually(t, func() bool { return countConsul(h, consul.UpdateServiceCommand) >= 1 }, time.Second, 5*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	assert.LessOrEqual(t, countConsul(h, consul.UpdateServiceCommand), 2)
}

func TestReconciler_Run_SignalStop(t *testing.T) {
	h := newTestReconciler(t)
	done := startRun(h, time.Hour)

	h.reconciler.signalCh <- syscall.SIGTERM

	assert.Eventually(t, func() bool {
		return countConsul(h, consul.StopConsulCommand) == 1 && len(h.seenRedis()) >= 2
	}, time.Second, 5*time.Millisecond)
	assert.Contains(t, h.seenRedis(), redis.StopCommand)

	h.redisStateCh <- state.Redis{Stopped: true}
	h.consulStateCh <- state.Consul{Stopped: true}

	waitDone(t, done, time.Second)
	select {
	case _, ok := <-h.reconciler.stopCh:
		assert.False(t, ok, "stopCh must be closed")
	default:
		t.Error("stopCh not closed")
	}
}

func TestReconciler_stop_Direct(t *testing.T) {
	h := newTestReconciler(t)
	h.consume()
	r := h.reconciler
	r.stateListenAddress = "127.0.0.1:0"
	r.httpServer = r.newStateServer() // never started, Shutdown must still work

	done := make(chan struct{})
	go func() {
		defer close(done)
		r.stop()
	}()

	assert.Eventually(t, func() bool {
		return countConsul(h, consul.StopConsulCommand) == 1 && len(h.seenRedis()) == 1
	}, time.Second, 5*time.Millisecond)
	assert.Equal(t, []redis.CommandName{redis.StopCommand}, h.seenRedis())

	// stop() polls every second
	r.Lock()
	r.redisState.Stopped = true
	r.consulState.Stopped = true
	r.Unlock()

	waitDone(t, done, 3*time.Second)
	select {
	case _, ok := <-r.stopCh:
		assert.False(t, ok)
	default:
		t.Error("stopCh not closed")
	}
}

// globalHook captures log entries of the standard logger.
func globalHook(t *testing.T) *logtest.Hook {
	t.Helper()
	hook := logtest.NewGlobal()
	t.Cleanup(func() { log.StandardLogger().ReplaceHooks(make(log.LevelHooks)) })
	return hook
}

func dumpEntries(hook *logtest.Hook, kind string) int {
	n := 0
	for _, e := range hook.AllEntries() {
		if e.Level == log.WarnLevel && e.Data["dump_state"] == kind {
			n++
		}
	}
	return n
}

func TestReconciler_Run_SIGUSR1(t *testing.T) {
	hook := globalHook(t)
	h := newTestReconciler(t)
	startRun(h, time.Hour)
	defer close(h.reconciler.stopCh)

	h.reconciler.debugSignalCh <- syscall.SIGUSR1

	assert.Eventually(t, func() bool {
		return dumpEntries(hook, "consul") == 1 && dumpEntries(hook, "redis") == 1
	}, time.Second, 5*time.Millisecond)
}

func TestReconciler_Run_SIGUSR2(t *testing.T) {
	hook := globalHook(t)
	h := newTestReconciler(t)
	startRun(h, time.Hour)
	defer close(h.reconciler.stopCh)

	h.reconciler.debugSignalCh <- syscall.SIGUSR2

	assert.Eventually(t, func() bool { return dumpEntries(hook, "reconciler") == 1 }, time.Second, 5*time.Millisecond)
}

func TestReconciler_Run_IgnoresOtherDebugSignal(t *testing.T) {
	hook := globalHook(t)
	h := newTestReconciler(t)
	startRun(h, time.Hour)
	defer close(h.reconciler.stopCh)

	h.reconciler.debugSignalCh <- os.Interrupt
	time.Sleep(30 * time.Millisecond)
	assert.Equal(t, 0, dumpEntries(hook, "consul")+dumpEntries(hook, "redis")+dumpEntries(hook, "reconciler"))
}
