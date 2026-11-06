// +test
package reconciler

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/aireuropa/resec/resec/consul"
	"github.com/aireuropa/resec/resec/redis"
	"github.com/aireuropa/resec/resec/state"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

type helper struct {
	consulCommandCh        chan consul.Command
	consulCommandsExpected []consul.CommandName
	consulCommandsSeen     []consul.CommandName
	consulStateCh          chan state.Consul
	reconciler             *Reconciler
	redisCommandCh         chan redis.Command
	redisCommandsExpected  []redis.CommandName
	redisCommandsSeen      []redis.CommandName
	redisCommandsFull      []redis.Command  // full payloads, never reset
	consulCommandsFull     []consul.Command // full payloads, never reset
	redisStateCh           chan state.Redis
	t                      *testing.T
	mu                     sync.Mutex // protects the *CommandsSeen slices
}

// consume all consul/redis commands from the reconciler
func (h *helper) consume() {
	go func() {
		for {
			select {
			case <-h.reconciler.stopCh:
				return
			case cmd := <-h.redisCommandCh:
				h.mu.Lock()
				h.redisCommandsSeen = append(h.redisCommandsSeen, cmd.Name())
				h.redisCommandsFull = append(h.redisCommandsFull, cmd)
				h.mu.Unlock()
			case cmd := <-h.consulCommandCh:
				h.mu.Lock()
				h.consulCommandsSeen = append(h.consulCommandsSeen, cmd.Name())
				h.consulCommandsFull = append(h.consulCommandsFull, cmd)
				h.mu.Unlock()
			}
		}
	}()
}

// withConsulState will change the reconcilers Consul state
func (h *helper) withConsulState(consulState state.Consul) *helper {
	h.reconciler.consulState = consulState
	return h
}

// withRedisState will change the reconcilers Redis state
func (h *helper) withRedisState(redisState state.Redis) *helper {
	h.reconciler.redisState = redisState
	return h
}

// expectRedisCommands sets the expected Redis commands we should see
// from the reconciler during an evaluation
func (h *helper) expectRedisCommands(commands ...redis.CommandName) *helper {
	h.redisCommandsExpected = commands
	return h
}

// expectConsulCommands sets the expected Consul commands we should see
// from the reconciler during an evaluation
func (h *helper) expectConsulCommands(commands ...consul.CommandName) *helper {
	h.consulCommandsExpected = commands
	return h
}

// eval will trigger a reconciler evaluation and assert the result matches the
// configured expectations (expectRedisCommands / expectConsulCommands)
func (h *helper) eval(expectedResult resultType) {
	actualResult := h.reconciler.evaluate()
	h.reconciler.apply(actualResult)

	assert.EqualValues(h.t, string(expectedResult), string(actualResult))
	time.Sleep(5 * time.Millisecond)

	h.mu.Lock()
	assert.EqualValues(h.t, h.consulCommandsExpected, h.consulCommandsSeen)
	assert.EqualValues(h.t, h.redisCommandsExpected, h.redisCommandsSeen)
	h.mu.Unlock()

	h.reset()
}

// reset the internal assertions
func (h *helper) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.consulCommandsExpected = []consul.CommandName{}
	h.consulCommandsSeen = []consul.CommandName{}
	h.redisCommandsExpected = []redis.CommandName{}
	h.redisCommandsSeen = []redis.CommandName{}
}

// stop will stop the internal go-routines
func (h *helper) stop() {
	time.Sleep(5 * time.Millisecond)
	close(h.reconciler.stopCh)
}

// newTestReconciler will create a new reconciler for testing purposes
func newTestReconciler(t *testing.T) *helper {
	consulCommandCh := make(chan consul.Command, 1)
	consulStateCh := make(chan state.Consul, 1)

	redisCommandCh := make(chan redis.Command, 1)
	redisStateCh := make(chan state.Redis, 1)

	r := &Reconciler{
		consulCommandCh:        consulCommandCh,
		consulStateCh:          consulStateCh,
		forceReconcileInterval: 5 * time.Millisecond,
		redisCommandCh:         redisCommandCh,
		redisStateCh:           redisStateCh,
		signalCh:               make(chan os.Signal, 0),
		debugSignalCh:          make(chan os.Signal, 1),
		reconcileCh:            make(chan interface{}, 10),
		stopCh:                 make(chan interface{}, 0),
		logger:                 log.WithField("testing", true),
	}

	h := &helper{
		t:               t,
		reconciler:      r,
		consulCommandCh: consulCommandCh,
		consulStateCh:   consulStateCh,
		redisCommandCh:  redisCommandCh,
		redisStateCh:    redisStateCh,
	}

	h.reset()
	return h
}

// redisCommands returns all full Redis commands seen so far
func (h *helper) redisCommands() []redis.Command {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]redis.Command(nil), h.redisCommandsFull...)
}

// consulCommands returns all full Consul commands seen so far
func (h *helper) consulCommands() []consul.Command {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]consul.Command(nil), h.consulCommandsFull...)
}

// seenConsul returns the Consul command names seen so far
func (h *helper) seenConsul() []consul.CommandName {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]consul.CommandName(nil), h.consulCommandsSeen...)
}

// seenRedis returns the Redis command names seen so far
func (h *helper) seenRedis() []redis.CommandName {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]redis.CommandName(nil), h.redisCommandsSeen...)
}
