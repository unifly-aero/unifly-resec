package reconciler

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aireuropa/resec/resec/consul"
	"github.com/aireuropa/resec/resec/redis"
	"github.com/urfave/cli/v2"
)

// minReconcileInterval is the lowest allowed value of healthcheck-timeout.
const minReconcileInterval = 500 * time.Millisecond

// setup returns the default configuration for the ReSeC
func NewReconciler(c *cli.Context) (*Reconciler, error) {
	// The debounce timer restarts on each event. A short interval can starve
	// the reconcile loop. Reject it at start.
	if interval := c.Duration("healthcheck-timeout"); interval < minReconcileInterval {
		return nil, fmt.Errorf("healthcheck-timeout (%s) must be at least %s", interval, minReconcileInterval)
	}

	redisConnection, err := redis.NewConnection(c)
	if err != nil {
		return nil, err
	}

	consulConnection, err := consul.NewConnection(c, redisConnection.Config())
	if err != nil {
		return nil, err
	}

	reconciler := &Reconciler{
		consulManager:          consulConnection,
		redisManager:           redisConnection,
		consulCommandCh:        consulConnection.GetCommandWriter(),
		consulStateCh:          consulConnection.GetStateReader(),
		debugSignalCh:          make(chan os.Signal, 1),
		forceReconcileInterval: c.Duration("healthcheck-timeout"),
		reconcileCh:            make(chan interface{}, 10),
		redisCommandCh:         redisConnection.CommandChWriter(),
		redisStateCh:           redisConnection.StateChReader(),
		signalCh:               make(chan os.Signal, 1),
		stopCh:                 make(chan interface{}, 1),
		stateServerOn:          c.Bool("state-server"),
		stateListenAddress:     c.String("state-listen-addr"),
		consulHealth:           consulConnection.LockKeyHealthy,
		consulHealthTimeout:    c.Duration("health-consul-timeout"),
		consulHealthFailures:   c.Int("health-consul-failures"),
	}

	reconciler.announceHost, reconciler.announcePort = consulConnection.AnnounceAddr()
	if host, _, err := net.SplitHostPort(redisConnection.Config().Address); err == nil {
		reconciler.redisHost = host
	}

	if reconciler.consulHealthFailures < 1 {
		reconciler.consulHealthFailures = 1
	}

	signal.Notify(reconciler.signalCh, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT)
	signal.Notify(reconciler.debugSignalCh, syscall.SIGUSR1, syscall.SIGUSR2)

	go redisConnection.CommandRunner()
	go consulConnection.CommandRunner()

	return reconciler, nil
}
