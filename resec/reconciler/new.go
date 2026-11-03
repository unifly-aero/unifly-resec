package reconciler

import (
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/aireuropa/resec/resec/consul"
	"github.com/aireuropa/resec/resec/redis"
	"github.com/urfave/cli/v2"
)

// setup returns the default configuration for the ReSeC
func NewReconciler(c *cli.Context) (*Reconciler, error) {
	redisConnection, err := redis.NewConnection(c)
	if err != nil {
		return nil, err
	}

	consulConnection, err := consul.NewConnection(c, redisConnection.Config())
	if err != nil {
		return nil, err
	}

	reconciler := &Reconciler{
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
