// Package clitest builds *cli.Context values for tests. The flag list is a
// copy of the flags in main.go. Keep both in sync.
package clitest

import (
	"io"
	"testing"
	"time"

	"github.com/urfave/cli/v2"
)

// Flags returns the same flags as the resec binary.
func Flags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{Name: "announce-addr", EnvVars: []string{"ANNOUNCE_ADDR"}},
		&cli.DurationFlag{Name: "consul-deregister-service-after", Value: 72 * time.Hour, EnvVars: []string{"CONSUL_DEREGISTER_SERVICE_AFTER"}},
		&cli.StringFlag{Name: "consul-lock-key", Value: "resec/.lock", EnvVars: []string{"CONSUL_LOCK_KEY"}},
		&cli.IntFlag{Name: "consul-lock-monitor-retries", Value: 30, EnvVars: []string{"CONSUL_LOCK_MONITOR_RETRIES"}},
		&cli.DurationFlag{Name: "consul-lock-monitor-retry-interval", Value: time.Second, EnvVars: []string{"CONSUL_LOCK_MONITOR_RETRY_INTERVAL"}},
		&cli.StringFlag{Name: "consul-lock-session-name", Value: "resec", EnvVars: []string{"CONSUL_LOCK_SESSION_NAME"}},
		&cli.DurationFlag{Name: "consul-lock-ttl", Value: 15 * time.Second, EnvVars: []string{"CONSUL_LOCK_TTL"}},
		&cli.StringFlag{Name: "consul-service-name", EnvVars: []string{"CONSUL_SERVICE_NAME"}},
		&cli.StringFlag{Name: "consul-service-prefix", Value: "redis", EnvVars: []string{"CONSUL_SERVICE_PREFIX"}},
		&cli.StringFlag{Name: "consul-master-tags", EnvVars: []string{"MASTER_TAGS"}},
		&cli.StringFlag{Name: "consul-slave-tags", EnvVars: []string{"SLAVE_TAGS"}},
		&cli.DurationFlag{Name: "health-consul-timeout", Value: 2 * time.Second, EnvVars: []string{"HEALTH_CONSUL_TIMEOUT"}},
		&cli.IntFlag{Name: "health-consul-failures", Value: 1, EnvVars: []string{"HEALTH_CONSUL_FAILURES"}},
		&cli.DurationFlag{Name: "healthcheck-interval", Value: 5 * time.Second, EnvVars: []string{"HEALTHCHECK_INTERVAL"}},
		&cli.DurationFlag{Name: "healthcheck-timeout", Value: 2 * time.Second, EnvVars: []string{"HEALTHCHECK_TIMEOUT"}},
		&cli.StringFlag{Name: "log-level", Value: "info", EnvVars: []string{"LOG_LEVEL"}},
		&cli.StringFlag{Name: "log-format", Value: "text", EnvVars: []string{"LOG_FORMAT"}},
		&cli.StringFlag{Name: "redis-addr", Value: "127.0.0.1:6379", EnvVars: []string{"REDIS_ADDR"}},
		&cli.StringFlag{Name: "redis-password", EnvVars: []string{"REDIS_PASSWORD"}},
		&cli.BoolFlag{Name: "state-server", Value: false, EnvVars: []string{"STATE_SERVER"}, Aliases: []string{"s"}},
		&cli.StringFlag{Name: "state-listen-addr", Value: "0.0.0.0:8080", EnvVars: []string{"STATE_LISTEN_ADDR"}},
	}
}

// NewContext parses args like the resec binary and returns the context that
// app.Action receives. The program name is added in front of args.
func NewContext(t testing.TB, args ...string) *cli.Context {
	t.Helper()

	var captured *cli.Context
	app := &cli.App{
		Name:      "resec",
		Flags:     Flags(),
		Writer:    io.Discard,
		ErrWriter: io.Discard,
		Action: func(c *cli.Context) error {
			captured = c
			return nil
		},
	}

	if err := app.Run(append([]string{"resec"}, args...)); err != nil {
		t.Fatalf("clitest: parse %v: %v", args, err)
	}
	if captured == nil {
		t.Fatalf("clitest: action did not run for %v", args)
	}
	return captured
}
