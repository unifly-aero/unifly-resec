package main

import (
	"io"
	"sort"
	"testing"
	"time"

	gelf "github.com/seatgeek/logrus-gelf-formatter"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/urfave/cli/v2"
)

// restoreLogger puts the logrus formatter, level and output back after a test.
func restoreLogger(t *testing.T) {
	t.Helper()
	formatter, level, out := log.StandardLogger().Formatter, log.GetLevel(), log.StandardLogger().Out
	t.Cleanup(func() {
		log.SetFormatter(formatter)
		log.SetLevel(level)
		log.SetOutput(out)
	})
	log.SetOutput(io.Discard)
}

// runApp runs newApp with a capture action and returns the captured context.
func runApp(t *testing.T, args ...string) (*cli.App, *cli.Context, error) {
	t.Helper()
	restoreLogger(t)

	app := newApp()
	app.Writer, app.ErrWriter = io.Discard, io.Discard
	var captured *cli.Context
	app.Action = func(c *cli.Context) error {
		captured = c
		return nil
	}
	err := app.Run(append([]string{"resec"}, args...))
	return app, captured, err
}

func TestNewApp_FlagDefaults(t *testing.T) {
	_, c, err := runApp(t)
	if !assert.NoError(t, err) || !assert.NotNil(t, c) {
		return
	}

	assert.Equal(t, 72*time.Hour, c.Duration("consul-deregister-service-after"))
	assert.Equal(t, "resec/.lock", c.String("consul-lock-key"))
	assert.Equal(t, 30, c.Int("consul-lock-monitor-retries"))
	assert.Equal(t, time.Second, c.Duration("consul-lock-monitor-retry-interval"))
	assert.Equal(t, "resec", c.String("consul-lock-session-name"))
	assert.Equal(t, 15*time.Second, c.Duration("consul-lock-ttl"))
	assert.Equal(t, "redis", c.String("consul-service-prefix"))
	assert.Equal(t, 2*time.Second, c.Duration("health-consul-timeout"))
	assert.Equal(t, 1, c.Int("health-consul-failures"))
	assert.Equal(t, 5*time.Second, c.Duration("healthcheck-interval"))
	assert.Equal(t, 2*time.Second, c.Duration("healthcheck-timeout"))
	assert.Equal(t, "info", c.String("log-level"))
	assert.Equal(t, "text", c.String("log-format"))
	assert.Equal(t, "127.0.0.1:6379", c.String("redis-addr"))
	assert.False(t, c.Bool("state-server"))
	assert.Equal(t, "0.0.0.0:8080", c.String("state-listen-addr"))
}

func TestNewApp_EnvOverride(t *testing.T) {
	t.Setenv("CONSUL_LOCK_KEY", "x/.lock")
	_, c, err := runApp(t)
	if assert.NoError(t, err) {
		assert.Equal(t, "x/.lock", c.String("consul-lock-key"))
	}
}

func TestNewApp_FlagsSorted(t *testing.T) {
	app := newApp()
	names := make([]string, len(app.Flags))
	for i, f := range app.Flags {
		names[i] = f.Names()[0]
	}
	assert.True(t, sort.StringsAreSorted(names), "flags: %v", names)
}

func TestNewApp_Version(t *testing.T) {
	app := newApp()
	assert.Equal(t, Version, app.Version)
	assert.Equal(t, "resec", app.Name)
}

func TestNewApp_LogLevelBad(t *testing.T) {
	restoreLogger(t)
	app := newApp()
	app.Writer, app.ErrWriter = io.Discard, io.Discard
	err := app.Run([]string{"resec", "--log-level", "nope"})
	assert.Error(t, err)
}

func TestNewApp_LogLevelApplied(t *testing.T) {
	_, _, err := runApp(t, "--log-level", "debug")
	assert.NoError(t, err)
	assert.Equal(t, log.DebugLevel, log.GetLevel())
}

func TestNewApp_LogFormatBad(t *testing.T) {
	restoreLogger(t)
	app := newApp()
	app.Writer, app.ErrWriter = io.Discard, io.Discard
	err := app.Run([]string{"resec", "--log-format", "yaml"})
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "yaml")
	}
}

func TestNewApp_LogFormatText(t *testing.T) {
	_, _, err := runApp(t, "--log-format", "text")
	assert.NoError(t, err)
	assert.IsType(t, &log.TextFormatter{}, log.StandardLogger().Formatter)
}

func TestNewApp_LogFormatJSON(t *testing.T) {
	_, _, err := runApp(t, "--log-format", "json")
	assert.NoError(t, err)
	assert.IsType(t, &log.JSONFormatter{}, log.StandardLogger().Formatter)
}

func TestNewApp_LogFormatGelf(t *testing.T) {
	_, _, err := runApp(t, "--log-format", "gelf")
	assert.NoError(t, err)
	assert.IsType(t, &gelf.GelfFormatter{}, log.StandardLogger().Formatter)
}
