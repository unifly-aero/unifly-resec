//go:build integration

// Package integration runs two resec reconcilers against one real Consul agent
// and two real Redis servers.
//
// Run with: RESEC_INTEGRATION=1 go test -tags integration -race -count=1 ./resec/integration/...
//
// The test starts the binaries `consul` and `redis-server` from PATH
// (testcontainers is not a dependency of this module). It skips when
// RESEC_INTEGRATION is not 1 or when a binary is missing.
//
// Differences from the work order:
//   - redis-server runs on a real port (--port 0 disables TCP).
//   - The state server port is picked first, because --state-listen-addr
//     127.0.0.1:0 does not tell the test which port was chosen.
//   - The tests run in the order election, replication, multi-master,
//     failover, health. The health test needs the one resec that is still
//     running after the failover test.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/aireuropa/resec/resec/internal/clitest"
	"github.com/aireuropa/resec/resec/reconciler"
	consulapi "github.com/hashicorp/consul/api"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// startProcess starts a binary and stops it at the end of the test.
func startProcess(t *testing.T, name string, args ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(name, args...)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { stopProcess(cmd) })
	return cmd
}

func stopProcess(cmd *exec.Cmd) {
	if cmd.Process == nil || cmd.ProcessState != nil {
		return
	}
	_ = cmd.Process.Signal(syscall.SIGTERM)
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = cmd.Process.Kill()
		<-done
	}
}

func startConsul(t *testing.T) (addr string, cmd *exec.Cmd) {
	t.Helper()
	httpPort := freePort(t)
	cfg := map[string]any{"ports": map[string]int{
		"http": httpPort, "dns": -1, "grpc": -1, "https": -1,
		"serf_lan": freePort(t), "serf_wan": -1, "server": freePort(t),
	}}
	dir := t.TempDir()
	cfgFile := filepath.Join(dir, "consul.json")
	b, _ := json.Marshal(cfg)
	require.NoError(t, os.WriteFile(cfgFile, b, 0o600))

	cmd = startProcess(t, "consul", "agent", "-dev", "-node=itest",
		"-bind=127.0.0.1", "-client=127.0.0.1", "-config-file="+cfgFile)
	addr = fmt.Sprintf("127.0.0.1:%d", httpPort)

	client, err := consulapi.NewClient(&consulapi.Config{Address: addr})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		leader, err := client.Status().Leader()
		return err == nil && leader != ""
	}, 30*time.Second, 200*time.Millisecond, "consul has no leader")
	return addr, cmd
}

func startRedis(t *testing.T) (addr string) {
	t.Helper()
	port := freePort(t)
	startProcess(t, "redis-server", "--port", strconv.Itoa(port), "--bind", "127.0.0.1",
		"--save", "", "--appendonly", "no", "--dir", t.TempDir())
	addr = fmt.Sprintf("127.0.0.1:%d", port)

	c := goredis.NewClient(&goredis.Options{Addr: addr})
	t.Cleanup(func() { c.Close() })
	require.Eventually(t, func() bool { return c.Ping(context.Background()).Err() == nil },
		10*time.Second, 100*time.Millisecond, "redis does not answer")
	return addr
}

func redisRole(addr string) string {
	c := goredis.NewClient(&goredis.Options{Addr: addr, DialTimeout: time.Second, ReadTimeout: time.Second})
	defer c.Close()
	info, err := c.Info(context.Background(), "replication").Result()
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(info, "\r\n") {
		if strings.HasPrefix(line, "role:") {
			return strings.TrimPrefix(line, "role:")
		}
	}
	return ""
}

type node struct {
	redisAddr string
	stateAddr string
	r         *reconciler.Reconciler
	signalCh  chan os.Signal
	stopped   chan struct{}
}

// signalChannel reads the unexported signalCh of a Reconciler.
func signalChannel(r *reconciler.Reconciler) chan os.Signal {
	f := reflect.ValueOf(r).Elem().FieldByName("signalCh")
	f = reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem()
	return f.Interface().(chan os.Signal)
}

func startResec(t *testing.T, redisAddr string) *node {
	t.Helper()
	stateAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	ctx := clitest.NewContext(t,
		"--redis-addr", redisAddr,
		"--announce-addr", redisAddr,
		"--consul-lock-ttl", "15s",
		"--healthcheck-timeout", "1s",
		"--state-server", "--state-listen-addr", stateAddr,
	)
	r, err := reconciler.NewReconciler(ctx)
	require.NoError(t, err)

	n := &node{redisAddr: redisAddr, stateAddr: stateAddr, r: r, signalCh: signalChannel(r), stopped: make(chan struct{})}
	go func() {
		defer close(n.stopped)
		r.Run()
	}()
	return n
}

func passing(t *testing.T, client *consulapi.Client, service string) []*consulapi.ServiceEntry {
	t.Helper()
	entries, _, err := client.Health().Service(service, "", true, nil)
	if err != nil {
		return nil
	}
	return entries
}

func httpStatus(url string) int {
	c := http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get(url)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	return resp.StatusCode
}

func TestIntegration(t *testing.T) {
	if os.Getenv("RESEC_INTEGRATION") != "1" {
		t.Skip("set RESEC_INTEGRATION=1 to run")
	}
	for _, bin := range []string{"consul", "redis-server"} {
		if _, err := exec.LookPath(bin); err != nil {
			t.Skipf("%s not in PATH", bin)
		}
	}

	consulAddr, consulCmd := startConsul(t)
	t.Setenv("CONSUL_HTTP_ADDR", consulAddr)
	client, err := consulapi.NewClient(&consulapi.Config{Address: consulAddr})
	require.NoError(t, err)

	redis1, redis2 := startRedis(t), startRedis(t)
	n1, n2 := startResec(t, redis1), startResec(t, redis2)
	nodes := []*node{n1, n2}

	var master, slave *node

	t.Run("election", func(t *testing.T) {
		require.Eventually(t, func() bool {
			r1, r2 := redisRole(redis1), redisRole(redis2)
			return (r1 == "master") != (r2 == "master")
		}, 20*time.Second, 250*time.Millisecond, "exactly one master")

		require.Eventually(t, func() bool {
			return len(passing(t, client, "redis-master")) == 1 && len(passing(t, client, "redis-slave")) == 1
		}, 20*time.Second, 250*time.Millisecond, "one passing master and one passing slave service")

		for _, n := range nodes {
			if redisRole(n.redisAddr) == "master" {
				master = n
			} else {
				slave = n
			}
		}
		require.NotNil(t, master)
		require.NotNil(t, slave)
	})
	if master == nil {
		t.FailNow()
	}

	t.Run("replication", func(t *testing.T) {
		mc := goredis.NewClient(&goredis.Options{Addr: master.redisAddr})
		sc := goredis.NewClient(&goredis.Options{Addr: slave.redisAddr})
		defer mc.Close()
		defer sc.Close()

		require.NoError(t, mc.Set(context.Background(), "itest", "v1", 0).Err())
		assert.Eventually(t, func() bool {
			v, err := sc.Get(context.Background(), "itest").Result()
			return err == nil && v == "v1"
		}, 5*time.Second, 100*time.Millisecond)
	})

	t.Run("multi-master", func(t *testing.T) {
		// A catalog-only service on a fake node. The local agent does not
		// sync it back, so the removal is final.
		_, err := client.Catalog().Register(&consulapi.CatalogRegistration{
			Node:    "ghost",
			Address: "10.9.9.9",
			Service: &consulapi.AgentService{ID: "redis@10.9.9.9:6379", Service: "redis-master", Port: 6379},
			Check: &consulapi.AgentCheck{
				Node: "ghost", CheckID: "ghost-check", Name: "ghost",
				Status: consulapi.HealthPassing, ServiceID: "redis@10.9.9.9:6379",
			},
		}, nil)
		require.NoError(t, err)

		assert.Eventually(t, func() bool {
			for _, e := range passing(t, client, "redis-master") {
				if e.Node.Node == "ghost" {
					return false
				}
			}
			return true
		}, 10*time.Second, 250*time.Millisecond, "lock holder removes the foreign master service")
		assert.Len(t, passing(t, client, "redis-master"), 1)
	})

	t.Run("failover", func(t *testing.T) {
		master.signalCh <- syscall.SIGTERM
		select {
		case <-master.stopped:
		case <-time.After(70 * time.Second):
			t.Fatal("master resec did not stop")
		}

		// TTL 15s + lock delay 15s + margin
		assert.Eventually(t, func() bool { return redisRole(slave.redisAddr) == "master" },
			45*time.Second, 500*time.Millisecond, "other redis becomes master")
	})

	t.Run("health", func(t *testing.T) {
		url := "http://" + slave.stateAddr + "/health"
		assert.Eventually(t, func() bool { return httpStatus(url) == 200 }, 10*time.Second, 250*time.Millisecond)

		stopProcess(consulCmd)
		assert.Eventually(t, func() bool { return httpStatus(url) == 503 }, 10*time.Second, 250*time.Millisecond)
	})
}
