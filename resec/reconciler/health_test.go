package reconciler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/aireuropa/resec/resec/state"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
)

type ctxKey struct{}

func TestReconciler_checkConsulHealth(t *testing.T) {
	errCheck := errors.New("consul down")

	t.Run("nil check", func(t *testing.T) {
		r := newTestReconciler(t).reconciler
		r.consulHealthFailures = 3
		assert.NoError(t, r.checkConsulHealth(context.Background()))
	})

	t.Run("single failure", func(t *testing.T) {
		r := newTestReconciler(t).reconciler
		r.consulHealthFailures = 1
		r.consulHealth = func(context.Context) error { return errCheck }
		assert.Equal(t, errCheck, r.checkConsulHealth(context.Background()))
	})

	t.Run("threshold 3, two failures", func(t *testing.T) {
		r := newTestReconciler(t).reconciler
		r.consulHealthFailures = 3
		r.consulHealth = func(context.Context) error { return errCheck }
		assert.NoError(t, r.checkConsulHealth(context.Background()))
		assert.NoError(t, r.checkConsulHealth(context.Background()))
		assert.Equal(t, 2, r.consulHealthFailed)
	})

	t.Run("threshold 3, third failure", func(t *testing.T) {
		r := newTestReconciler(t).reconciler
		r.consulHealthFailures = 3
		r.consulHealth = func(context.Context) error { return errCheck }
		assert.NoError(t, r.checkConsulHealth(context.Background()))
		assert.NoError(t, r.checkConsulHealth(context.Background()))
		assert.Equal(t, errCheck, r.checkConsulHealth(context.Background()))
	})

	t.Run("success resets", func(t *testing.T) {
		r := newTestReconciler(t).reconciler
		r.consulHealthFailures = 3
		var err error
		r.consulHealth = func(context.Context) error { return err }

		err = errCheck
		assert.NoError(t, r.checkConsulHealth(context.Background()))
		assert.NoError(t, r.checkConsulHealth(context.Background()))
		err = nil
		assert.NoError(t, r.checkConsulHealth(context.Background()))
		assert.Equal(t, 0, r.consulHealthFailed)
		err = errCheck
		assert.NoError(t, r.checkConsulHealth(context.Background()))
		assert.Equal(t, 1, r.consulHealthFailed)
	})

	t.Run("context passed", func(t *testing.T) {
		r := newTestReconciler(t).reconciler
		r.consulHealthFailures = 1
		var got context.Context
		r.consulHealth = func(ctx context.Context) error { got = ctx; return nil }
		ctx := context.WithValue(context.Background(), ctxKey{}, "v")
		assert.NoError(t, r.checkConsulHealth(ctx))
		assert.Equal(t, ctx, got)
	})
}

// stateServer starts the state server handler of a test reconciler.
func stateServer(t *testing.T) (*Reconciler, *httptest.Server) {
	t.Helper()
	r := newTestReconciler(t).reconciler
	r.consulHealthTimeout = 100 * time.Millisecond
	r.consulHealthFailures = 1
	srv := httptest.NewServer(r.newStateServer().Handler)
	t.Cleanup(srv.Close)
	return r, srv
}

func get(t *testing.T, url string) (int, string, http.Header) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), resp.Header
}

func TestStateServer_Overview(t *testing.T) {
	tests := []struct {
		name        string
		redis       state.Redis
		contains    []string
		notContains []string
	}{
		{
			name:     "ready master",
			redis:    state.Redis{Ready: true, Healthy: true, Info: state.RedisStatus{Role: "master"}},
			contains: []string{"Instance is ready", "./health"},
		},
		{
			name:     "loading",
			redis:    state.Redis{Ready: true, Healthy: true, Info: state.RedisStatus{Role: "master", Loading: true}},
			contains: []string{"loading data from disk"},
		},
		{
			name:     "slave syncing",
			redis:    state.Redis{Ready: true, Healthy: true, Info: state.RedisStatus{Role: "slave", MasterSyncInProgress: true, MasterLinkUp: true}},
			contains: []string{"syncing data from master"},
		},
		{
			name:     "slave link down",
			redis:    state.Redis{Ready: true, Healthy: true, Info: state.RedisStatus{Role: "slave", MasterLinkUp: false}},
			contains: []string{"Link to master is down"},
		},
		{
			name:        "master with link flag false",
			redis:       state.Redis{Ready: true, Healthy: true, Info: state.RedisStatus{Role: "master", MasterLinkUp: false}},
			notContains: []string{"Link to master is down"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r, srv := stateServer(t)
			r.redisState = tt.redis
			code, body, _ := get(t, srv.URL+"/")
			assert.Equal(t, 200, code)
			for _, s := range tt.contains {
				assert.Contains(t, body, s)
			}
			for _, s := range tt.notContains {
				assert.NotContains(t, body, s)
			}
		})
	}
}

func TestStateServer_NotFound(t *testing.T) {
	_, srv := stateServer(t)
	code, _, _ := get(t, srv.URL+"/nope")
	assert.Equal(t, 404, code)
}

func TestStateServer_State(t *testing.T) {
	r, srv := stateServer(t)
	r.consulState = state.Consul{MasterAddr: "10.0.0.1"}
	r.redisState = state.Redis{Info: state.RedisStatus{Role: "master"}}

	code, body, hdr := get(t, srv.URL+"/state")
	assert.Equal(t, 200, code)
	assert.Equal(t, "application/json", hdr.Get("Content-Type"))

	var m map[string]json.RawMessage
	if assert.NoError(t, json.Unmarshal([]byte(body), &m)) {
		assert.Contains(t, m, "consulState")
		assert.Contains(t, m, "redisState")
		assert.Contains(t, m, "forceReconcileInterval")
		var cs state.Consul
		assert.NoError(t, json.Unmarshal(m["consulState"], &cs))
		assert.Equal(t, "10.0.0.1", cs.MasterAddr)
	}
}

func TestStateServer_Info(t *testing.T) {
	r, srv := stateServer(t)
	r.redisState = state.Redis{InfoString: "role:master\r\n"}
	code, body, _ := get(t, srv.URL+"/info")
	assert.Equal(t, 200, code)
	assert.Equal(t, "role:master\r\n", body)
}

func TestStateServer_Health(t *testing.T) {
	master := state.Redis{Ready: true, Healthy: true, Info: state.RedisStatus{Role: "master"}}

	t.Run("no consul check, redis ready", func(t *testing.T) {
		r, srv := stateServer(t)
		r.redisState = master
		code, body, _ := get(t, srv.URL+"/health")
		assert.Equal(t, 200, code)
		assert.Equal(t, "ok", body)
	})

	t.Run("consul error", func(t *testing.T) {
		r, srv := stateServer(t)
		r.redisState = master
		r.consulHealth = func(context.Context) error { return errors.New("no leader") }
		code, body, _ := get(t, srv.URL+"/health")
		assert.Equal(t, 503, code)
		assert.Contains(t, body, "fail: consul:")
	})

	t.Run("redis not ready", func(t *testing.T) {
		r, srv := stateServer(t)
		r.consulHealth = func(context.Context) error { return nil }
		code, body, _ := get(t, srv.URL+"/health")
		assert.Equal(t, 503, code)
		assert.Equal(t, "fail: redis not ready", body)
	})

	t.Run("slave link up", func(t *testing.T) {
		r, srv := stateServer(t)
		r.redisState = state.Redis{Ready: true, Healthy: true, Info: state.RedisStatus{Role: "slave", MasterLinkUp: true}}
		code, _, _ := get(t, srv.URL+"/health")
		assert.Equal(t, 200, code)
	})

	t.Run("slave syncing", func(t *testing.T) {
		r, srv := stateServer(t)
		r.redisState = state.Redis{Ready: true, Healthy: true, Info: state.RedisStatus{Role: "slave", MasterLinkUp: true, MasterSyncInProgress: true}}
		code, _, _ := get(t, srv.URL+"/health")
		assert.Equal(t, 503, code)
	})

	t.Run("timeout", func(t *testing.T) {
		r, srv := stateServer(t)
		r.redisState = master
		r.consulHealth = func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}
		start := time.Now()
		code, body, _ := get(t, srv.URL+"/health")
		assert.Equal(t, 503, code)
		assert.Contains(t, body, "deadline")
		assert.Less(t, time.Since(start), 500*time.Millisecond)
	})

	t.Run("failure threshold", func(t *testing.T) {
		r, srv := stateServer(t)
		r.redisState = master
		r.consulHealthFailures = 2
		var mu sync.Mutex
		var checkErr = errors.New("down")
		r.consulHealth = func(context.Context) error {
			mu.Lock()
			defer mu.Unlock()
			return checkErr
		}

		code, _, _ := get(t, srv.URL+"/health")
		assert.Equal(t, 200, code)
		code, _, _ = get(t, srv.URL+"/health")
		assert.Equal(t, 503, code)

		mu.Lock()
		checkErr = nil
		mu.Unlock()
		code, _, _ = get(t, srv.URL+"/health")
		assert.Equal(t, 200, code)
	})
}

func TestStateServer_ServerSettings(t *testing.T) {
	r := newTestReconciler(t).reconciler
	r.stateListenAddress = "127.0.0.1:1234"
	s := r.newStateServer()
	assert.Equal(t, 5*time.Second, s.ReadHeaderTimeout)
	assert.Equal(t, "127.0.0.1:1234", s.Addr)
}

func TestReconciler_serveState_Closed(t *testing.T) {
	r := newTestReconciler(t).reconciler
	logger, hook := logtest.NewNullLogger()
	r.logger = logger.WithField("testing", true)
	r.stateListenAddress = "127.0.0.1:0"
	server := r.newStateServer()
	server.Close()

	done := make(chan struct{})
	go func() {
		r.serveState(server)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("serveState did not return")
	}
	for _, e := range hook.AllEntries() {
		assert.NotEqual(t, log.ErrorLevel, e.Level, e.Message)
	}
}

func TestReconciler_serveState_BadAddress(t *testing.T) {
	r := newTestReconciler(t).reconciler
	logger, hook := logtest.NewNullLogger()
	r.logger = logger.WithField("testing", true)
	r.stateListenAddress = "256.256.256.256:1"
	server := r.newStateServer()

	done := make(chan struct{})
	go func() {
		r.serveState(server)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("serveState did not return")
	}

	var errs []string
	for _, e := range hook.AllEntries() {
		if e.Level == log.ErrorLevel {
			errs = append(errs, e.Message)
		}
	}
	if assert.Len(t, errs, 1) {
		assert.Contains(t, errs[0], "State server stopped")
	}
}

func TestReconciler_MarshalJSON(t *testing.T) {
	r := newTestReconciler(t).reconciler
	b, err := r.MarshalJSON()
	assert.NoError(t, err)

	var m map[string]any
	if assert.NoError(t, json.Unmarshal(b, &m)) {
		assert.Len(t, m, 3)
		assert.Contains(t, m, "consulState")
		assert.Contains(t, m, "redisState")
		assert.Contains(t, m, "forceReconcileInterval")
	}
}

func TestReconciler_prettyPrint(t *testing.T) {
	r := newTestReconciler(t).reconciler

	out := r.prettyPrint(struct{ FieldName int }{1})
	assert.Contains(t, out, "\t")
	assert.Contains(t, out, "FieldName")

	logger, hook := logtest.NewNullLogger()
	r.logger = logger.WithField("testing", true)
	assert.Equal(t, "", r.prettyPrint(make(chan int)))
	if assert.Len(t, hook.AllEntries(), 1) {
		assert.Equal(t, log.ErrorLevel, hook.LastEntry().Level)
	}
}

func TestReconciler_diffState(t *testing.T) {
	r := newTestReconciler(t).reconciler

	a := state.Redis{Ready: true}
	assert.False(t, r.diffState(a, a))

	b := a
	b.Healthy = true
	assert.True(t, r.diffState(a, b))

	c := a
	c.InfoString = "different"
	assert.False(t, r.diffState(a, c))
}
