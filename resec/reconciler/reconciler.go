package reconciler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/aireuropa/resec/resec/consul"
	"github.com/aireuropa/resec/resec/redis"
	"github.com/aireuropa/resec/resec/state"
	"github.com/bep/debounce"
	log "github.com/sirupsen/logrus"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"gopkg.in/d4l3k/messagediff.v1"
)

const (
	ResultConsulNotHealthy     = resultType("consul_not_healthy")
	ResultMasterLinkDown       = resultType("master_link_down")
	ResultMasterSyncInProgress = resultType("master_sync_in_progress")
	ResultMissingState         = resultType("missing_state")
	ResultNoMasterElected      = resultType("no_master_elected")
	ResultRedisNotHealthy      = resultType("redis_not_healthy")
	ResultRunAsMaster          = resultType("run_as_master")
	ResultRunAsSlave           = resultType("run_as_slave")
	ResultUnknown              = resultType("unknown")
	ResultUpdateService        = resultType("consul_update_service")
	ResultMultiMaster          = resultType("consul_multi_master_detected")
)

type resultType string

// reconciler will take a stream of changes happening to
// consul and redis and decide what actions that should be taken
type Reconciler struct {
	consulCommandCh        chan<- consul.Command // Write-only channel to request Consul actions to be taken
	consulState            state.Consul          // Latest (cached) Consul state
	consulStateCh          <-chan state.Consul   // Read-only channel to get Consul state updates
	debugSignalCh          chan os.Signal        // signal channel (OS / signal debug state)
	forceReconcileInterval time.Duration         // How often we should force reconcile
	logger                 *log.Entry            // reconciler logger
	reconcileCh            chan interface{}      // Channel to trigger a reconcile loop
	redisCommandCh         chan<- redis.Command  // Write-only channel to request Redis actions to be taken
	redisState             state.Redis           // Latest (cached) Redis state
	redisStateCh           <-chan state.Redis    // Read-only channel to get Redis state updates
	signalCh               chan os.Signal        // signal channel (OS / signal shutdown)
	stopCh                 chan interface{}      // stop channel (internal shutdown)
	stateServerOn          bool                  // activate the state server?
	stateListenAddress     string                // address:port for status server
	httpServer             *http.Server          // state server (nil if not active)
	consulManager          *consul.Manager       // Consul manager, stopped by Close
	redisManager           *redis.Manager        // Redis manager, stopped by Close

	consulHealth         func(context.Context) error // live Consul check used by /health
	consulHealthTimeout  time.Duration               // timeout for the live Consul check
	consulHealthFailures int                         // consecutive failed Consul checks before /health fails
	consulHealthFailed   int                         // current number of consecutive failed Consul checks
	consulHealthMu       sync.Mutex                  // protects consulHealthFailed

	announceHost string // host announced to Consul (empty when only the port is announced)
	announcePort int    // port announced to Consul
	redisHost    string // host from REDIS_ADDR, used when announceHost is empty

	sync.Mutex
}

// Close stops the signal handlers and the manager goroutines. It is for
// tests and for callers that never call Run.
func (r *Reconciler) Close() {
	signal.Stop(r.signalCh)
	signal.Stop(r.debugSignalCh)

	if r.consulManager != nil {
		r.consulManager.Close()
	}

	if r.redisManager != nil {
		r.redisManager.Close()
	}
}

// sendRedisCommand will build and send a Redis command
func (r *Reconciler) sendRedisCommand(cmd redis.CommandName) {
	r.redisCommandCh <- redis.NewCommand(cmd, r.consulState)
}

// sendConsulCommand will build and send a Consul command
func (r *Reconciler) sendConsulCommand(cmd consul.CommandName) {
	r.consulCommandCh <- consul.NewCommand(cmd, r.redisState)
}

// Run starts the procedure
func (r *Reconciler) Run() {
	// Default state.
	// Only the debounced callback reads and writes currentState. The debounce
	// package starts each callback on a new timer goroutine. The mutex
	// protects currentState and keeps the state transitions in order.
	currentState := ResultUnknown
	var currentStateMu sync.Mutex

	// Configure logger. Never change it after this line. Other goroutines read it.
	r.logger = log.WithField("system", "reconciler")

	// Fire up our internal state reader, consuming updates from Consul and Redis
	go r.stateReader()

	// Start the Consul reader
	r.sendConsulCommand(consul.StartCommand)

	// Start the Redis reader
	r.sendRedisCommand(redis.StartCommand)

	// how long to wait between forced renconcile (e.g. to keep TTL happy)
	periodicReconcileCh := time.NewTicker(r.forceReconcileInterval)

	// Debounce reconciler update events if they happen in rapid succession
	debounced := debounce.New(100 * time.Millisecond)

	// Start the state server
	if r.stateServerOn {
		r.httpServer = r.newStateServer()
		go r.serveState(r.httpServer)
	}

	for {
		select {
		// signal handler
		case <-r.signalCh:
			fmt.Println("")
			r.logger.Warn("Caught signal, stopping reconciler loop")
			go r.stop()

		case sig := <-r.debugSignalCh:
			if sig == syscall.SIGUSR1 {
				r.Lock()
				consulState, redisState := r.consulState, r.redisState
				r.Unlock()
				r.logger.WithField("dump_state", "consul").Warn(r.prettyPrint(consulState))
				r.logger.WithField("dump_state", "redis").Warn(r.prettyPrint(redisState))
			}

			if sig == syscall.SIGUSR2 {
				r.logger.WithField("dump_state", "reconciler").Warn(r.prettyPrint(r))
			}

		// Trigger a reconcile
		case <-periodicReconcileCh.C:
			r.reconcileCh <- true

		// stop the infinite loop
		case <-r.stopCh:
			r.logger.Info("Shutdown requested, stopping reconciler loop")
			return

		// evaluate state and reconcile the systems under control
		case <-r.reconcileCh:
			debounced(func() {
				currentStateMu.Lock()
				defer currentStateMu.Unlock()

				newState := r.evaluate()

				// always apply the state, even if its the same (for cases like ResultUpdateService)
				r.apply(newState)

				// If there are no state change, we're done
				if currentState == newState {
					return
				}

				// Log the reconciler state change
				r.logger.
					WithField("state", newState).
					WithField("old_state", currentState).
					Infof("Reconciler state transitioned from '%s' to '%s'", currentState, newState)

				// Update current state
				currentState = newState
			})
		}
	}
}

func (r *Reconciler) apply(state resultType) {
	span := tracer.StartSpan("resec.apply", tracer.ResourceName(string(state)))
	defer span.Finish()

	logger := r.logger.WithField("state", state)

	switch state {
	case ResultMissingState:
		logger.Debug("Not ready to reconcile yet, missing initial state")

	case ResultConsulNotHealthy:
		logger.Debugf("Can't reconcile, Consul is not healthy")

	case ResultRedisNotHealthy:
		logger.Debugf("Redis is not healthy, deregister Consul service and don't do any further changes")
		r.sendConsulCommand(consul.ReleaseLockCommand)
		r.sendConsulCommand(consul.DeregisterServiceCommand)

	case ResultUpdateService:
		r.sendConsulCommand(consul.UpdateServiceCommand)

	case ResultRunAsMaster:
		logger.Info("Configure Redis as master")
		r.sendRedisCommand(redis.RunAsMasterCommand)
		r.sendConsulCommand(consul.RegisterServiceCommand)

	case ResultNoMasterElected:
		logger.Warn("Currently no master Redis is elected in Consul catalog, can't enslave local Redis")

	case ResultRunAsSlave:
		logger.Info("Reconfigure Redis as slave")
		r.sendRedisCommand(redis.RunAsSlaveCommand)

	case ResultMasterSyncInProgress:
		logger.Warn("Master sync in progress, can't serve traffic")
		r.sendConsulCommand(consul.DeregisterServiceCommand)

	case ResultMasterLinkDown:
		logger.Warn("Master link is down, can't serve traffic")
		r.sendConsulCommand(consul.DeregisterServiceCommand)
	case ResultMultiMaster:
		logger.Warn("Detected multiple masters trying to unregister")
		r.sendConsulCommand(consul.DeregisterMultiMasterCommand)

	default:
		logger.Errorf("Unknown state: %s", state)
	}
}

func (r *Reconciler) evaluate() (result resultType) {
	span := tracer.StartSpan("resec.evaluate")
	defer func() {
		span.SetTag("result", string(result))
		span.Finish()
	}()

	// Make sure the state doesn't change half-way through our evaluation
	r.Lock()
	defer r.Unlock()

	defer r.timeTrack(time.Now(), "reconciler")

	// do we have the initial state to start reconciliation
	if r.missingInitialState() {
		return ResultMissingState
	}

	// If Consul is not healthy, we can't make changes to the topology, as
	// we are unable to update the Consul catalog
	if r.consulState.IsUnhealhy() {
		return ResultConsulNotHealthy
	}

	// If Redis is not healthy, we can't re-configure Redis if need be, so the only
	// option is to step down as leader (if we are) and remove our Consul service
	if r.redisState.IsUnhealthy() {
		return ResultRedisNotHealthy
	}

	// If there's multiplemaster detected on the catalog, we need to unregister one.
	if r.consulState.IsMultiMaster() {
		return ResultMultiMaster
	}

	// if the Consul Lock is held (aka this instance should be master)
	if r.consulState.IsMaster() {
		// redis is already configured as master, so just update the consul check
		if r.redisState.IsRedisMaster() {
			return ResultUpdateService
		}

		// redis is not currently configured as master
		return ResultRunAsMaster
	}

	// if the consul lock is *not* held (aka this instance should be slave)
	if r.consulState.IsSlave() {
		// can't enslave if there are no known master redis in consul catalog
		if r.consulState.NoMasterElected() {
			return ResultNoMasterElected
		}

		// the catalog can still list our own service as master after we
		// lost the lock, never enslave Redis to its own address
		if r.masterIsSelf() {
			return ResultNoMasterElected
		}

		// is not following the current Redis master
		if r.notSlaveOfCurrentMaster() {
			return ResultRunAsSlave
		}

		// if sycing with redis master, lets wait for it to complete
		if r.isMasterSyncInProgress() {
			return ResultMasterSyncInProgress
		}

		// if master link is down, lets wait for it to come back up
		if r.isMasterLinkDown() && r.isMasterLinkDownTooLong() {
			return ResultMasterLinkDown
		}

		// everything is fine, update service ttl
		return ResultUpdateService
	}

	return ResultUnknown
}

func (r *Reconciler) overviewPage() string {
	o := "<html><head><title>Resec</title><body><h2>Resec Overview</h2><p>"
	if r.redisState.IsReadyToServe() {
		o += "<b>Instance is ready</b><p>"
	}

	if r.redisState.Info.Loading {
		o += "<b>Redis is loading data from disk</b><br>"
	}
	if r.redisState.Info.MasterSyncInProgress {
		o += "<b>Redis is syncing data from master</b><br>"
	}
	if !r.redisState.Info.MasterLinkUp && r.redisState.Info.Role != "master" {
		o += "<b>Link to master is down </b><br>"
	}

	o += "<ul>" +
		"<li> <a href=./info>redis info</a>" +
		"<li> <a href=./state>resec state</a>" +
		"<li> <a href=./health>healthcheck</a>" +
		"</ul>"

	o += "</body></html>"
	return o
}

// checkConsulHealth does the live Consul check. It returns an error only
// after consulHealthFailures consecutive failed checks.
func (r *Reconciler) checkConsulHealth(ctx context.Context) error {
	if r.consulHealth == nil {
		return nil
	}

	err := r.consulHealth(ctx)

	r.consulHealthMu.Lock()
	defer r.consulHealthMu.Unlock()

	if err == nil {
		r.consulHealthFailed = 0
		return nil
	}

	r.consulHealthFailed++
	if r.consulHealthFailed < r.consulHealthFailures {
		r.logger.Warnf("health: consul check failed (%d/%d): %v", r.consulHealthFailed, r.consulHealthFailures, err)
		return nil
	}

	return err
}

// newStateServer creates the HTTP state server
func (r *Reconciler) newStateServer() *http.Server {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/" {
			http.NotFound(w, req)
			return
		}
		r.Lock()
		page := r.overviewPage()
		r.Unlock()
		w.Write([]byte(page))
	})

	mux.HandleFunc("/state", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		jData, _ := r.MarshalJSON()
		w.Write(jData)
	})

	mux.HandleFunc("/info", func(w http.ResponseWriter, req *http.Request) {
		r.Lock()
		info := r.redisState.InfoString
		r.Unlock()
		fmt.Fprint(w, info)
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithTimeout(req.Context(), r.consulHealthTimeout)
		defer cancel()

		if err := r.checkConsulHealth(ctx); err != nil {
			r.logger.Warnf("health: consul check failed: %v", err)
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprintf(w, "fail: consul: %v", err)
			return
		}

		r.Lock()
		ready := r.redisState.IsReadyToServe()
		r.Unlock()

		if !ready {
			w.WriteHeader(http.StatusServiceUnavailable)
			w.Write([]byte("fail: redis not ready"))
			return
		}
		w.Write([]byte("ok"))
	})

	return &http.Server{
		Addr:              r.stateListenAddress,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

// serveState runs the state server. An error does not stop resec.
func (r *Reconciler) serveState(server *http.Server) {
	r.logger.Info("Starting state server at ", server.Addr)

	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		r.logger.Errorf("State server stopped: %v", err)
	}
}

func (r *Reconciler) stateReader() {
	for {
		select {
		// stop the infinite loop
		case <-r.stopCh:
			r.logger.Info("Shutdown requested, stopping state loop")
			return

		// New redis state change
		case redis, ok := <-r.redisStateCh:
			if !ok {
				r.logger.Error("Redis state channel was closed, shutting down")
				return
			}

			r.Lock()
			r.logger.Debug("New Redis state")
			changed := r.diffState(r.redisState, redis)
			if changed {
				r.redisState = redis
			}
			r.Unlock()

			// send after Unlock, evaluate() needs the lock
			if changed {
				r.reconcileCh <- true
			}

		// New Consul state change
		case consul, ok := <-r.consulStateCh:
			if !ok {
				r.logger.Error("Consul state channel was closed, shutting down")
				return
			}

			r.Lock()
			r.logger.Debug("New Consul state")
			changed := r.diffState(r.consulState, consul)
			if changed {
				r.consulState = consul
			}
			r.Unlock()

			// send after Unlock, evaluate() needs the lock
			if changed {
				r.reconcileCh <- true
			}
		}
	}
}

// isMasterSyncInProgress return whether the slave is currently doing a full sync from
// the redis master - this is also the initial sync triggered by doing a SLAVEOF command
func (r *Reconciler) isMasterSyncInProgress() bool {
	return r.redisState.Info.MasterSyncInProgress
}

// isMasterLinkDown return whether the slave has lost connection to the
// redis master
func (r *Reconciler) isMasterLinkDown() bool {
	return r.redisState.Info.MasterLinkUp == false
}

// isMasterLinkDownTooLong return whether the slave has lost connectity to the
// redis master for too long
func (r *Reconciler) isMasterLinkDownTooLong() bool {
	// TODO(jippi): make 10s configurable
	return r.redisState.Info.MasterLinkDownSince > 10*time.Second
}

// missingInitialState return whether we got initial state from both Consul
// and Redis, so we are able to start making decissions on the state of
// the Redis under management
func (r *Reconciler) missingInitialState() bool {
	if r.redisState.Ready == false {
		r.logger.Warn("Redis still missing initial state")
		return true
	}

	if r.consulState.Ready == false {
		r.logger.Warn("Consul still missing initial state")
		return true
	}

	return false
}

// masterIsSelf returns true when the master address in Consul is our own address
func (r *Reconciler) masterIsSelf() bool {
	if r.announcePort == 0 || r.consulState.MasterPort != r.announcePort {
		return false
	}

	host := r.announceHost
	if host == "" {
		host = r.redisHost
	}

	return host != "" && r.consulState.MasterAddr == host
}

// notSlaveOfCurrentMaster return wheter the Redis under management currently
// are configured to be slave of the currently elected master Redis
func (r *Reconciler) notSlaveOfCurrentMaster() bool {
	logger := r.logger.WithField("check", "isSlaveOfCurrentMaster")
	// if Redis thing its master, it can't be a slave of another node
	if r.redisState.IsRedisMaster() {
		logger.Debugf("isRedismaster() == true")
		return true
	}

	// if the host don't match consul state, it's not slave (of the right node)
	if r.redisState.Info.MasterHost != r.consulState.MasterAddr {
		logger.Debugf("'master_host=%s' do not match expected master host %s", r.redisState.Info.MasterHost, r.consulState.MasterAddr)
		return true
	}

	// if the port don't match consul state, it's not slave (of the right node)
	if r.redisState.Info.MasterPort != r.consulState.MasterPort {
		logger.Debugf("'master_port=%d' do not match expected master host %d", r.redisState.Info.MasterPort, r.consulState.MasterPort)
		return true
	}

	// looks good
	return false
}

// stop will ensure Consul and Redis will gracefully stop
func (r *Reconciler) stop() {
	var wg sync.WaitGroup

	wg.Add(3)

	r.logger.Debugf("Consul Cleanup started ")
	r.sendConsulCommand(consul.StopConsulCommand)

	r.logger.Debugf("Redis Cleanup started ")
	r.sendRedisCommand(redis.StopCommand)

	// monitor cleanup process from state
	go func() {
		redisStopped := false
		consulStopped := false

		timeoutCh := time.NewTimer(1 * time.Minute)
		intervalCh := time.NewTicker(50 * time.Millisecond)

		for {
			select {
			case <-timeoutCh.C:
				r.logger.Fatal("Did not gracefully shut down within 60s, hard quitting")

			case <-intervalCh.C:
				r.Lock()
				redisDone, consulDone := r.redisState.Stopped, r.consulState.Stopped
				r.Unlock()

				if redisDone && redisStopped == false {
					redisStopped = true
					wg.Done()
				}

				if consulDone && consulStopped == false {
					consulStopped = true
					wg.Done()
				}

				if redisStopped && consulStopped {
					wg.Done()
					return
				}
			}
		}
	}()

	wg.Wait()

	if r.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := r.httpServer.Shutdown(ctx); err != nil {
			r.logger.Errorf("Could not stop state server: %v", err)
		}
		cancel()
	}

	r.logger.Debugf("Cleanup completed")
	close(r.stopCh)
}

func (r *Reconciler) timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	r.logger.Debugf("%s took %s", name, elapsed)
}

func (r *Reconciler) diffState(a, b interface{}) bool {
	d, equal := messagediff.DeepDiff(a, b)
	if equal {
		return false
	}

	for path, added := range d.Added {
		r.logger.Debugf("added: %s = %#v", path.String(), added)
	}
	for path, removed := range d.Removed {
		r.logger.Debugf("removed: %s = %#v", path.String(), removed)
	}
	for path, modified := range d.Modified {
		r.logger.Debugf("modified: %s = %#v", path.String(), modified)
	}

	return true
}

// prettyPrint will JSON encode the input and return the string
// we use it to print the internal state of the reconciler when getting
// the right SIG
func (r *Reconciler) prettyPrint(data interface{}) string {
	var p []byte
	p, err := json.MarshalIndent(data, "", "\t")
	if err != nil {
		r.logger.Error(err)
		return ""
	}

	return string(p)
}

// Marshalling to JSON is used when sendnig debug signal to the process
func (r *Reconciler) MarshalJSON() ([]byte, error) {
	r.Lock()
	defer r.Unlock()

	return json.Marshal(map[string]interface{}{
		"consulState":            r.consulState,
		"forceReconcileInterval": r.forceReconcileInterval,
		"redisState":             r.redisState,
	})
}
