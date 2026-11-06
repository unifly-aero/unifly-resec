package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aireuropa/resec/resec/state"
	"github.com/redis/go-redis/v9"
)

// emit will send a state update to the reconciler.
// The caller copies the state under m.mu and sends the copy.
func (m *Manager) emit(s state.Redis) {
	m.stateCh <- s
}

// isReady returns true when Redis is ready.
func (m *Manager) isReady() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.state.Ready
}

// runAsMaster sets the instance to be the master
func (m *Manager) runAsMaster() error {
	if err := m.client.Do(context.Background(), "REPLICAOF", "NO", "ONE").Err(); err != nil {
		return err
	}

	logger := m.logger.WithField("role", "master")
	logger.Info("Promoted redis to Master")
	return nil
}

// runAsSlave sets the instance to be a slave for the master
func (m *Manager) runAsSlave(masterAddress string, masterPort int) error {
	m.logger.Infof("Enslaving redis to be slave of %s:%d", masterAddress, masterPort)

	if err := m.client.Do(context.Background(), "REPLICAOF", masterAddress, strconv.Itoa(masterPort)).Err(); err != nil {
		return fmt.Errorf("Could not enslave redis to be slave of %s:%d (%v)", masterAddress, masterPort, err)
	}

	logger := m.logger.WithField("role", "slave")
	logger.Infof("Enslaved redis to be slave of %s:%d", masterAddress, masterPort)
	return nil
}

// disconnect all normal connections
func (m *Manager) disconnectUsers() error {
	m.logger.Warnf("Disconnecting all clients")
	ctx := context.Background()
	cmd := redis.NewIntCmd(ctx, "CLIENT", "KILL", "TYPE", "normal")
	m.client.Process(ctx, cmd)
	if err := cmd.Err(); err != nil {
		return err
	}

	m.logger.Warnf("Disconnected %d users", cmd.Val())
	return nil
}

func (m *Manager) cleanup() {
	m.stopOnce.Do(func() { close(m.stopCh) })

	m.mu.Lock()
	m.state.Stopped = true
	s := *m.state
	m.mu.Unlock()

	m.emit(s)
}

// Close stops the goroutines of the manager. It is safe to call it more than once.
func (m *Manager) Close() {
	m.stopOnce.Do(func() { close(m.stopCh) })
}

func (m *Manager) CommandRunner() {
	for {
		select {

		case <-m.stopCh:
			m.logger.Infof("Shutting down Redis command runner")
			return

		case payload := <-m.commandCh:
			switch payload.name {
			case StartCommand:
				go m.watchStatus()

			case StopCommand:
				m.logger.Infof("Stop command sent to Redis")
				m.cleanup()

			case RunAsMasterCommand:
				if !m.isReady() {
					m.logger.Warnf("Got 'runAsMaster' command, but Redis is not ready yet - ignoring")
					continue
				}

				if err := m.runAsMaster(); err != nil {
					m.logger.Error(err)
				}

			case RunAsSlaveCommand:
				if !m.isReady() {
					m.logger.Warnf("Got 'runAsSlave' command, but Redis is not ready yet - ignoring")
					continue
				}

				if err := m.runAsSlave(payload.consulState.MasterAddr, payload.consulState.MasterPort); err != nil {
					m.logger.Error(err)
					continue
				}

				if err := m.disconnectUsers(); err != nil {
					m.logger.Error(err)
				}
			}
		}
	}
}

// watchStatus checks redis replication status
func (m *Manager) watchStatus() {
	m.mu.Lock()
	if m.watcherRunning {
		m.mu.Unlock()
		m.logger.Warn("Trying to start status watcher, but already running, ignoring...")
		return
	}
	m.watcherRunning = true
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		m.watcherRunning = false
		m.mu.Unlock()
	}()

	interval := time.Second
	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-m.stopCh:
			return

		case <-timer.C:
			result, err := m.client.Info(context.Background()).Result()
			// any failure will trigger a disconnect event
			if err != nil {
				m.mu.Lock()
				m.state.Healthy = false
				s := *m.state
				m.mu.Unlock()
				m.emit(s)

				// Lets start backing off in case it's a long standing issue that is going on
				backoffDuration := m.backoff.Duration()
				m.logger.Errorf("Redis is not healthy, going to apply backoff of %s until next attempt: %s", backoffDuration.Round(time.Second).String(), err)
				timer.Reset(backoffDuration)
				continue
			}

			// Success, reset the backoff counter
			m.backoff.Reset()

			// Queue next execution
			timer.Reset(interval)

			// Parse the "info" result from Redis
			info := m.parseInfoResult(result)

			// If Redis is loading data from disk, do not mark us as healthy
			// If Redis is _not_ loading data from disk, we're healthy
			m.mu.Lock()
			m.state.Healthy = !info.Loading

			// Mark Redis as "ready" (e.g. we can connect)
			m.state.Ready = true

			// Update state with most recent output
			m.state.Info = info
			m.state.InfoString = result
			s := *m.state
			m.mu.Unlock()
			m.emit(s)
		}
	}
}

func (m *Manager) Config() Config {
	return *m.config
}

func (m *Manager) StateChReader() <-chan state.Redis {
	return m.stateCh
}

func (m *Manager) CommandChWriter() chan<- Command {
	return m.commandCh
}

func (m *Manager) parseInfoResult(str string) state.RedisStatus {
	kvPair := m.parseKeyValue(str)

	// Create new replication state
	newState := state.RedisStatus{
		Role: kvPair["role"],
	}

	// Add master_host to state (if set) - only available for slaves
	if masterHost, ok := kvPair["master_host"]; ok {
		newState.MasterHost = masterHost
	}

	// Add master_port to state (if set) - only available for slaves
	if masterPortString, ok := kvPair["master_port"]; ok {
		masterPort, err := strconv.Atoi(masterPortString)
		if err == nil {
			newState.MasterPort = masterPort
		}
	}

	// Is the master link up?
	if masterLinkUpString, ok := kvPair["master_link_status"]; ok {
		newState.MasterLinkUp = masterLinkUpString == "up"
	}

	// Is master sync in process?
	if masterSyncInProgress, ok := kvPair["master_sync_in_progress"]; ok {
		newState.MasterSyncInProgress = masterSyncInProgress != "0"
	}

	// track the time since the master link went down
	if masterLinkDownSinceSecondsString, ok := kvPair["master_link_down_since_seconds"]; ok {
		number, _ := strconv.Atoi(masterLinkDownSinceSecondsString)
		newState.MasterLinkDownSince = time.Duration(number) * time.Second
	}

	// track if redis is loading data
	if loadingStr, ok := kvPair["loading"]; ok {
		loading, _ := strconv.ParseBool(loadingStr)
		newState.Loading = loading
	}

	return newState
}

func (m *Manager) parseKeyValue(str string) map[string]string {
	res := make(map[string]string)

	lines := strings.Split(str, "\n")
	for _, line := range lines {
		line = strings.TrimRight(line, "\r")
		if strings.HasPrefix(line, "#") {
			continue
		}

		// SplitN keeps values with ':' (for example IPv6 master_host)
		pair := strings.SplitN(line, ":", 2)
		if len(pair) != 2 {
			continue
		}

		res[strings.TrimSpace(pair[0])] = pair[1]
	}

	return res
}
