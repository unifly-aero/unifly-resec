package consul

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/aireuropa/resec/resec/state"
	consulapi "github.com/hashicorp/consul/api"
)

// emit will emit a consul state change to the reconciler
func (m *Manager) emit() {
	m.mu.Lock()
	snapshot := m.copyState()
	m.mu.Unlock()

	m.stateCh <- snapshot
}

// copyState returns a deep copy of the state. The caller must hold m.mu.
func (m *Manager) copyState() state.Consul {
	s := *m.state
	if m.state.MasterServices != nil {
		s.MasterServices = append([]state.ConsulService(nil), m.state.MasterServices...)
	}
	return s
}

// update changes the state under the lock and emits the new state
func (m *Manager) update(fn func(s *state.Consul)) {
	m.mu.Lock()
	fn(m.state)
	snapshot := m.copyState()
	m.mu.Unlock()

	m.stateCh <- snapshot
}

// snapshot returns a copy of the current state
func (m *Manager) snapshot() state.Consul {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.copyState()
}

// LockKeyHealthy does a live KV read of the lock key.
// It returns an error if Consul has no leader, the agent is down,
// or the key does not exist.
func (m *Manager) LockKeyHealthy(ctx context.Context) error {
	q := (&consulapi.QueryOptions{RequireConsistent: true}).WithContext(ctx)
	pair, _, err := m.client.KVGet(m.config.lockKey, q)
	if err != nil {
		return fmt.Errorf("consul kv get %q: %w", m.config.lockKey, err)
	}

	// a consistent read needs a Consul leader
	m.markConsulHealthy()

	if pair == nil {
		return fmt.Errorf("consul kv key %q not found", m.config.lockKey)
	}
	return nil
}

// cleanup will do cleanup tasks when the reconciler is shutting down
func (m *Manager) cleanup() {
	m.logger.Debug("Releasing lock")
	m.releaseConsulLock()

	m.logger.Debug("Deregister service")
	m.deregisterService()

	m.logger.Debug("Closing stopCh")
	m.lockChOnce.Do(func() { close(m.lockCh) })
	m.stopOnce.Do(func() { close(m.stopCh) })

	m.update(func(s *state.Consul) { s.Stopped = true })
}

// Close stops the goroutines of the manager. It does not release the lock
// and it does not deregister the service. It is safe to call it more than once.
func (m *Manager) Close() {
	m.lockChOnce.Do(func() { close(m.lockCh) })
	m.stopOnce.Do(func() { close(m.stopCh) })
}

// AnnounceAddr returns the host and port announced to Consul.
// The host is empty when only the port is announced.
func (m *Manager) AnnounceAddr() (host string, port int) {
	return m.config.announceHost, m.config.announcePort
}

// watchConsulRecovery does a consistent KV read of the lock key while
// Consul is not healthy. A slave can wait inside Lock() for a long time,
// this read lets Healthy return to true without a lock acquisition.
func (m *Manager) watchConsulRecovery() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return

		case <-ticker.C:
			m.recoveryCheck()
		}
	}
}

// recoveryCheck is one tick of watchConsulRecovery. It does nothing while
// Consul is healthy.
func (m *Manager) recoveryCheck() {
	if m.snapshot().Healthy {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := m.LockKeyHealthy(ctx); err != nil {
		m.logger.Debugf("Consul recovery check failed: %v", err)
	}
}

// continuouslyAcquireConsulLeadership waits to acquire the lock to the Consul KV key.
// it will run until the stopCh is closed
func (m *Manager) continuouslyAcquireConsulLeadership() {
	interval := 250 * time.Millisecond
	timer := time.NewTimer(interval)

	go m.watchConsulRecovery()

	for {
		select {
		// if closed, we should stop working
		case <-m.stopCh:
			return

		// Periodically try to acquire the consul lock
		case <-timer.C:
			m.acquireConsulLeadership()

			// if we are not healthy, apply exponential backoff
			if !m.snapshot().Healthy {
				backoffDuration := m.backoff.Duration()
				m.logger.Errorf("Consul is not healthy, going to apply backoff of %s until next attempt", backoffDuration.Round(time.Second).String())
				timer.Reset(backoffDuration)
				continue
			}

			timer.Reset(interval)
		}
	}
}

// acquireConsulLeadership will one-off try to acquire the consul lock needed to become
// redis Master node
func (m *Manager) acquireConsulLeadership() {
	// if we already hold the lock, we can't acquire it again
	if m.snapshot().Master {
		m.logger.Debug("We already hold the lock, can't acquire it again")
		return
	}

	// create the lock structs
	lockOptions := &consulapi.LockOptions{
		Key:              m.config.lockKey,
		SessionName:      m.config.lockSessionName,
		SessionTTL:       m.config.lockTTL.String(),
		MonitorRetries:   m.config.lockMonitorRetries,
		MonitorRetryTime: m.config.lockMonitorRetryInterval,
	}

	// Create the Lock options
	{
		var err error

		m.config.lock, err = m.client.LockOpts(lockOptions)
		if err != nil {
			m.logger.Errorf("Failed create lock options: %+v", err)
			return
		}
	}

	// try to acquire the lock
	{
		var err error

		m.logger.Infof("Trying to acquire consul lock")
		m.lockErrorCh, err = m.config.lock.Lock(m.lockCh)
		m.handleConsulError(err)
		if err != nil {
			return
		}

		// Lock() returns (nil, nil) when lockCh is closed (shutdown)
		if m.lockErrorCh == nil {
			m.logger.Debug("Lock attempt aborted")
			return
		}

		m.logger.Info("Lock successfully acquired")
		m.markConsulHealthy()
	}

	//
	// start monitoring the consul lock for errors / changes
	//

	releaseCh := make(chan interface{})

	m.update(func(s *state.Consul) {
		s.Master = true
		m.config.voluntarilyReleaseLockCh = releaseCh
	})

	// At this point, if we return from this function, we need to make sure
	// we release the lock
	defer func() {
		err := m.config.lock.Unlock()
		m.handleConsulError(err)
		if err != nil {
			m.logger.Errorf("Could not release Consul Lock: %v", err)
		} else {
			m.logger.Info("Consul Lock successfully released")
		}

		m.update(func(s *state.Consul) {
			s.Master = false
			// the catalog can still list us as master until the TTL
			// check expires, clear the address so the watch reloads it
			s.MasterAddr = ""
			s.MasterPort = 0
			m.config.voluntarilyReleaseLockCh = nil
		})
	}()

	// Wait for changes to Consul Lock
	for {
		select {
		// Global stop of all go-routines, reconciler is shutting down
		case <-m.stopCh:
			return

		// Changes on the lock error channel
		// if the channel is closed, it mean that we no longer hold the lock
		// if written to, we simply pass on the message
		case data, ok := <-m.lockErrorCh:
			if !ok {
				m.logger.Error("Consul Lock error channel was closed, we no longer hold the lock")
				return
			}

			m.logger.Warnf("Something wrote to lock error channel %+v", data)

		// voluntarily release our claim on the lock
		case <-releaseCh:
			m.logger.Warnf("Voluntarily releasing the Consul lock")
			return
		}
	}
}

// releaseConsulLock stops consul lock handler")
func (m *Manager) releaseConsulLock() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.state.Master || m.config.voluntarilyReleaseLockCh == nil {
		m.logger.Debug("Can't release Consul lock, we don't have it")
		return
	}

	m.logger.Info("Releasing Consul lock")
	close(m.config.voluntarilyReleaseLockCh)
	// prevent a second close before the lock goroutine has cleaned up
	m.config.voluntarilyReleaseLockCh = nil
}

// getReplicationStatus will return current replication status
// the default value is 'slave' - only if we hold the lock will 'master' be returned
func (m *Manager) getReplicationStatus() string {
	if m.snapshot().Master {
		return "master"
	}

	return "slave"
}

// getConsulServiceName will return the consul service name to use
// depending on using tagged service (replication status as tag) or
// service name (with replication status as suffix)
func (m *Manager) getConsulServiceName() string {
	name := m.config.serviceName

	if name == "" {
		name = m.config.serviceNamePrefix + "-" + m.getReplicationStatus()
	}

	return name
}

// getConsulServiceID will return the consul service ID
// it will either use the service name, or the service name prefix depending
// on your configuration
func (m *Manager) getConsulServiceID() string {
	ID := m.config.serviceName

	if m.config.serviceName == "" {
		ID = m.config.serviceNamePrefix
	}

	return ID
}

// registerService registers a service in consul
func (m *Manager) registerService(redisState state.Redis) {
	serviceID := m.getConsulServiceID()
	replicationStatus := m.getReplicationStatus()

	// Keep the IDs in local variables. Set them in the config only after
	// the service and the check are both registered.
	fullServiceID := serviceID + "@" + m.config.announceAddr
	checkID := fullServiceID + ":replication-status-check"

	serviceInfo := &consulapi.AgentServiceRegistration{
		ID:   fullServiceID,
		Port: m.config.announcePort,
		Name: m.getConsulServiceName(),
		Tags: m.config.serviceTagsByRole[replicationStatus],
	}

	if m.config.announceHost != "" {
		serviceInfo.Address = m.config.announceHost
	}

	m.logger.Infof("Registering %s service in consul", serviceInfo.Name)

	err := m.client.ServiceRegister(serviceInfo)
	m.handleConsulError(err)
	if err != nil {
		return
	}

	m.logger.Infof("Registered service %s (%s) with address %s:%d", serviceInfo.Name, serviceInfo.ID, serviceInfo.Address, serviceInfo.Port)

	check := &consulapi.AgentCheckRegistration{
		Name:      "Resec: " + replicationStatus + " replication status",
		ID:        checkID,
		ServiceID: fullServiceID,
		AgentServiceCheck: consulapi.AgentServiceCheck{
			TTL:                            m.config.serviceTTL.String(),
			Status:                         "warning",
			DeregisterCriticalServiceAfter: m.config.deregisterServiceAfter.String(),
		},
	}

	err = m.client.CheckRegister(check)
	m.handleConsulError(err)
	if err != nil {
		// Do not leave a service without a check in the catalog.
		m.handleConsulError(m.client.ServiceDeregister(fullServiceID))
		return
	}

	m.config.serviceID = fullServiceID
	m.config.checkID = checkID
}

// deregisterService will deregister the consul service from Consul catalog
func (m *Manager) deregisterService() {
	if m.snapshot().Healthy && m.config.serviceID != "" {
		err := m.client.ServiceDeregister(m.config.serviceID)
		m.handleConsulError(err)
	}

	m.config.serviceID = ""
	m.config.checkID = ""
}

// localNodeName returns the name of the local Consul agent node
func (m *Manager) localNodeName() (string, error) {
	if m.config.nodeName != "" {
		return m.config.nodeName, nil
	}

	name, err := m.client.NodeName()
	if err != nil {
		return "", err
	}

	m.config.nodeName = name
	return name, nil
}

// deregisterMultiMasterService removes the wrong master services when
// more than one master service is found in the Consul catalog.
//
// If this node holds the lock, it keeps its own service and removes the
// services of other nodes from the catalog (needs node:write on those nodes).
// If this node does not hold the lock, it removes only its own service.
func (m *Manager) deregisterMultiMasterService(redisState state.Redis) {
	current := m.snapshot()

	if !current.Master {
		m.logger.Warn("Multiple masters found and we do not hold the lock, deregistering our own service")
		m.deregisterService()
		return
	}

	nodeName, err := m.localNodeName()
	if err != nil {
		m.handleConsulError(err)
		return
	}

	for _, service := range current.MasterServices {
		if service.Node == nodeName && service.ServiceID == m.config.serviceID {
			continue
		}

		m.logger.Infof("Deregistering foreign master service %s:%d, %s on node %s", service.Address, service.Port, service.ServiceID, service.Node)
		err := m.client.CatalogDeregister(service.Node, service.ServiceID)
		m.handleConsulError(err)
	}
}

// setConsulCheckStatus sets consul status check TTL and output
func (m *Manager) setConsulCheckStatus(redisState state.Redis) {
	if m.config.checkID == "" {
		m.registerService(redisState)
	}

	m.logger.Debug("Updating Check TTL for service")
	err := m.client.UpdateTTL(m.config.checkID, redisState.InfoString, "passing")

	// if the check don't have a TTL, it mean that the service + check is gone
	// deregister the service+check so we can register a new one
	if err != nil && strings.Contains(err.Error(), "does not have associated TTL") {
		m.logger.Warnf("Consul check is gone, will register service again: %v", err)
		m.deregisterService()
		return
	}

	m.handleConsulError(err)
}

// getConsulMasterDetails will return the Consul service name and tag
// that matches what the Resec acting as master will expose in Consul
func (m *Manager) getConsulMasterDetails() (serviceName string, serviceTag string) {
	serviceName = m.config.serviceNamePrefix + "-master"

	if m.config.serviceName != "" {
		serviceName = m.config.serviceName
		serviceTag = m.config.serviceTagsByRole["master"][0]
	}

	return serviceName, serviceTag
}

// watchConsulMasterService starts watching the service (+ tags) that represents the
// redis master service. All changes will be emitted to masterConsulServiceCh.
func (m *Manager) watchConsulMasterService() {
	serviceName, serviceTag := m.getConsulMasterDetails()

	q := &consulapi.QueryOptions{
		WaitIndex: 1,
		WaitTime:  30 * time.Second,
	}

	// How often we should force a refresh of external state?
	duration := 250 * time.Millisecond
	timer := time.NewTimer(duration)

	for {
		select {

		case <-m.stopCh:
			return

		case <-timer.C:
			if err := m.pollMasterService(serviceName, serviceTag, q); err != nil {
				timer.Reset(m.backoff.ForAttempt(m.backoff.Attempt()))
				continue
			}

			timer.Reset(duration)
		}
	}
}

// pollMasterService does one query for the master service and emits
// a state change if the master service changed
func (m *Manager) pollMasterService(serviceName, serviceTag string, q *consulapi.QueryOptions) error {
	services, meta, err := m.client.ServiceHealth(serviceName, serviceTag, true, q)
	m.handleConsulError(err)
	if err != nil {
		// after a leader change the raft index can move, a stale
		// WaitIndex can block the query for the full WaitTime
		q.WaitIndex = 1
		return err
	}

	// handleConsulError() can clear the master address from another
	// go-routine, do not skip the update if the address is empty
	current := m.snapshot()
	if q.WaitIndex == meta.LastIndex && current.MasterAddr != "" {
		m.logger.Debugf("No change in master service health")
		return nil
	}

	q.WaitIndex = meta.LastIndex

	if len(services) == 0 {
		m.logger.Warn("No (healthy) master service found in Consul catalog")
		return nil
	}

	if len(services) > 1 {
		m.logger.Error("More than 1 (healthy) master service found in Consul catalog")
		masterServices := m.GetMasterServices(services)
		m.update(func(s *state.Consul) { s.MasterServices = masterServices })
		return nil
	}

	master := services[0]

	// handle If master is registred in consul with port only
	// Use node IP insted of service IP
	masterAddr := master.Service.Address
	if masterAddr == "" {
		masterAddr = master.Node.Address
	}

	if current.MasterAddr == masterAddr && current.MasterPort == master.Service.Port && len(current.MasterServices) <= 1 {
		m.logger.Debugf("No change in master service configuration")
		return nil
	}

	m.update(func(s *state.Consul) {
		s.MasterAddr = masterAddr
		s.MasterPort = master.Service.Port
		s.MasterServices = nil
	})
	return nil
}

// GetMasterServices
func (m *Manager) GetMasterServices(services []*consulapi.ServiceEntry) []state.ConsulService {
	css := []state.ConsulService{}
	for _, service := range services {
		var cs state.ConsulService

		if service.Service.Address != "" {
			cs.Address = service.Service.Address
		} else {
			cs.Address = service.Node.Address
		}
		cs.Port = service.Service.Port
		cs.ServiceID = service.Service.ID
		cs.Node = service.Node.Node
		css = append(css, cs)
	}
	return css
}

// isConsulDown returns true for transport and server errors, for example
// when the agent is down or when the Consul servers have no leader.
func isConsulDown(err error) bool {
	if err == nil {
		return false
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	s := err.Error()
	for _, needle := range []string{
		"dial tcp",
		"No cluster leader",
		"rpc error",
		"Unexpected response code: 500",
		"Unexpected response code: 502",
		"Unexpected response code: 503",
		"connection refused",
		"connection reset",
		"EOF",
		"context deadline exceeded",
	} {
		if strings.Contains(s, needle) {
			return true
		}
	}

	return false
}

// markConsulHealthy is called only after a call that needs a Consul
// leader succeeded: Lock(), session create, or a consistent KV read.
// Agent-local calls succeed without a leader and must not call it.
func (m *Manager) markConsulHealthy() {
	if m.snapshot().Healthy {
		return
	}

	// reset exponential backoff
	m.backoff.Reset()

	m.logger.Info("Consul is healthy again")
	m.update(func(s *state.Consul) { s.Healthy = true })
}

// handleConsulError logs the error and marks Consul as not healthy
// for transport and server errors. It never marks Consul healthy.
func (m *Manager) handleConsulError(err error) {
	if err == nil {
		return
	}

	m.logger.Errorf("Consul error: %v", err)

	if !isConsulDown(err) || !m.snapshot().Healthy {
		return
	}

	// Mark Consul as not healthy. The reconciler then freezes the Redis
	// topology. Do not deregister the service: the agent-local call works
	// while the servers have no leader, and clients can continue to use
	// the current master. Clear the master address so that a stale value
	// can not cause SLAVEOF to our own address.
	m.logger.Warn("Consul is not healthy, freezing the Redis topology")
	m.update(func(s *state.Consul) {
		s.Healthy = false
		s.MasterAddr = ""
		s.MasterPort = 0
		s.MasterServices = nil
	})
}

func (m *Manager) CommandRunner() {
	for {
		select {

		case <-m.stopCh:
			return

		case payload := <-m.commandCh:
			switch payload.name {
			case RegisterServiceCommand:
				m.registerService(payload.redisState)

			case DeregisterServiceCommand:
				m.deregisterService()

			case UpdateServiceCommand:
				m.setConsulCheckStatus(payload.redisState)

			case ReleaseLockCommand:
				m.releaseConsulLock()

			case StartCommand:
				m.start()

			case StopConsulCommand:
				m.cleanup()

			case DeregisterMultiMasterCommand:
				m.deregisterMultiMasterService(payload.redisState)
			}
		}
	}
}

func (m *Manager) start() {
	go m.continuouslyAcquireConsulLeadership()
	go m.watchConsulMasterService()

	m.update(func(s *state.Consul) { s.Ready = true })
}

func (m *Manager) GetStateReader() <-chan state.Consul {
	return m.stateCh
}

func (m *Manager) GetCommandWriter() chan<- Command {
	return m.commandCh
}
