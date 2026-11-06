package redis

import (
	"sync"

	"github.com/aireuropa/resec/resec/state"
	"github.com/jpillora/backoff"
	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
)

type Manager struct {
	backoff        *backoff.Backoff // exponential backoff helper
	logger         *log.Entry       // logging specificall for Redis
	client         *redis.Client    // redis client
	config         *Config          // redis config
	state          *state.Redis     // redis state
	stateCh        chan state.Redis // redis state channel to publish updates to the reconciler
	commandCh      chan Command
	stopCh         chan interface{}
	stopOnce       sync.Once  // closes stopCh only once
	mu             sync.Mutex // protects state and watcherRunning
	watcherRunning bool
}

type Config struct {
	Address string // address (IP+Port) used to talk to Redis
}
