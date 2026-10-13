package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flowrulz/flow-rulz/internal/ports"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

const minimumLockTTL = time.Second

// ErrLockNotAcquired is returned when a lock cannot be acquired.
var ErrLockNotAcquired = errors.New("redis lock not acquired")

// ErrLockLost is returned when lock ownership is lost during renewal.
var ErrLockLost = errors.New("redis lock ownership lost")

// LockManager owns distributed-lock creation independently of the key/value cache.
type LockManager struct {
	client *redis.Client
}

// NewLockManager creates a new LockManager from a Redis client.
func NewLockManager(client *Client) *LockManager {
	return &LockManager{client: client.client}
}

// Lock represents an acquired distributed lock with automatic renewal.
type Lock struct {
	client      *redis.Client
	key         string
	redisKey    string
	token       string
	ttl         time.Duration
	watchCtx    context.Context
	watchCancel context.CancelFunc
	done        chan struct{}
	lost        chan struct{}
	lostOnce    sync.Once
	stopping    atomic.Bool
}

// Acquire attempts to acquire a lock and returns true if successful.
func (m *LockManager) Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	_, err := m.Lock(ctx, key, ttl)
	if err != nil {
		return false, err
	}
	return true, nil
}

// Extend extends the TTL of an existing lock if the token matches.
func (m *LockManager) Extend(ctx context.Context, key, token string, ttl time.Duration) (bool, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return false, fmt.Errorf("redis lock extend: key is required")
	}
	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("pexpire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`)
	result, err := script.Run(ctx, m.client, []string{"lock:" + key}, token, ttl.Milliseconds()).Int()
	if err != nil {
		return false, fmt.Errorf("extend redis lock: %w", err)
	}
	return result == 1, nil
}

// Release releases a lock if the token matches.
func (m *LockManager) Release(ctx context.Context, key, token string) (bool, error) {
	key = strings.TrimSpace(key)
	if key == "" {
		return false, fmt.Errorf("redis lock release: key is required")
	}
	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`)
	result, err := script.Run(ctx, m.client, []string{"lock:" + key}, token).Int()
	if err != nil {
		return false, fmt.Errorf("release redis lock: %w", err)
	}
	return result == 1, nil
}

// Lock acquires a distributed lock with automatic renewal and returns a LockGuard.
func (m *LockManager) Lock(ctx context.Context, key string, ttl time.Duration) (ports.LockGuard, error) {
	if ctx == nil {
		return nil, fmt.Errorf("redis lock: context is required")
	}
	if m == nil {
		return nil, fmt.Errorf("redis lock: manager is required")
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return nil, fmt.Errorf("redis lock: key is required")
	}
	if ttl < minimumLockTTL {
		return nil, fmt.Errorf("redis lock: ttl must be at least %s", minimumLockTTL)
	}
	if m.client == nil {
		return nil, fmt.Errorf("redis lock: client is required")
	}

	token := uuid.NewString()
	redisKey := "lock:" + key
	acquired, err := m.client.SetNX(ctx, redisKey, token, ttl).Result()
	if err != nil {
		return nil, fmt.Errorf("acquire redis lock: %w", err)
	}
	if !acquired {
		return nil, ErrLockNotAcquired
	}

	watchCtx, watchCancel := context.WithCancel(ctx)
	lock := &Lock{
		client: m.client, key: key, redisKey: redisKey, token: token, ttl: ttl,
		watchCtx: watchCtx, watchCancel: watchCancel, done: make(chan struct{}), lost: make(chan struct{}),
	}
	go lock.renewLoop()
	return lock, nil
}

func (l *Lock) markLost() {
	l.lostOnce.Do(func() {
		close(l.lost)
		if l.watchCancel != nil {
			l.watchCancel()
		}
	})
}

func (l *Lock) isLost() bool {
	select {
	case <-l.lost:
		return true
	default:
		return false
	}
}

func (l *Lock) renewLoop() {
	defer close(l.done)
	ticker := time.NewTicker(l.ttl / 3)
	defer ticker.Stop()

	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("pexpire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`)
	for {
		select {
		case <-l.watchCtx.Done():
			if !l.stopping.Load() {
				l.markLost()
			}
			return
		case <-ticker.C:
			result, err := script.Run(l.watchCtx, l.client, []string{l.redisKey}, l.token, l.ttl.Milliseconds()).Int()
			if err != nil || result != 1 {
				l.markLost()
				return
			}
		}
	}
}

// Key returns the lock key.
func (l *Lock) Key() string {
	return l.key
}

// Token returns the lock token.
func (l *Lock) Token() string {
	return l.token
}

// Lost returns a channel that is closed when the lock is lost.
func (l *Lock) Lost() <-chan struct{} {
	return l.lost
}

// Release releases the lock and verifies ownership.
func (l *Lock) Release(ctx context.Context) error {
	if ctx == nil {
		return fmt.Errorf("release redis lock: context is required")
	}
	l.stopping.Store(true)
	if l.watchCancel != nil {
		l.watchCancel()
	}
	<-l.done

	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`)
	result, err := script.Run(ctx, l.client, []string{l.redisKey}, l.token).Int()
	if err != nil {
		return fmt.Errorf("release redis lock: %w", err)
	}
	if result != 1 {
		l.markLost()
		return ErrLockLost
	}
	return nil
}

// Extend extends the lock TTL if the lock is still held.
func (l *Lock) Extend(ctx context.Context, ttl time.Duration) error {
	if ctx == nil {
		return fmt.Errorf("extend redis lock: context is required")
	}
	if ttl < l.ttl {
		return fmt.Errorf("redis lock: extension ttl must be at least the original ttl %s", l.ttl)
	}
	if l.isLost() {
		return ErrLockLost
	}
	script := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("pexpire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`)
	result, err := script.Run(ctx, l.client, []string{l.redisKey}, l.token, ttl.Milliseconds()).Int()
	if err != nil {
		return fmt.Errorf("extend redis lock: %w", err)
	}
	if result != 1 {
		l.markLost()
		return ErrLockLost
	}
	return nil
}

var _ ports.DistributedLock = (*LockManager)(nil)
var _ ports.LockGuard = (*Lock)(nil)
