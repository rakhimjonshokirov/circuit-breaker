package redisdriver

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Fix #11: only the three state-machine field names remain.
// OnRequest / OnSuccess / OnFailure / GetConsecutiveFailures were dead code —
// the circuit breaker counts locally via atomics and never called them.
const (
	fieldState      = "state_val"
	fieldExpiry     = "expiry_unix"
	fieldGeneration = "generation"
)

// newGenerationScript atomically transitions circuit breaker state.
// State machine lives in Redis; this Lua script is the only place that mutates
// state_val / expiry_unix / generation, making every transition a distributed CAS.
//
// KEYS[1]  – hash key
// ARGV[1]  – fromState  (-1 = "initialize only if key is brand new")
// ARGV[2]  – toState
// ARGV[3]  – expiryUnix (0 = no expiry)
// ARGV[4]  – ttlSeconds (0 = no TTL on the key)
//
// Returns the new generation on success, -1 if the CAS check failed.
var newGenerationScript = redis.NewScript(`
local key         = KEYS[1]
local from_state  = tonumber(ARGV[1])
local to_state    = tonumber(ARGV[2])
local expiry_unix = tonumber(ARGV[3])
local ttl_seconds = tonumber(ARGV[4])

local raw_state = redis.call('HGET', key, 'state_val')
local raw_gen   = redis.call('HGET', key, 'generation')

local cur_state = (raw_state ~= false) and tonumber(raw_state) or 0
local cur_gen   = (raw_gen   ~= false) and tonumber(raw_gen)   or 0

-- fromState == -1: initialize only when the key does not yet exist.
if from_state == -1 then
    if raw_gen ~= false then
        return cur_gen  -- already initialized; return current generation unchanged
    end
elseif cur_state ~= from_state then
    return -1           -- CAS failed: another instance already transitioned
end

local new_gen = cur_gen + 1
redis.call('HSET', key,
    'state_val',  to_state,
    'expiry_unix', expiry_unix,
    'generation', new_gen
)
if ttl_seconds > 0 then
    redis.call('EXPIRE', key, ttl_seconds)
end
return new_gen
`)

// Counts is a Redis-backed Driver for CircuitBreaker.
// All state-machine fields are stored in a single Redis hash under key.
// A zero TTL means the key never expires automatically.
type Counts struct {
	redisClient redis.UniversalClient
	key         string
	ttl         time.Duration
}

// New creates a Counts backed by the given Redis client and key.
// ttl sets how long the key survives after the last write; pass 0 to disable expiry.
func New(rds redis.UniversalClient, key string, ttl time.Duration) *Counts {
	return &Counts{redisClient: rds, key: key, ttl: ttl}
}

// GetStateSnapshot reads state, expiry, and generation in a single HMGet round-trip.
func (c *Counts) GetStateSnapshot(ctx context.Context) (state int64, expiry time.Time, generation uint64, err error) {
	vals, err := c.redisClient.HMGet(ctx, c.key, fieldState, fieldExpiry, fieldGeneration).Result()
	if err != nil {
		slog.Error("redis_driver: GetStateSnapshot failed", "key", c.key, "error", err)
		return 0, time.Time{}, 0, err
	}

	if v, ok := vals[0].(string); ok {
		state, _ = strconv.ParseInt(v, 10, 64)
	}
	if v, ok := vals[1].(string); ok {
		if unix, parseErr := strconv.ParseInt(v, 10, 64); parseErr == nil && unix > 0 {
			expiry = time.Unix(unix, 0)
		}
	}
	if v, ok := vals[2].(string); ok {
		g, _ := strconv.ParseUint(v, 10, 64)
		generation = g
	}
	return state, expiry, generation, nil
}

// NewGeneration atomically transitions circuit breaker state via a Lua CAS script.
// Pass fromState=-1 to initialize a brand-new key without overwriting an existing one.
// Returns (newGeneration, ok, error); ok=false means CAS check failed.
func (c *Counts) NewGeneration(ctx context.Context, fromState, toState int64, expiry time.Time) (uint64, bool, error) {
	var expiryUnix int64
	if !expiry.IsZero() {
		expiryUnix = expiry.Unix()
	}
	ttlSec := int64(c.ttl.Seconds())

	result, err := newGenerationScript.Run(
		ctx, c.redisClient, []string{c.key},
		fromState, toState, expiryUnix, ttlSec,
	).Int64()
	if err != nil {
		slog.Error("redis_driver: NewGeneration script failed",
			"key", c.key, "from", fromState, "to", toState, "error", err)
		return 0, false, fmt.Errorf("redis_driver: NewGeneration: %w", err)
	}

	if result == -1 {
		slog.Debug("redis_driver: NewGeneration CAS failed (state changed by peer)",
			"key", c.key, "from", fromState, "to", toState)
		return 0, false, nil
	}

	slog.Info("redis_driver: state transition committed",
		"key", c.key, "from", fromState, "to", toState, "generation", result)
	return uint64(result), true, nil
}
