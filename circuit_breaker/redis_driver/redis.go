package redisdriver

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// fix #10: constants live only here; circuit_breaker.go uses typed accessors, not raw field names.
const (
	requests             = "requests"
	totalSuccesses       = "total_successes"
	totalFailures        = "total_failures"
	consecutiveSuccesses = "consecutive_successes"
	consecutiveFailures  = "consecutive_failures"

	// fix #1: state-machine fields stored alongside counts in the same hash.
	fieldState      = "state_val"
	fieldExpiry     = "expiry_unix"
	fieldGeneration = "generation"
)

// newGenerationScript atomically transitions circuit breaker state.
// fix #1: state machine lives in Redis; this Lua script is the only place
//
//	that mutates state_val / expiry_unix / generation, making every
//	transition a distributed compare-and-swap.
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
    'state_val',             to_state,
    'expiry_unix',           expiry_unix,
    'generation',            new_gen,
    'requests',              0,
    'total_successes',       0,
    'total_failures',        0,
    'consecutive_successes', 0,
    'consecutive_failures',  0
)
if ttl_seconds > 0 then
    redis.call('EXPIRE', key, ttl_seconds)
end
return new_gen
`)

// Counts is a Redis-backed Driver for CircuitBreaker.
// All counts and state-machine fields are stored in a single Redis hash under key.
// A zero TTL means the key never expires automatically.
type Counts struct {
	redisClient redis.UniversalClient
	key         string
	ttl         time.Duration // fix #9: written on every mutating call; zero disables
}

// New creates a Counts backed by the given Redis client and key.
// ttl sets how long the key survives after the last write; pass 0 to disable expiry.
func New(rds redis.UniversalClient, key string, ttl time.Duration) *Counts {
	return &Counts{redisClient: rds, key: key, ttl: ttl}
}

// OnRequest increments the request counter.
// fix #9: TTL is refreshed on every write so the key lives as long as traffic flows.
func (c *Counts) OnRequest(ctx context.Context) error {
	_, err := c.redisClient.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.HIncrBy(ctx, c.key, requests, 1)
		if c.ttl > 0 {
			p.Expire(ctx, c.key, c.ttl)
		}
		return nil
	})
	if err != nil {
		slog.Error("redis_driver: OnRequest failed", "key", c.key, "error", err) // fix #3
		return err
	}
	return nil
}

// OnSuccess atomically increments success counters and returns the new consecutive_successes.
//
// fix #2: TxPipelined (MULTI/EXEC) makes the three writes atomic — no intermediate
//
//	state is visible to concurrent readers between the increments.
//
// fix #5: capturing the HIncrBy result directly from the pipeline eliminates the
//
//	separate GetField round-trip and the TOCTOU race it introduced.
//
// fix #7: HSet replaces the deprecated HMSet (deprecated since Redis 4.0).
// fix #9: TTL refreshed.
func (c *Counts) OnSuccess(ctx context.Context) (uint32, error) {
	var consecCmd *redis.IntCmd
	_, err := c.redisClient.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.HIncrBy(ctx, c.key, totalSuccesses, 1)
		consecCmd = p.HIncrBy(ctx, c.key, consecutiveSuccesses, 1) // fix #5: capture result
		p.HSet(ctx, c.key, consecutiveFailures, 0)                 // fix #7: HSet not HMSet
		if c.ttl > 0 {
			p.Expire(ctx, c.key, c.ttl) // fix #9
		}
		return nil
	})
	if err != nil {
		slog.Error("redis_driver: OnSuccess failed", "key", c.key, "error", err) // fix #3
		return 0, err
	}
	return uint32(consecCmd.Val()), nil
}

// OnFailure atomically increments failure counters.
// fix #2: TxPipelined; fix #7: HSet; fix #9: TTL refresh.
func (c *Counts) OnFailure(ctx context.Context) error {
	_, err := c.redisClient.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.HIncrBy(ctx, c.key, totalFailures, 1)
		p.HIncrBy(ctx, c.key, consecutiveFailures, 1)
		p.HSet(ctx, c.key, consecutiveSuccesses, 0) // fix #7
		if c.ttl > 0 {
			p.Expire(ctx, c.key, c.ttl) // fix #9
		}
		return nil
	})
	if err != nil {
		slog.Error("redis_driver: OnFailure failed", "key", c.key, "error", err) // fix #3
		return err
	}
	return nil
}

// GetConsecutiveFailures returns the current consecutive failure count.
// fix #8: uses strconv.ParseUint — HIncrBy stores plain decimal integers, not JSON.
// fix #10: typed accessor replaces the generic GetField(field string) method,
//
//	eliminating the duplicated field-name constants in circuit_breaker.go.
func (c *Counts) GetConsecutiveFailures(ctx context.Context) (uint32, error) {
	return c.readUint32(ctx, consecutiveFailures)
}

// GetStateSnapshot reads state, expiry, and generation in a single HMGet round-trip.
// fix #1: exposes the Redis-stored state-machine fields so the circuit breaker
//
//	reads authoritative state from Redis instead of process-local variables.
func (c *Counts) GetStateSnapshot(ctx context.Context) (state int64, expiry time.Time, generation uint64, err error) {
	vals, err := c.redisClient.HMGet(ctx, c.key, fieldState, fieldExpiry, fieldGeneration).Result()
	if err != nil {
		slog.Error("redis_driver: GetStateSnapshot failed", "key", c.key, "error", err) // fix #3
		return 0, time.Time{}, 0, err
	}

	if v, ok := vals[0].(string); ok {
		state, _ = strconv.ParseInt(v, 10, 64) // fix #8
	}
	if v, ok := vals[1].(string); ok {
		if unix, parseErr := strconv.ParseInt(v, 10, 64); parseErr == nil && unix > 0 {
			expiry = time.Unix(unix, 0)
		}
	}
	if v, ok := vals[2].(string); ok {
		g, _ := strconv.ParseUint(v, 10, 64) // fix #8
		generation = g
	}
	return state, expiry, generation, nil
}

// NewGeneration atomically transitions circuit breaker state via a Lua CAS script.
//
// fix #1: state transition is distributed and atomic — safe across multiple instances
//
//	sharing the same Redis key.
//
// fix #4: the returned generation is the Redis-authoritative value, so the
//
//	generation mismatch guard in afterRequest works across all processes.
//
// fix #9: TTL is set inside the Lua script on every successful transition.
//
// Pass fromState=-1 to initialize a brand-new key without resetting an existing one.
// Returns (newGeneration, ok, error); ok=false means the CAS check failed.
func (c *Counts) NewGeneration(ctx context.Context, fromState, toState int64, expiry time.Time) (uint64, bool, error) {
	var expiryUnix int64
	if !expiry.IsZero() {
		expiryUnix = expiry.Unix()
	}
	ttlSec := int64(c.ttl.Seconds()) // 0 when c.ttl == 0

	result, err := newGenerationScript.Run(
		ctx, c.redisClient, []string{c.key},
		fromState, toState, expiryUnix, ttlSec,
	).Int64()
	if err != nil {
		slog.Error("redis_driver: NewGeneration script failed",
			"key", c.key, "from", fromState, "to", toState, "error", err) // fix #3
		return 0, false, fmt.Errorf("redis_driver: NewGeneration: %w", err)
	}

	if result == -1 {
		// fix #4: CAS failed because a peer already transitioned; caller re-reads state.
		slog.Debug("redis_driver: NewGeneration CAS failed (state changed by peer)",
			"key", c.key, "from", fromState, "to", toState)
		return 0, false, nil
	}

	slog.Info("redis_driver: state transition committed",
		"key", c.key, "from", fromState, "to", toState, "generation", result) // fix #1
	return uint64(result), true, nil
}

// readUint32 fetches a single integer hash field.
// fix #8: strconv.ParseUint instead of json.Unmarshal.
func (c *Counts) readUint32(ctx context.Context, field string) (uint32, error) {
	result, err := c.redisClient.HGet(ctx, c.key, field).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		slog.Error("redis_driver: HGet failed", "key", c.key, "field", field, "error", err) // fix #3
		return 0, err
	}
	val, err := strconv.ParseUint(result, 10, 32) // fix #8
	if err != nil {
		return 0, fmt.Errorf("redis_driver: parse %s=%q: %w", field, result, err)
	}
	return uint32(val), nil
}
