# circuit-breaker

A production-grade, Redis-backed circuit breaker for Go services.

**Zero Redis I/O on the hot path** — every request decision is a pure memory operation (~1 ns `beforeRequest` via `atomic.Pointer.Load`, ~70 ns full `Execute` at 100k+ RPS). Redis is used only for distributed state synchronization in the background.

## Features

- **Local-first hot path** — state is cached behind an `atomic.Pointer`; hot-path reads are a single atomic load with no mutex and no Redis I/O.
- **Distributed state machine** — state transitions use a Lua compare-and-swap script, safe across any number of pods sharing one Redis key.
- **Background sync** — a goroutine polls Redis every `SyncInterval` (default 100 ms); cross-pod propagation ≤ one interval, ~10 Redis reads/sec per instance regardless of RPS.
- **Degraded mode** — when Redis is unavailable the breaker keeps protecting the downstream service with local-only transitions. On reconnection it reconciles: the more protective state (`Open > HalfOpen > Closed`) wins.
- **New-pod inheritance** — a pod starting after the breaker has been tripped reads the existing Open state from Redis immediately (no warm-up period).
- **Zero allocations** — `Execute` allocates nothing on the hot path (`0 B/op, 0 allocs/op`).
- **Structured logging** — `log/slog` (Go 1.24+), distinct log entries for every significant event.

## Documentation

| Document | Description |
|---|---|
| [Technical Deep Dive](docs/technical-deep-dive.md) | Internal architecture, Lua CAS script, hot-path analysis, concurrency guarantees, degraded mode, multi-pod reconciliation |

## Requirements

- Go 1.24+
- Redis 4.0+ (uses `HSET`, Lua scripting via `EVALSHA`)

## Installation

```sh
go get github.com/rakhimjonshokirov/circuit-breaker
```

## Quick start

```go
package main

import (
    "context"
    "errors"
    "log"
    "time"

    "github.com/redis/go-redis/v9"
    circuitbreaker "github.com/rakhimjonshokirov/circuit-breaker/circuit_breaker"
    redisdriver "github.com/rakhimjonshokirov/circuit-breaker/circuit_breaker/redis_driver"
)

func main() {
    rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

    cb := circuitbreaker.NewCircuitBreaker(circuitbreaker.Settings{
        Driver:       redisdriver.New(rdb, "myservice:cb", time.Hour),
        Name:         "myservice",
        MaxRequests:  5,                        // max concurrent probes in HalfOpen
        Timeout:      30 * time.Second,         // how long to stay Open before probing
        Interval:     60 * time.Second,         // Closed-state counter reset period (0 = never)
        SyncInterval: 100 * time.Millisecond,
        ReadyToTrip: func(c circuitbreaker.LocalCounts) bool {
            return c.ConsecutiveFailures >= 10
        },
        OnStateChange: func(name string, from, to circuitbreaker.State) {
            log.Printf("circuit breaker %s: %s → %s", name, from, to)
        },
    })
    defer cb.Stop()

    err := cb.Execute(context.Background(), func() error {
        return callDownstream()
    })
    if err != nil {
        switch {
        case errors.Is(err, circuitbreaker.ErrOpenState):
            log.Println("circuit is open — downstream skipped")
        case errors.Is(err, circuitbreaker.ErrProbesFull):
            log.Println("HalfOpen probe slots full — retry shortly")
        default:
            log.Println("downstream error:", err)
        }
    }
}
```

### Multiple providers (one CB per downstream)

This mirrors the pattern used in production services where each upstream has its own breaker:

```go
type providerCfg struct {
    redisKey            string
    consecutiveFailures uint32
    timeout             time.Duration
    maxRequests         uint32
}

var sources = map[string]providerCfg{
    "uzcard": {redisKey: "svc:cb:uzcard", consecutiveFailures: 5, timeout: 30 * time.Second, maxRequests: 3},
    "humo":   {redisKey: "svc:cb:humo",   consecutiveFailures: 5, timeout: 30 * time.Second, maxRequests: 3},
}

breakers := make(map[string]*circuitbreaker.CircuitBreaker)
for name, cfg := range sources {
    cf := cfg // capture
    breakers[name] = circuitbreaker.NewCircuitBreaker(circuitbreaker.Settings{
        Driver:      redisdriver.New(rdb, cf.redisKey, cf.timeout*2),
        Name:        name,
        MaxRequests: cf.maxRequests,
        Timeout:     cf.timeout,
        ReadyToTrip: func(c circuitbreaker.LocalCounts) bool {
            return c.ConsecutiveFailures >= cf.consecutiveFailures
        },
    })
}
defer func() {
    for _, cb := range breakers {
        cb.Stop()
    }
}()

// Use per-request context with a timeout (not the outer lifecycle context).
reqCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
defer cancel()

err := breakers["uzcard"].Execute(reqCtx, func() error {
    return uzcardClient.GetBalance(reqCtx, cardID)
})
```

## Settings reference

| Field | Type | Default | Description |
|---|---|---|---|
| `Driver` | `Driver` | **required** | State backend. Use `redisdriver.New(rdb, key, ttl)`. |
| `Name` | `string` | `""` | Human-readable name, included in every log entry. |
| `MaxRequests` | `uint32` | `1` | Max concurrent probe requests allowed in `HalfOpen`. Enforced as an in-flight concurrency limit, not a completion count. |
| `Interval` | `time.Duration` | `0` | Closed-state counter reset period. `0` = never reset. |
| `Timeout` | `time.Duration` | `60s` | How long to stay `Open` before moving to `HalfOpen`. |
| `SyncInterval` | `time.Duration` | `100ms` | How often the background goroutine reads Redis. Lower = faster cross-pod propagation, more Redis reads. |
| `ReadyToTrip` | `func(LocalCounts) bool` | `failures >= 10` | Called on every failure in `Closed` state. Return `true` to trip. |
| `OnStateChange` | `func(name, from, to)` | `nil` | Callback fired after every state transition. |
| `IsSuccessful` | `func(error) bool` | `err == nil` | Override to treat certain errors as successes (e.g. `context.Canceled`). |

## Errors

| Error | When returned |
|---|---|
| `ErrOpenState` | Circuit is `Open` — downstream is skipped entirely. |
| `ErrProbesFull` | Circuit is `HalfOpen` and all `MaxRequests` probe slots are taken. Transient — slots free as probes complete. Retry shortly. |

`ErrProbesFull` is distinct from `ErrOpenState` so callers can implement different retry strategies:

```go
switch {
case errors.Is(err, circuitbreaker.ErrOpenState):
    return cachedResponse, nil   // hard-open: serve from cache, don't retry
case errors.Is(err, circuitbreaker.ErrProbesFull):
    time.Sleep(10 * time.Millisecond)
    return cb.Execute(ctx, req)  // slots will free up soon
}
```

## State machine

```
          failures ≥ threshold
  Closed ─────────────────────► Open
    ▲                              │
    │                              │  timeout elapsed
    │                         HalfOpen ◄┘
    │
    └── maxRequests successes in HalfOpen → Closed
```

| State | Behavior |
|---|---|
| `Closed` | All requests pass through. Failures counted locally with `atomic.Int64`. |
| `Open` | All requests rejected immediately with `ErrOpenState`. After `Timeout`, transitions to `HalfOpen`. |
| `HalfOpen` | Up to `MaxRequests` concurrent probes allowed. Excess probes get `ErrProbesFull`. On `MaxRequests` successes → `Closed`; on any failure → `Open`. |

## Architecture: why Redis is never on the hot path

```
   Request
      │
      ▼
 beforeRequest()         ← atomic.Pointer.Load()         ~1 ns   0 Redis
      │
      │  (allowed)
      ▼
 user function runs
      │
      ▼
 afterRequest()          ← atomic.Pointer.Load()          ~5 ns   0 Redis
                           + 1-2 atomic.Int64 ops
      │
      │  (transition needed — rare)
      ▼
 tryTransition()         ← atomic.Bool.CompareAndSwap (dedup)
      │
      ▼
 NewGeneration Lua CAS   ← 1 Redis round-trip (rare)

────────────────────────────────────────────────────────────
 Background goroutine (every SyncInterval, default 100 ms):
   GetStateSnapshot()    ← 1 Redis HMGET round-trip
   reconcile if needed   ← 0-1 additional round-trips
```

At the default 100 ms sync interval, each pod issues **~10 Redis reads/sec** regardless of request rate. A service running 100 000 RPS still produces only ~10 Redis reads/sec.

## Redis driver

The `redis_driver` package wraps `go-redis/v9` and implements the two-method `Driver` interface:

```go
type Driver interface {
    GetStateSnapshot(ctx context.Context) (state int64, expiry time.Time, generation uint64, err error)
    NewGeneration(ctx context.Context, fromState, toState int64, expiry time.Time) (uint64, bool, error)
}
```

### Creating a driver

```go
import redisdriver "github.com/rakhimjonshokirov/circuit-breaker/circuit_breaker/redis_driver"

// Shared key across all pods of the same service.
// ttl refreshes on every write; pass 0 to never expire.
driver := redisdriver.New(rdb, "myservice:cb", time.Hour)
```

`redis.UniversalClient` is accepted, so the driver works with standalone Redis, Sentinel, and Redis Cluster.

### Redis data layout

All state lives in a single Redis hash key. Only three fields are written:

| Field | Value |
|---|---|
| `state_val` | `0` = Closed, `1` = HalfOpen, `2` = Open |
| `expiry_unix` | Unix timestamp of the next time-based transition, or `0` |
| `generation` | Monotonically increasing counter — distributed version stamp |

State transitions use a Lua script (`EVALSHA`) to atomically compare-and-swap all three fields in a single round-trip. The `fromState = -1` sentinel initializes a new key without overwriting an existing one, enabling safe concurrent pod restarts.

Per-request counters (`consecutive_failures`, `total_successes`, etc.) are **not stored in Redis**. They are `atomic.Int64` fields in process memory, reset to `0` when the generation changes.

## Degraded mode

When Redis becomes unreachable:

1. The breaker detects the error on the next `GetStateSnapshot` call (background goroutine or transition attempt).
2. It sets `degraded = true` and logs **once** per outage window — no per-tick spam.
3. Transitions are applied locally: `state` and `generation` are updated in the in-process `atomic.Pointer`.
4. The downstream service continues to be protected.

On reconnection:

1. The background goroutine's next `refreshCache` call succeeds.
2. `reconcileWithRedis` runs: **more protective state wins** (`Open > HalfOpen > Closed`).
3. If the local state is more protective than Redis (this pod tripped during the outage), the local state is pushed via Lua CAS so all other pods converge within one `SyncInterval`.
4. If Redis is same or more protective, the pod adopts the Redis state.
5. `degraded` is set to `false` and one recovery log entry is emitted.

Check degraded status from application code:

```go
if cb.IsDegraded() {
    metrics.Increment("circuit_breaker.degraded")
}
```

## Multi-pod operation

All pods that share the same Redis key and `Driver` form a distributed circuit breaker cluster.

```
  Pod 1                    Redis                    Pod 2
    │                        │                        │
    │── 10 failures ─────────►                        │
    │◄── gen=2, state=Open ──│                        │
    │                        │                        │
    │                        │◄── GetStateSnapshot ───│  (next SyncInterval)
    │                        │─── state=Open, gen=2 ──►│
    │                        │                        │── adopts Open state
```

- **Trip propagation**: within one `SyncInterval` of the tripping pod committing to Redis.
- **New pod startup**: `NewGeneration(-1, Closed, ...)` finds the existing key and returns the current generation. `refreshCache` immediately loads the live state (e.g. `Open`). The new pod inherits circuit state without any warm-up period.
- **CAS deduplication**: if two pods race to trip simultaneously, the Lua CAS ensures `NewGeneration` succeeds on exactly one. The losing pod calls `refreshCache` and adopts the winner's state.

## Observability

All log output uses `log/slog` at the following levels:

| Level | Event |
|---|---|
| `INFO` | Initialized, state changed, state synced from Redis, stopped, reconcile decisions |
| `WARN` | Entering degraded mode, local transition during outage |
| `DEBUG` | Stale generation discarded, CAS miss, HalfOpen threshold details |
| `ERROR` | Redis write failures |

To silence logs in tests:

```go
slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
```

## Testing

### Unit tests (no Redis required)

```sh
go test -v -race ./circuit_breaker/...
```

The test suite uses an in-process `fakeDriver` that mirrors the Lua CAS semantics without a network hop. Tests drive time-based transitions by calling the internal `cb.tick(ctx, t)` instead of sleeping.

### Integration tests (Redis required)

Set `REDIS_URL` or ensure Redis is reachable at `redis://:123@localhost:6379/0`. Integration tests skip automatically when Redis is unavailable.

```sh
REDIS_URL=redis://:123@localhost:6379/0 go test -v -race ./circuit_breaker/...
```

### Benchmarks

```sh
go test -bench=. -benchmem ./circuit_breaker/
```

Expected results on Apple M-class hardware:

| Benchmark | ns/op | allocs/op | Notes |
|---|---|---|---|
| `BenchmarkBeforeRequest` | ~14 | 0 | Pure allow/reject cost |
| `BenchmarkHotPath_Closed_Sequential` | ~70 | 0 | Full `Execute`, single goroutine |
| `BenchmarkHotPath_Open_Parallel` | ~127 | 0 | Fast rejection path, 8 goroutines |
| `BenchmarkHotPath_MixedFailures_Parallel` | ~284 | 0 | 10% failure rate, 8 goroutines |
| `BenchmarkHotPath_Closed_Parallel` | ~304 | 0 | Full `Execute`, 8 goroutines |
| `BenchmarkIntegration_Closed_Parallel` | ~312 | 0 | Real Redis, same as fake — 0 hot-path I/O |

The integration benchmark matches the in-memory fake, confirming Redis is never called during request execution.

## License

MIT
