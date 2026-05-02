# circuit-breaker

A production-grade, Redis-backed circuit breaker for Go services.

**Zero Redis I/O on the hot path** — every request decision is a pure memory operation (~14 ns `beforeRequest`, ~70 ns full `Execute` at 100k+ RPS). Redis is used only for distributed state synchronization in the background.

## Features

- **Local-first hot path** — state is cached in memory behind a `sync.RWMutex`; requests never block on Redis.
- **Distributed state machine** — state transitions use a Lua compare-and-swap script, safe across any number of pods sharing one Redis key.
- **Background sync** — a goroutine polls Redis every `SyncInterval` (default 100 ms); cross-pod propagation ≤ one interval, ~10 Redis reads/sec per instance regardless of RPS.
- **Degraded mode** — when Redis is unavailable the breaker keeps protecting the downstream service with local-only transitions. On reconnection it reconciles: the more protective state (`Open > HalfOpen > Closed`) wins.
- **New-pod inheritance** — a pod starting after the breaker has been tripped reads the existing Open state from Redis immediately (no warm-up period).
- **Zero allocations** — `Execute` allocates nothing on the hot path.
- **Structured logging** — `log/slog` (Go 1.21+), distinct log entries for every significant event.

## Documentation

| Document | Description |
|---|---|
| [Technical Deep Dive](docs/technical-deep-dive.md) | Internal architecture, Lua CAS script, hot-path analysis, concurrency guarantees, degraded mode, multi-pod reconciliation |

## Requirements

- Go 1.24+
- Redis 4.0+ (uses `HSET`, `MULTI/EXEC`, Lua scripting)

## Installation

```sh
go get github.com/rakhimjonshokirov/circuit-breaker
```

## Quick start

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/redis/go-redis/v9"
    circuitbreaker "github.com/rakhimjonshokirov/circuit-breaker/circuit_breaker"
    redisdriver "github.com/rakhimjonshokirov/circuit-breaker/circuit_breaker/redis_driver"
)

func main() {
    rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

    cb := circuitbreaker.NewCircuitBreaker(circuitbreaker.Settings{
        Redis:        redisdriver.New(rdb, "myservice:cb", time.Hour),
        Name:         "myservice",
        MaxRequests:  5,                 // probe requests allowed in HalfOpen
        Timeout:      30 * time.Second,  // how long to stay Open before probing
        Interval:     60 * time.Second,  // Closed-state counter reset period (0 = never)
        SyncInterval: 100 * time.Millisecond,
        ReadyToTrip: func(c circuitbreaker.LocalCounts) bool {
            return c.ConsecutiveFailures >= 10
        },
        OnStateChange: func(name string, from, to circuitbreaker.State) {
            log.Printf("circuit breaker %s: %s → %s", name, from, to)
        },
    })
    defer cb.Stop()

    result, err := cb.Execute(context.Background(), func() (any, error) {
        // your downstream call here
        return callDownstream()
    })
    _ = result
    if err != nil {
        log.Println("request failed or circuit is open:", err)
    }
}
```

## Settings reference

| Field | Type | Default | Description |
|---|---|---|---|
| `Redis` | `Driver` | **required** | Redis driver (see `redis_driver.New`). |
| `Name` | `string` | `""` | Human-readable name, included in every log entry. |
| `MaxRequests` | `uint32` | `1` | Max probe requests allowed in `HalfOpen`. |
| `Interval` | `time.Duration` | `0` | Closed-state counter reset period. `0` = never reset. |
| `Timeout` | `time.Duration` | `60s` | How long to stay `Open` before moving to `HalfOpen`. |
| `SyncInterval` | `time.Duration` | `100ms` | How often the background goroutine reads Redis. Lower = faster cross-pod propagation, more Redis reads. |
| `ReadyToTrip` | `func(LocalCounts) bool` | `failures > 10` | Called on every failure in `Closed` state. Return `true` to trip. |
| `OnStateChange` | `func(name, from, to)` | `nil` | Callback fired after every state transition. |
| `IsSuccessful` | `func(error) bool` | `err == nil` | Override to treat certain errors as successes (e.g. `context.Canceled`). |

## State machine

```
          failures ≥ threshold
  Closed ─────────────────────► Open
    ▲                              │
    │    timeout elapsed           │
    │   ◄──────────────── HalfOpen ◄┘
    │
    └── maxRequests successes ── HalfOpen → Closed
```

| State | Behavior |
|---|---|
| `Closed` | All requests pass through. Failures are counted locally. |
| `Open` | All requests are rejected immediately with `ErrOpenState`. After `Timeout`, transitions to `HalfOpen`. |
| `HalfOpen` | Up to `MaxRequests` probe requests are allowed. On success → `Closed`; on failure → `Open`. |

## Architecture: why Redis is never on the hot path

```
   Request
      │
      ▼
 beforeRequest()         ← RLock + struct copy        ~14 ns  0 Redis
      │
      │  (allowed)
      ▼
 user function runs
      │
      ▼
 afterRequest()          ← RLock + 1-2 atomic ops     ~30 ns  0 Redis
      │
      │  (transition needed — rare)
      ▼
 tryTransition()         ← atomic CAS (dedup)
      │
      ▼
 NewGeneration Lua CAS   ← 1 Redis round-trip (rare)

────────────────────────────────────────────────────────────
 Background goroutine (every SyncInterval, default 100 ms):
   GetStateSnapshot()    ← 1 Redis round-trip
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

### Redis data layout

All state is stored in a single Redis hash key:

| Field | Value |
|---|---|
| `state_val` | `0` = Closed, `1` = HalfOpen, `2` = Open |
| `expiry_unix` | Unix timestamp of next time-based transition, or `0` |
| `generation` | Monotonically increasing counter; used as a distributed generation guard |
| `requests` | Total request count in this generation |
| `total_successes` | Total successes in this generation |
| `total_failures` | Total failures in this generation |
| `consecutive_successes` | Resets to 0 on any failure |
| `consecutive_failures` | Resets to 0 on any success |

State transitions use a Lua script (`EVALSHA`) to atomically compare-and-swap `state_val` + `generation` + all counters in a single round-trip. The `fromState = -1` sentinel initializes a new key without overwriting an existing one, enabling safe pod restarts.

## Degraded mode

When Redis becomes unreachable:

1. The breaker detects the error on the next `GetStateSnapshot` call (background goroutine or a transition attempt).
2. It sets `degraded = true` and logs **once** per outage (no per-tick spam).
3. Transitions are applied locally: `state` and `generation` are incremented in the in-process cache.
4. The downstream service continues to be protected.

On reconnection:

1. The background goroutine's next `refreshCache` call succeeds.
2. `reconcileWithRedis` runs with the rule: **more protective state wins** (`Open > HalfOpen > Closed`).
3. If the local state is more open than Redis (this pod tripped during the outage), the local state is pushed to Redis via Lua CAS so all other pods converge within one `SyncInterval`.
4. If Redis is same or more open, the pod adopts the Redis state.
5. `degraded` is set to `false` and **one** recovery log entry is emitted.

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
    │── 3 failures ──────────►                        │
    │◄── gen=2, state=Open ──│                        │
    │                        │                        │
    │                        │◄── GetStateSnapshot ───│  (next SyncInterval)
    │                        │─── state=Open, gen=2 ──►│
    │                        │                        │── adopts Open state
```

- **Trip propagation**: within one `SyncInterval` of the tripping pod committing to Redis.
- **New pod startup**: `NewGeneration(-1, Closed, ...)` finds the existing key and returns the current generation. `refreshCache` immediately loads the live state (e.g. `Open`). The new pod inherits circuit state without any warm-up period.
- **CAS deduplication**: if two pods race to trip simultaneously, the Lua CAS ensures `NewGeneration` succeeds on exactly one of them. The losing pod calls `refreshCache` and adopts the winner's state.

## Observability

All log output uses `log/slog` at the following levels:

| Level | Event |
|---|---|
| `INFO` | Initialized, state changed, state synced from Redis, stopped, reconcile decisions |
| `WARN` | Entering degraded mode, local transition during outage |
| `DEBUG` | Stale generation discarded, CAS miss, HalfOpen threshold details |
| `ERROR` | Redis write failures |

To silence logs in tests, replace the default handler:

```go
slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
```

## Testing

### Unit tests (no Redis required)

```sh
go test -v -race ./circuit_breaker/...
```

The test suite uses an in-process `fakeDriver` that mirrors the Lua CAS semantics without a network hop. Tests drive time-based transitions with `cb.tick(t)` instead of sleeping.

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
