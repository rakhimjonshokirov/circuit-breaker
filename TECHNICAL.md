# Technical Deep Dive

This document explains the internal design of the circuit breaker — how it works, why every decision was made, and what happens at each layer.

---

## Table of Contents

1. [High-level architecture](#1-high-level-architecture)
2. [Data model: what lives where](#2-data-model-what-lives-where)
3. [The local-first hot path](#3-the-local-first-hot-path)
4. [The Lua CAS script — the heart of distribution](#4-the-lua-cas-script--the-heart-of-distribution)
5. [State transitions step by step](#5-state-transitions-step-by-step)
6. [The generation counter](#6-the-generation-counter)
7. [Background sync goroutine](#7-background-sync-goroutine)
8. [Degraded mode — Redis unavailability](#8-degraded-mode--redis-unavailability)
9. [Multi-pod reconciliation](#9-multi-pod-reconciliation)
10. [Deployment restart behaviour](#10-deployment-restart-behaviour)
11. [Performance model](#11-performance-model)
12. [Concurrency guarantees](#12-concurrency-guarantees)

---

## 1. High-level architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Pod (one process)                                          │
│                                                             │
│  ┌──────────────┐    hot path      ┌───────────────────┐   │
│  │  Your code   │ ──────────────►  │  CircuitBreaker   │   │
│  │  Execute()   │                  │                   │   │
│  └──────────────┘                  │  cacheMu RWMutex  │   │
│                                    │  cache struct     │   │  ← in-process memory
│                                    │  localConsecFail  │   │
│                                    │  localConsecSucc  │   │
│                                    └────────┬──────────┘   │
│                                             │               │
│                                      background goroutine   │
│                                      every 100ms            │
└─────────────────────────────────────────────────────────────┘
                                             │
                                             │  GetStateSnapshot (read)
                                             │  NewGeneration Lua CAS (write, rare)
                                             ▼
                              ┌──────────────────────────────┐
                              │  Redis hash key              │
                              │                              │
                              │  state_val       = 0/1/2     │
                              │  generation      = N         │
                              │  expiry_unix     = unix ts   │
                              │  requests        = 0         │
                              │  total_successes = 0         │
                              │  total_failures  = 0         │
                              │  consec_success  = 0         │
                              │  consec_failures = 0         │
                              └──────────────────────────────┘
                                             ▲
                              same goroutine pattern on every other pod
```

The key insight: **your request never waits for Redis**. Redis is only touched in the background, once every `SyncInterval` per pod.

---

## 2. Data model: what lives where

### In Redis (durable, shared across pods)

All data lives in a single Redis hash under one key (e.g. `myservice:cb`).

| Field | Type | Meaning |
|---|---|---|
| `state_val` | integer | Current state: `0`=Closed, `1`=HalfOpen, `2`=Open |
| `generation` | integer | Monotonically increasing counter. Incremented on every state transition. Used as a distributed version stamp. |
| `expiry_unix` | integer | Unix timestamp when the current state expires and should transition. `0` means no expiry. |
| `requests` | integer | Total requests in this generation (reset to 0 on each transition). |
| `total_successes` | integer | Total successes in this generation. |
| `total_failures` | integer | Total failures in this generation. |
| `consecutive_successes` | integer | Reset to 0 on any failure. |
| `consecutive_failures` | integer | Reset to 0 on any success. |

**Important:** counters (`requests`, `total_*`, `consecutive_*`) are NOT incremented on every request. They are reset to 0 by the Lua script on every state transition. Between transitions, the per-request counting is done entirely in process memory.

### In process memory (fast, local, ephemeral)

```go
type CircuitBreaker struct {
    cacheMu         sync.RWMutex   // protects cache
    cache           stateSnapshot  // local copy of Redis state machine fields
    localConsecFail atomic.Int64   // failure counter — never written to Redis
    localConsecSucc atomic.Int64   // success counter — never written to Redis
    transitioning   atomic.Bool    // deduplication flag for concurrent transitions
    degraded        atomic.Bool    // true while Redis is unreachable
}

type stateSnapshot struct {
    state      State     // copied from Redis state_val
    generation uint64    // copied from Redis generation
    expiry     time.Time // copied from Redis expiry_unix
}
```

The `cache` struct is the pod's local view of what Redis holds. It is the only thing the hot path reads. The background goroutine is responsible for keeping it fresh.

---

## 3. The local-first hot path

Every call to `Execute` passes through two functions: `beforeRequest` and `afterRequest`. Neither ever touches Redis.

### `beforeRequest` — allow or reject

```go
func (cb *CircuitBreaker) beforeRequest() (uint64, error) {
    cb.cacheMu.RLock()
    snap := cb.cache        // struct copy — 3 fields, ~40 bytes
    cb.cacheMu.RUnlock()

    if snap.state == StateOpen {
        return snap.generation, ErrOpenState
    }
    return snap.generation, nil
}
```

**Cost: one `RLock` + one struct copy ≈ 14 ns. Zero Redis I/O.**

The function returns the current `generation` alongside the allow/reject decision. This generation is passed to `afterRequest` and used as a guard to discard stale results (explained in section 6).

### `afterRequest` — record the result

```go
func (cb *CircuitBreaker) afterRequest(ctx context.Context, before uint64, success bool) {
    cb.cacheMu.RLock()
    snap := cb.cache
    cb.cacheMu.RUnlock()

    if snap.generation != before {
        return  // stale — state changed between before and after, discard
    }

    if success {
        cb.localConsecFail.Store(0)
        newSucc := cb.localConsecSucc.Add(1)
        if snap.state == StateHalfOpen && uint32(newSucc) >= cb.maxRequests {
            cb.tryTransition(ctx, StateHalfOpen, StateClosed, now)
        }
        return
    }

    cb.localConsecSucc.Store(0)
    newFail := cb.localConsecFail.Add(1)

    switch snap.state {
    case StateClosed:
        if cb.readyToTrip(LocalCounts{ConsecutiveFailures: uint32(newFail)}) {
            cb.tryTransition(ctx, StateClosed, StateOpen, now)
        }
    case StateHalfOpen:
        cb.tryTransition(ctx, StateHalfOpen, StateOpen, now)
    }
}
```

**Cost: one `RLock` + one or two `atomic.Int64` operations ≈ 30 ns. Zero Redis I/O in steady state.**

Redis is only touched inside `tryTransition`, which is called only when a state change is needed — rare compared to the millions of requests per second that go through the check above.

### Why `atomic.Int64` instead of a mutex-protected field

`atomic.Int64.Add` and `atomic.Int64.Store` are single CPU instructions (on amd64/arm64). They cost ~5 ns and produce zero allocations. A mutex for the same operation would be ~20 ns and block other goroutines.

---

## 4. The Lua CAS script — the heart of distribution

This is the most critical piece. Every state transition across every pod goes through this script.

```lua
local key         = KEYS[1]
local from_state  = tonumber(ARGV[1])   -- expected current state (-1 = init sentinel)
local to_state    = tonumber(ARGV[2])   -- desired next state
local expiry_unix = tonumber(ARGV[3])   -- unix timestamp for next timeout (0 = none)
local ttl_seconds = tonumber(ARGV[4])   -- Redis key TTL (0 = no expiry)

local raw_state = redis.call('HGET', key, 'state_val')
local raw_gen   = redis.call('HGET', key, 'generation')

local cur_state = (raw_state ~= false) and tonumber(raw_state) or 0
local cur_gen   = (raw_gen   ~= false) and tonumber(raw_gen)   or 0

-- Special sentinel: fromState == -1 means "initialize if key does not exist yet"
if from_state == -1 then
    if raw_gen ~= false then
        return cur_gen   -- key already exists, do nothing, return current gen
    end
elseif cur_state ~= from_state then
    return -1            -- CAS FAILED: Redis state no longer matches what we expected
end

-- CAS passed: write new state atomically
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
```

### Why Lua and not a transaction?

Redis `MULTI/EXEC` (transactions) are optimistic — they detect key changes via `WATCH` but still require two round-trips (WATCH + EXEC). A Lua script runs atomically as a single command on the Redis server. No other command can interleave between the `HGET` reads and the `HSET` write. This gives us a true compare-and-swap in one network round-trip.

### The CAS logic

The script reads `state_val` from Redis and compares it to `from_state` (the state the caller expects). If they don't match, another pod already transitioned — return `-1`. If they match, write the new state and increment `generation`.

```
Pod A (Closed→Open):            Pod B (Closed→Open, concurrent):
  HGET state_val → 0              HGET state_val → 0   (reads before A writes)
  cur_state == from_state ✓       cur_state == from_state ✓
  HSET state_val=2, gen=2         HSET state_val=2, gen=? ← BLOCKED
                                  (Lua is atomic — B waits for A to finish)
                                  A's write completed, gen=2
                                  B reads state_val=2, from_state=0
                                  cur_state(2) != from_state(0) → return -1
```

Pod B gets `-1`, calls `refreshCache`, and adopts the state Pod A wrote. `onStateChange` fires exactly once.

### The `-1` sentinel (initialization)

When `fromState = -1`, the script checks if the key already exists (`raw_gen ~= false`). If it exists, it does nothing and returns the current generation. This is safe for concurrent pod startups — all pods call `NewGeneration(-1, Closed, ...)` at boot, but only the first one initializes the key. The rest see the key exists and get the current generation back without overwriting anything.

### Script caching (`EVALSHA`)

`redis.NewScript` in go-redis automatically uses `EVALSHA` (cached script by SHA1) on the first call. If the script isn't in Redis's cache (e.g. after a Redis restart), it falls back to `EVAL` and re-caches it. This is transparent — the caller always uses `script.Run(...)`.

---

## 5. State transitions step by step

### Closed → Open (trip)

```
1. Request fails in afterRequest()
2. localConsecFail.Add(1) → new count
3. readyToTrip(LocalCounts{ConsecutiveFailures: count}) → true
4. tryTransition(ctx, Closed, Open, now)
   └── transitioning.CompareAndSwap(false, true) → acquired
       setState(ctx, Closed, Open, now)
       └── GetStateSnapshot() → reads Redis: state=0, gen=N
           State(0) == Closed ✓ (matches from)
           NewGeneration(ctx, 0, 2, expiry=now+Timeout)
           └── Lua CAS: cur_state==0==from_state ✓
                        new_gen = N+1
                        HSET state_val=2 expiry_unix=T generation=N+1 ...reset counters...
                        returns N+1
           cache = {state: Open, generation: N+1, expiry: now+Timeout}
           localConsecFail.Store(0)
           localConsecSucc.Store(0)
           onStateChange("myservice", Closed, Open)
       transitioning.Store(false)
```

### Open → HalfOpen (timeout)

This is not triggered by a request — it is driven by the background goroutine.

```
1. syncLoop ticker fires
2. tick(now)
   └── cacheMu.RLock() → snap = {state: Open, expiry: T}
       snap.expiry.Before(now) → true
       tryTransition(ctx, Open, HalfOpen, now)
       └── setState: GetStateSnapshot, NewGeneration(0→2, 2→1, expiry=zero)
           cache = {state: HalfOpen, generation: N+2, expiry: zero}
3. refreshCache() also called in tick — syncs with Redis
```

### HalfOpen → Closed (recovery)

```
1. Probe request succeeds in afterRequest()
2. localConsecSucc.Add(1) → newSucc
3. snap.state == HalfOpen && uint32(newSucc) >= maxRequests → true
4. tryTransition(ctx, HalfOpen, Closed, now)
   └── NewGeneration(1→1, 0=Closed, expiry depending on Interval)
       cache = {state: Closed, generation: N+3}
```

### HalfOpen → Open (probe fails)

```
1. Probe request fails in afterRequest()
2. snap.state == HalfOpen → tryTransition(ctx, HalfOpen, Open, now)
   └── same path as Closed→Open above
```

---

## 6. The generation counter

The generation counter solves a subtle distributed race condition.

### The problem without generations

```
Time →
T1: Request R starts. beforeRequest() → snap.state=Closed, allowed.
T2: Circuit trips: Closed→Open. Cache updated.
T3: Request R finishes. afterRequest() → records SUCCESS.
    But this success happened AFTER the trip — it is stale.
    Without a guard, this success would decrement the failure counter
    for the NEW generation, potentially preventing the next trip.
```

### The solution

`beforeRequest` returns the generation at the time of the decision. `afterRequest` re-reads the current generation and compares:

```go
if snap.generation != before {
    return  // state changed between before and after — discard result
}
```

If the generation changed (a transition happened while the request was in flight), the result is silently dropped. The request's outcome is irrelevant because it ran under a different circuit breaker state.

### Cross-pod guarantee

The generation is Redis-authoritative: it is written by the Lua CAS script and is unique globally, not per-pod. When Pod B's background sync reads `generation=5` from Redis after Pod A tripped the circuit, Pod B's `afterRequest` correctly discards any in-flight requests that started before the sync with `generation < 5`.

### Degraded-mode generation

When Redis is unreachable, the generation is incremented locally:

```go
func (cb *CircuitBreaker) applyStateLocally(from, to State, now time.Time) {
    cb.cacheMu.Lock()
    newGen := cb.cache.generation + 1   // local increment, no Redis
    cb.cache = stateSnapshot{state: to, generation: newGen, expiry: newExpiry}
    cb.cacheMu.Unlock()
}
```

This still prevents stale results within the pod. The cross-pod guarantee is temporarily lost (only one pod is making decisions), but it is restored during reconciliation when Redis comes back.

---

## 7. Background sync goroutine

Every `CircuitBreaker` starts exactly one goroutine at construction time.

```go
func (cb *CircuitBreaker) syncLoop() {
    ticker := time.NewTicker(cb.syncInterval)  // default 100ms
    defer ticker.Stop()
    for {
        select {
        case <-cb.stopSync:
            return
        case now := <-ticker.C:
            cb.tick(now)
        }
    }
}
```

### What `tick` does

```
tick(now)
 │
 ├── 1. cacheMu.RLock() → read local cache (no Redis)
 │      Check time-based transitions:
 │      ├── state==Closed && expiry non-zero && expiry.Before(now)
 │      │     → tryTransition(Closed→Closed)  [resets counts for new interval]
 │      └── state==Open && expiry non-zero && expiry.Before(now)
 │            → tryTransition(Open→HalfOpen)
 │
 └── 2. refreshCache(ctx)
         └── GetStateSnapshot() → one Redis round-trip
             ├── if degraded==true: reconcileWithRedis()
             └── if degraded==false: applyRedisSnapshot()
                   updates cache.state / cache.generation / cache.expiry
                   if generation changed: reset local atomic counters
```

### Redis load calculation

At `SyncInterval = 100ms`:
- 1 `GetStateSnapshot` call per tick = 1 `HMGET` command
- 10 ticks/sec × 1 HMGET = **10 Redis reads/sec per pod**
- 3 pods = 30 reads/sec total — regardless of whether the service handles 1 RPS or 1,000,000 RPS

State transitions add 1-2 extra Redis commands but happen rarely (only when the circuit trips or recovers).

---

## 8. Degraded mode — Redis unavailability

### Entering degraded mode

Redis failure is detected at two points:

1. **Startup**: `NewCircuitBreaker` calls `NewGeneration(-1, ...)`. If Redis returns an error, `degraded.Store(true)` is set immediately.
2. **During operation**: `refreshCache` (called by `tick`) calls `GetStateSnapshot`. If it errors, `enterDegraded(err)` is called.

```go
func (cb *CircuitBreaker) enterDegraded(err error) {
    if cb.degraded.CompareAndSwap(false, true) {
        // CompareAndSwap ensures this log line fires ONCE per outage, not every 100ms
        slog.Warn("circuit_breaker: entering degraded mode", "name", cb.name, "error", err)
    }
}
```

`CompareAndSwap(false, true)` returns `true` only the first time — subsequent calls during the same outage are no-ops. This prevents log spam at 10 messages/sec during a Redis outage.

### Behaviour during outage

When a state transition is needed and Redis is down:

```go
func (cb *CircuitBreaker) setState(ctx, from, to, now) error {
    stateVal, _, _, err := cb.counts.GetStateSnapshot(ctx)
    if err != nil {
        cb.enterDegraded(err)
        cb.applyStateLocally(from, to, now)  // write to in-process cache only
        return nil
    }
    // ... normal Redis path
}
```

`applyStateLocally` increments the local generation and updates the cache struct under `cacheMu.Lock()`. The downstream service continues to be protected — the circuit breaker still trips, still rejects requests, still transitions to HalfOpen after timeout. It just does all of this in-process only.

---

## 9. Multi-pod reconciliation

When Redis comes back after an outage, the background goroutine detects it:

```go
func (cb *CircuitBreaker) refreshCache(ctx context.Context) error {
    stateVal, expiry, gen, err := cb.counts.GetStateSnapshot(ctx)
    if err != nil {
        cb.enterDegraded(err)
        return err
    }
    if cb.degraded.CompareAndSwap(true, false) {
        // First successful read after outage
        slog.Info("circuit_breaker: Redis reconnected, reconciling state")
        return cb.reconcileWithRedis(ctx, State(stateVal), expiry, gen)
    }
    cb.applyRedisSnapshot(stateVal, expiry, gen)
    return nil
}
```

`CompareAndSwap(true, false)` fires only once — the moment `degraded` transitions from `true` back to `false`. This triggers `reconcileWithRedis` exactly once per recovery.

### Reconciliation rule: most protective state wins

```
Open (2) > HalfOpen (1) > Closed (0)
```

```go
func (cb *CircuitBreaker) reconcileWithRedis(ctx, redisState, redisExpiry, redisGen) error {
    localSnap := cb.cache  // read under RLock

    if localSnap.state > redisState {
        // Local is more protective (e.g. this pod tripped while Redis was down)
        // Push local state to Redis so all peers converge
        gen, ok, err := cb.counts.NewGeneration(ctx,
            int64(redisState),     // from: what Redis currently holds
            int64(localSnap.state), // to:   what this pod observed
            newExpiry,
        )
        if ok {
            // Our CAS won — update cache with Redis-authoritative generation
            cb.cache = stateSnapshot{state: localSnap.state, generation: gen}
            return nil
        }
        // CAS lost — another pod raced us and already wrote a state
        // Fall through to adopt Redis
    }

    // Redis is same or more protective — adopt it
    cb.cache = stateSnapshot{state: redisState, generation: redisGen, expiry: redisExpiry}
    cb.localConsecFail.Store(0)
    cb.localConsecSucc.Store(0)
}
```

### Scenarios

**Scenario A: This pod tripped during outage, Redis still shows Closed**
```
Local: Open (gen=local+1)    Redis: Closed (gen=N)
localSnap.state(2) > redisState(0) → push Open to Redis
NewGeneration(from=0, to=2) → CAS succeeds → gen=N+1
All other pods pick up Open on their next tick (within 100ms)
```

**Scenario B: Another pod tripped Redis while this pod was degraded**
```
Local: Closed (gen=N, unchanged during outage)    Redis: Open (gen=N+3)
localSnap.state(0) < redisState(2) → adopt Redis
cache = {state: Open, generation: N+3}
onStateChange fires: Closed → Open
```

**Scenario C: Both pods tripped locally, race to push to Redis**
```
Pod A: NewGeneration(from=Closed, to=Open) → CAS wins, gen=N+1
Pod B: NewGeneration(from=Closed, to=Open) → CAS fails (Redis is already Open)
Pod B falls through to adopt Redis: cache = {state: Open, gen=N+1}
Both pods end up in Open. onStateChange fires once per pod.
```

---

## 10. Deployment restart behaviour

### What persists across a restart

The Redis hash key persists as long as the TTL hasn't expired. On startup:

```
NewCircuitBreaker()
 │
 ├── NewGeneration(fromState=-1, toState=Closed, expiry)
 │    └── Lua: from_state==-1 → check if raw_gen != false
 │         key EXISTS → return cur_gen (no write, no reset)   ← safe restart
 │         key MISSING → HSET all fields to Closed/gen=1      ← fresh start
 │
 └── refreshCache()
      └── GetStateSnapshot() → reads current state_val, expiry_unix, generation
          cache = whatever Redis holds
          (if Redis has Open, pod starts in Open state immediately)
```

### All pods restart simultaneously

All pods call `NewGeneration(-1, ...)` concurrently. The Lua script handles this correctly:

- First pod to reach Redis: `raw_gen == false` → initializes key, returns `gen=1`
- All subsequent pods: `raw_gen ~= false` (key exists) → returns `cur_gen`, no write

No matter how many pods call `NewGeneration(-1, ...)` simultaneously, the key is initialized at most once.

### Counter reset after restart

`localConsecFail` and `localConsecSucc` are `atomic.Int64` fields in process memory. They start at `0` on every pod boot. If the circuit was in `Closed` state and had accumulated 8 consecutive failures before restart, the new pod needs 10 fresh failures to trip again. This is an intentional trade-off: paying the price of losing in-flight counters on restart in exchange for zero per-request Redis I/O.

State (`Open`/`HalfOpen`/`Closed`) and its expiry timestamp are fully durable in Redis and are inherited immediately on startup.

---

## 11. Performance model

| Operation | Cost | Redis I/O |
|---|---|---|
| `beforeRequest` | ~14 ns | 0 |
| `afterRequest` (no transition) | ~30 ns | 0 |
| `afterRequest` (transition triggered) | ~30 ns + 1-2 Redis RTT | 1-2 |
| Background sync tick | 0 ns (goroutine) | 1 `HMGET` per `SyncInterval` |
| State transition (Lua CAS) | 1-2 Redis RTT | 1 `EVALSHA` + optional `HMGET` |

### Why zero allocations on the hot path

- `beforeRequest`: `RLock` + struct copy — the struct is value-typed (no pointers inside `stateSnapshot`), copy is stack-allocated.
- `afterRequest`: `RLock` + struct copy + `atomic.Int64.Add/Store` — all stack or register operations.
- `Execute` itself: wraps the two functions above. The `defer` for panic recovery is stack-allocated by the Go compiler.

The `b.ReportAllocs()` benchmark confirms `0 B/op, 0 allocs/op` on all hot-path benchmarks.

### Throughput calculation

At 8 goroutines on Apple M1 Pro:
- `BenchmarkHotPath_Closed_Parallel`: ~304 ns/op
- Throughput per goroutine: 1s / 304ns ≈ 3.3M ops/sec
- 8 goroutines: ~26M Execute() calls/sec on a single process

At 100k RPS (100,000 requests/sec), the circuit breaker overhead is negligible.

---

## 12. Concurrency guarantees

### Within a single process

| Race | Guard |
|---|---|
| Multiple goroutines reading `cache` | `cacheMu.RWMutex` — unlimited concurrent readers |
| Multiple goroutines writing `cache` | `cacheMu.Lock()` — exclusive write lock, only during transitions and sync |
| Multiple goroutines incrementing `localConsecFail` | `atomic.Int64.Add` — lock-free |
| Multiple goroutines attempting a transition simultaneously | `transitioning.CompareAndSwap(false, true)` — only one proceeds |

### Across pods

| Race | Guard |
|---|---|
| Two pods simultaneously try to trip the circuit | Lua CAS: only one `NewGeneration` with `fromState=Closed` succeeds; the other gets `-1` |
| One pod transitions while another is mid-request | Generation guard in `afterRequest`: stale results from pre-transition requests are discarded |
| Two pods restart simultaneously and try to initialize the key | Lua init sentinel (`fromState=-1`): only the first write initializes; subsequent calls are no-ops |
| One pod reconciles after outage while another is already running | Reconcile uses `NewGeneration` which is itself a CAS — loses to first writer, adopts Redis state |

### The `transitioning` flag scope

`transitioning` deduplicates within one process only. If 1,000 goroutines simultaneously detect that the failure threshold is crossed, only one calls `setState`. The other 999 return immediately from `tryTransition`. This prevents 999 unnecessary Redis round-trips.

Cross-process deduplication is handled by the Lua CAS: even if Pod A and Pod B both pass the `transitioning` check (they have independent flags), only one Lua CAS succeeds.

```
Pod A                    Redis                   Pod B
  │  transitioning=true    │   transitioning=true  │
  │── NewGeneration ───────►                        │
  │                        │◄─── NewGeneration ─────│
  │                        │  A's script running    │
  │◄── gen=N+1, ok=true ───│  B waits               │
  │   cache updated        │                        │
  │                        │──── return -1 ─────────►│
  │                        │  (CAS failed)          │── refreshCache
  │                        │                        │   adopts A's state
```
