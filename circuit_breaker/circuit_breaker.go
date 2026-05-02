// Package circuitbreaker implements the Circuit Breaker pattern.
// See https://msdn.microsoft.com/en-us/library/dn589784.aspx.
package circuitbreaker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// State is a type that represents a state of CircuitBreaker.
type State int

// These constants are states of CircuitBreaker.
const (
	StateClosed State = iota
	StateHalfOpen
	StateOpen
)

const (
	defaultInterval               = time.Duration(0)
	defaultTimeout                = 60 * time.Second
	defaultSyncInterval           = 100 * time.Millisecond
	defaultMaxConsecutiveFailures = 10
)

var (
	// ErrOpenState is returned when the CB state is open.
	ErrOpenState = errors.New("circuit breaker is open")
)

// String implements stringer interface.
func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateHalfOpen:
		return "half-open"
	case StateOpen:
		return "open"
	default:
		return fmt.Sprintf("unknown state: %d", s)
	}
}

// LocalCounts is the snapshot passed to ReadyToTrip.
// It carries only local atomic values — no Redis I/O.
type LocalCounts struct {
	ConsecutiveFailures  uint32
	ConsecutiveSuccesses uint32
}

// Settings configures CircuitBreaker.
//
// Name is the name of the CircuitBreaker.
//
// MaxRequests is the maximum number of requests allowed to pass through
// when the CircuitBreaker is half-open.
// If MaxRequests is 0, the CircuitBreaker allows only 1 request.
//
// Interval is the cyclic period of the closed state
// for the CircuitBreaker to clear the internal Counts.
// If Interval is less than or equal to 0, the CircuitBreaker doesn't clear internal Counts during the closed state.
//
// Timeout is the period of the open state,
// after which the state of the CircuitBreaker becomes half-open.
// If Timeout is less than or equal to 0, the timeout value of the CircuitBreaker is set to 60 seconds.
//
// SyncInterval controls how often the background goroutine re-reads Redis
// to pick up transitions made by other instances. Default: 100ms.
// Lower values = faster cross-instance propagation but more Redis reads.
// At the default each instance issues ~10 Redis reads/sec regardless of RPS.
//
// ReadyToTrip is called with local counters whenever a request fails in the closed state.
// It receives a LocalCounts value — no Redis I/O, safe to call on every failure.
// If ReadyToTrip is nil, default ReadyToTrip is used.
// Default ReadyToTrip returns true when consecutive failures exceed 10.
//
// OnStateChange is called whenever the state of the CircuitBreaker changes.
//
// IsSuccessful is called with the error returned from a request.
// If IsSuccessful returns true, the error is counted as a success.
// Otherwise the error is counted as a failure.
// If IsSuccessful is nil, default IsSuccessful is used, which returns false for all non-nil errors.
type Settings struct {
	Redis Driver

	Name         string
	MaxRequests  uint32
	Interval     time.Duration
	Timeout      time.Duration
	SyncInterval time.Duration

	ReadyToTrip   func(counts LocalCounts) bool
	OnStateChange func(name string, from State, to State)
	IsSuccessful  func(err error) bool
}

// Driver is the minimal Redis interface the circuit breaker needs.
// Only two methods: read current state and atomically transition to a new one.
// All per-request counting is done locally via atomics; Redis is never on the hot path.
type Driver interface {
	// GetStateSnapshot reads state, expiry, and generation in one round-trip.
	GetStateSnapshot(ctx context.Context) (state int64, expiry time.Time, generation uint64, err error)

	// NewGeneration atomically transitions state via a Lua CAS script,
	// clears counts, sets expiry, and increments the generation.
	// Pass fromState=-1 to initialize a brand-new key without overwriting an existing one.
	// Returns (newGeneration, ok, error); ok=false means CAS check failed.
	NewGeneration(ctx context.Context, fromState, toState int64, expiry time.Time) (uint64, bool, error)
}

// stateSnapshot is the locally cached view of Redis state.
type stateSnapshot struct {
	state      State
	generation uint64
	expiry     time.Time // used by syncLoop to drive time-based transitions
}

// CircuitBreaker is a state machine to prevent sending requests that are likely to fail.
//
// # Performance model
//
// Hot path (beforeRequest + afterRequest on every request):
//   - One sync.RWMutex.RLock + struct copy   ≈  30 ns
//   - One or two sync/atomic operations       ≈  10 ns
//   - Zero Redis I/O
//
// Background sync (syncLoop, runs every SyncInterval, default 100ms):
//   - One GetStateSnapshot round-trip per interval
//   - At 100ms: ~10 Redis reads/sec per instance, regardless of RPS
//
// State transitions (rare — only on trip/recover):
//   - One NewGeneration Lua CAS round-trip
//   - Deduplicated within the process via transitioning CAS flag
//
// Redis unavailability (degraded mode):
//   - Transitions are applied locally; the circuit breaker keeps protecting.
//   - On Redis recovery the background goroutine reconciles: the more
//     protective state (Open > HalfOpen > Closed) is pushed to Redis so
//     all pods converge within one SyncInterval.
type CircuitBreaker struct {
	name          string
	maxRequests   uint32
	interval      time.Duration
	timeout       time.Duration
	syncInterval  time.Duration
	readyToTrip   func(counts LocalCounts) bool
	isSuccessful  func(err error) bool
	onStateChange func(name string, from State, to State)

	counts Driver

	// Local state cache — the entire hot path reads from here, never from Redis.
	cacheMu sync.RWMutex
	cache   stateSnapshot

	// Local atomic counters — zero Redis writes on the hot path.
	localConsecFail atomic.Int64
	localConsecSucc atomic.Int64

	// Deduplicates concurrent transition attempts within one process.
	// Cross-instance races are handled by the Lua CAS in NewGeneration.
	transitioning atomic.Bool

	// degraded is true while Redis is unreachable.
	// Transitions during this window are applied locally and reconciled on recovery.
	// Logged once when entering and once when leaving — no per-tick spam.
	degraded atomic.Bool

	stopSync chan struct{}
}

// NewCircuitBreaker returns a new CircuitBreaker configured with the given Settings.
// It starts a background goroutine that syncs state from Redis every SyncInterval.
// Call Stop when the owning service shuts down.
//
// If Redis is unavailable at startup the breaker starts in local-only (degraded) mode
// and automatically joins the distributed cluster when Redis becomes reachable.
func NewCircuitBreaker(st Settings) *CircuitBreaker {
	cb := new(CircuitBreaker)
	cb.name = st.Name
	cb.onStateChange = st.OnStateChange

	cb.maxRequests = 1
	if st.MaxRequests != 0 {
		cb.maxRequests = st.MaxRequests
	}
	cb.interval = defaultInterval
	if st.Interval > 0 {
		cb.interval = st.Interval
	}
	cb.timeout = defaultTimeout
	if st.Timeout > 0 {
		cb.timeout = st.Timeout
	}
	cb.syncInterval = defaultSyncInterval
	if st.SyncInterval > 0 {
		cb.syncInterval = st.SyncInterval
	}
	cb.readyToTrip = defaultReadyToTrip
	if st.ReadyToTrip != nil {
		cb.readyToTrip = st.ReadyToTrip
	}
	cb.isSuccessful = defaultIsSuccessful
	if st.IsSuccessful != nil {
		cb.isSuccessful = st.IsSuccessful
	}

	cb.counts = st.Redis
	cb.stopSync = make(chan struct{})

	ctx := context.Background()
	expiry := cb.expiryForState(StateClosed, time.Now())

	if _, _, err := cb.counts.NewGeneration(ctx, -1, int64(StateClosed), expiry); err != nil {
		// Redis unavailable at startup — safe default is StateClosed local-only mode.
		cb.degraded.Store(true)
		slog.Warn("circuit_breaker: Redis unavailable at startup, running in local mode",
			"name", cb.name, "error", err)
	} else if err := cb.refreshCache(ctx); err != nil {
		// Key written but read failed — still start degraded.
		cb.degraded.Store(true)
		slog.Warn("circuit_breaker: initial state load failed, running in local mode",
			"name", cb.name, "error", err)
	}

	go cb.syncLoop()

	slog.Info("circuit_breaker: initialized",
		"name", cb.name, "sync_interval", cb.syncInterval, "degraded", cb.degraded.Load())
	return cb
}

// Stop shuts down the background sync goroutine. Call when the service shuts down.
func (cb *CircuitBreaker) Stop() {
	close(cb.stopSync)
	slog.Info("circuit_breaker: stopped", "name", cb.name)
}

func defaultReadyToTrip(counts LocalCounts) bool {
	return counts.ConsecutiveFailures > defaultMaxConsecutiveFailures
}

func defaultIsSuccessful(err error) bool { return err == nil }

// Name returns the name of the CircuitBreaker.
func (cb *CircuitBreaker) Name() string { return cb.name }

// State returns the current state from the local cache — no Redis I/O.
func (cb *CircuitBreaker) State() State {
	cb.cacheMu.RLock()
	s := cb.cache.state
	cb.cacheMu.RUnlock()
	return s
}

// IsDegraded reports whether Redis is currently unreachable.
// In degraded mode the breaker still protects the downstream service;
// state is local-only and not shared with other pods until Redis recovers.
func (cb *CircuitBreaker) IsDegraded() bool { return cb.degraded.Load() }

// Execute runs the given request if the CircuitBreaker accepts it.
// Execute returns an error instantly if the CircuitBreaker rejects the request.
// Otherwise, Execute returns the result of the request.
// If a panic occurs in the request, the CircuitBreaker handles it as an error
// and causes the same panic again.
func (cb *CircuitBreaker) Execute(
	ctx context.Context, req func() (any, error),
) (any, error) {
	generation, err := cb.beforeRequest()
	if err != nil {
		return nil, err
	}

	defer func() {
		e := recover()
		if e != nil {
			cb.afterRequest(ctx, generation, false)
			panic(e)
		}
	}()

	result, err := req()
	cb.afterRequest(ctx, generation, cb.isSuccessful(err))
	return result, err
}

// beforeRequest is the allow/reject decision — the innermost hot path.
// Cost: one RLock + one struct copy. Zero Redis I/O, even in degraded mode.
func (cb *CircuitBreaker) beforeRequest() (uint64, error) {
	cb.cacheMu.RLock()
	snap := cb.cache
	cb.cacheMu.RUnlock()

	if snap.state == StateOpen {
		return snap.generation, ErrOpenState
	}
	return snap.generation, nil
}

// afterRequest records the result — also hot path.
// Cost: one RLock + one or two atomic ops. Zero Redis I/O in steady state.
// Redis is only touched when a state transition is triggered (rare).
func (cb *CircuitBreaker) afterRequest(ctx context.Context, before uint64, success bool) {
	cb.cacheMu.RLock()
	snap := cb.cache
	cb.cacheMu.RUnlock()

	// Discard results from a previous generation.
	// The generation is Redis-authoritative in normal mode, so this guard works
	// across all pods. In degraded mode it is locally incremented, so it still
	// prevents stale results within the pod.
	if snap.generation != before {
		slog.Debug("circuit_breaker: stale generation, discarding result",
			"name", cb.name, "before", before, "current", snap.generation)
		return
	}

	now := time.Now()

	if success {
		cb.localConsecFail.Store(0)
		newSucc := cb.localConsecSucc.Add(1)

		if snap.state == StateHalfOpen && uint32(newSucc) >= cb.maxRequests {
			slog.Debug("circuit_breaker: HalfOpen threshold reached, closing",
				"name", cb.name, "consecutive_successes", newSucc)
			cb.tryTransition(ctx, StateHalfOpen, StateClosed, now)
		}
		return
	}

	cb.localConsecSucc.Store(0)
	newFail := cb.localConsecFail.Add(1)

	switch snap.state {
	case StateClosed:
		if cb.readyToTrip(LocalCounts{ConsecutiveFailures: uint32(newFail)}) {
			slog.Debug("circuit_breaker: failure threshold reached, opening",
				"name", cb.name, "consecutive_failures", newFail)
			cb.tryTransition(ctx, StateClosed, StateOpen, now)
		}
	case StateHalfOpen:
		slog.Debug("circuit_breaker: HalfOpen probe failed, re-opening", "name", cb.name)
		cb.tryTransition(ctx, StateHalfOpen, StateOpen, now)
	}
}

// tryTransition deduplicates concurrent transition attempts within this process.
// Cross-instance races are handled by the Lua CAS inside setState.
func (cb *CircuitBreaker) tryTransition(ctx context.Context, from, to State, now time.Time) {
	if !cb.transitioning.CompareAndSwap(false, true) {
		return
	}
	defer cb.transitioning.Store(false)

	if err := cb.setState(ctx, from, to, now); err != nil {
		slog.Error("circuit_breaker: state transition failed",
			"name", cb.name, "from", from, "to", to, "error", err)
	}
}

// setState writes the transition to Redis via Lua CAS, then updates the local cache.
// If Redis is unreachable the transition is applied locally so the circuit breaker
// keeps protecting the downstream service. The pod is marked degraded and the
// background sync will reconcile state once Redis recovers.
func (cb *CircuitBreaker) setState(ctx context.Context, from, to State, now time.Time) error {
	stateVal, _, _, err := cb.counts.GetStateSnapshot(ctx)
	if err != nil {
		// Redis unreachable — apply locally so protection is not lost.
		cb.enterDegraded(err)
		cb.applyStateLocally(from, to, now)
		return nil
	}

	if State(stateVal) != from {
		// A peer already transitioned. Sync the cache and continue.
		slog.Debug("circuit_breaker: setState: peer already transitioned",
			"name", cb.name, "expected_from", from, "actual", State(stateVal))
		return cb.refreshCache(ctx)
	}

	newExpiry := cb.expiryForState(to, now)
	gen, ok, err := cb.counts.NewGeneration(ctx, int64(from), int64(to), newExpiry)
	if err != nil {
		// Redis went away between GetStateSnapshot and NewGeneration — apply locally.
		cb.enterDegraded(err)
		cb.applyStateLocally(from, to, now)
		return nil
	}
	if !ok {
		// CAS race with a peer — sync and move on.
		slog.Debug("circuit_breaker: setState CAS missed, syncing",
			"name", cb.name, "from", from, "to", to)
		return cb.refreshCache(ctx)
	}

	cb.cacheMu.Lock()
	cb.cache = stateSnapshot{state: to, generation: gen, expiry: newExpiry}
	cb.cacheMu.Unlock()
	cb.localConsecFail.Store(0)
	cb.localConsecSucc.Store(0)

	slog.Info("circuit_breaker: state changed",
		"name", cb.name, "from", from, "to", to, "generation", gen)
	if cb.onStateChange != nil {
		cb.onStateChange(cb.name, from, to)
	}
	return nil
}

// applyStateLocally updates the local cache directly without touching Redis.
// Used when Redis is unavailable. The generation is incremented locally so
// in-flight requests with the old generation are correctly discarded.
func (cb *CircuitBreaker) applyStateLocally(from, to State, now time.Time) {
	newExpiry := cb.expiryForState(to, now)
	cb.cacheMu.Lock()
	newGen := cb.cache.generation + 1
	cb.cache = stateSnapshot{state: to, generation: newGen, expiry: newExpiry}
	cb.cacheMu.Unlock()
	cb.localConsecFail.Store(0)
	cb.localConsecSucc.Store(0)

	slog.Warn("circuit_breaker: applied transition locally (degraded)",
		"name", cb.name, "from", from, "to", to, "local_generation", newGen)
	if cb.onStateChange != nil {
		cb.onStateChange(cb.name, from, to)
	}
}

// refreshCache reads the authoritative state from Redis and updates the local snapshot.
// If the generation changed (a peer transitioned), local counters are reset.
// On the first successful read after a degraded period, reconcileWithRedis is called.
func (cb *CircuitBreaker) refreshCache(ctx context.Context) error {
	stateVal, expiry, gen, err := cb.counts.GetStateSnapshot(ctx)
	if err != nil {
		cb.enterDegraded(err)
		return err
	}

	// Redis is reachable. If we were degraded, reconcile before adopting Redis state.
	if cb.degraded.CompareAndSwap(true, false) {
		slog.Info("circuit_breaker: Redis reconnected, reconciling state", "name", cb.name)
		return cb.reconcileWithRedis(ctx, State(stateVal), expiry, gen)
	}

	cb.applyRedisSnapshot(stateVal, expiry, gen)
	return nil
}

// applyRedisSnapshot updates the local cache from already-read Redis values.
// No Redis I/O — callers are responsible for reading first.
func (cb *CircuitBreaker) applyRedisSnapshot(stateVal int64, expiry time.Time, gen uint64) {
	cb.cacheMu.Lock()
	prev := cb.cache
	cb.cache = stateSnapshot{state: State(stateVal), generation: gen, expiry: expiry}
	cb.cacheMu.Unlock()

	if prev.generation != gen {
		cb.localConsecFail.Store(0)
		cb.localConsecSucc.Store(0)
		slog.Info("circuit_breaker: state synced from Redis",
			"name", cb.name, "state", State(stateVal), "generation", gen,
			"prev_generation", prev.generation)
	}
}

// reconcileWithRedis is called once when Redis comes back after a degraded period.
//
// Reconciliation rule — the more protective state wins:
//
//	Open (2) > HalfOpen (1) > Closed (0)
//
// If the local state is more open than Redis (e.g. this pod tripped while Redis
// was down), we push our state to Redis so all other pods pick it up within their
// next SyncInterval. If Redis is same or more open, we adopt Redis.
//
// This guarantees that a pod which correctly detected failures during an outage
// does not silently discard that signal when the cluster reconnects.
func (cb *CircuitBreaker) reconcileWithRedis(ctx context.Context, redisState State, redisExpiry time.Time, redisGen uint64) error {
	cb.cacheMu.RLock()
	localSnap := cb.cache
	cb.cacheMu.RUnlock()

	slog.Info("circuit_breaker: reconcile",
		"name", cb.name, "local_state", localSnap.state, "redis_state", redisState)

	if localSnap.state > redisState {
		// Local is more protective — push it to Redis so peers converge.
		slog.Info("circuit_breaker: reconcile: pushing local state to Redis",
			"name", cb.name, "local", localSnap.state, "redis", redisState)

		newExpiry := cb.expiryForState(localSnap.state, time.Now())
		gen, ok, err := cb.counts.NewGeneration(ctx, int64(redisState), int64(localSnap.state), newExpiry)
		if err != nil {
			// Redis went away again immediately after recovery — stay local.
			cb.enterDegraded(err)
			return err
		}
		if ok {
			cb.cacheMu.Lock()
			cb.cache = stateSnapshot{state: localSnap.state, generation: gen, expiry: newExpiry}
			cb.cacheMu.Unlock()
			slog.Info("circuit_breaker: reconcile: local state pushed to Redis",
				"name", cb.name, "state", localSnap.state, "generation", gen)
			return nil
		}
		// CAS lost — another pod raced us. Fall through to adopt Redis state.
		slog.Debug("circuit_breaker: reconcile: push CAS missed, adopting Redis state",
			"name", cb.name)
	}

	// Redis is same or more open — adopt it.
	cb.cacheMu.Lock()
	cb.cache = stateSnapshot{state: redisState, generation: redisGen, expiry: redisExpiry}
	cb.cacheMu.Unlock()
	cb.localConsecFail.Store(0)
	cb.localConsecSucc.Store(0)

	if localSnap.state != redisState {
		slog.Info("circuit_breaker: reconcile: adopted Redis state",
			"name", cb.name, "from", localSnap.state, "to", redisState)
		if cb.onStateChange != nil {
			cb.onStateChange(cb.name, localSnap.state, redisState)
		}
	}
	return nil
}

// enterDegraded marks the breaker as degraded and logs once per outage window.
func (cb *CircuitBreaker) enterDegraded(err error) {
	if cb.degraded.CompareAndSwap(false, true) {
		slog.Warn("circuit_breaker: entering degraded mode (Redis unavailable), running locally",
			"name", cb.name, "error", err)
	}
}

// syncLoop is the background goroutine.
// It runs every SyncInterval to:
//  1. Drive time-based transitions (Open timeout → HalfOpen, Closed interval reset).
//  2. Pick up transitions made by peer pods via refreshCache.
//
// This is the only Redis I/O in steady state:
// one GetStateSnapshot every SyncInterval per pod, regardless of RPS.
func (cb *CircuitBreaker) syncLoop() {
	ticker := time.NewTicker(cb.syncInterval)
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

func (cb *CircuitBreaker) tick(now time.Time) {
	ctx := context.Background()

	// Check time-based transitions using the locally cached expiry — no Redis read.
	cb.cacheMu.RLock()
	snap := cb.cache
	cb.cacheMu.RUnlock()

	slog.Info("ticker is running")

	switch snap.state {
	case StateClosed:
		if !snap.expiry.IsZero() && snap.expiry.Before(now) {
			slog.Debug("circuit_breaker: closed interval expired, resetting counts", "name", cb.name)
			cb.tryTransition(ctx, StateClosed, StateClosed, now)
		}
	case StateOpen:
		if !snap.expiry.IsZero() && snap.expiry.Before(now) {
			slog.Info("circuit_breaker: open timeout elapsed, transitioning to HalfOpen",
				"name", cb.name)
			cb.tryTransition(ctx, StateOpen, StateHalfOpen, now)
		}
	}

	// Sync with Redis to pick up transitions made by other pods.
	// refreshCache handles all logging (once on degraded entry, once on recovery).
	// No additional log here to avoid duplicating messages.
	_ = cb.refreshCache(ctx)
}

func (cb *CircuitBreaker) expiryForState(state State, now time.Time) time.Time {
	switch state {
	case StateClosed:
		if cb.interval > 0 {
			return now.Add(cb.interval)
		}
		return time.Time{}
	case StateOpen:
		return now.Add(cb.timeout)
	default: // StateHalfOpen
		return time.Time{}
	}
}
