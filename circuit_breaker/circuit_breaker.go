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

// statePriority maps each State to its protection level.
// Fix #9: explicit map — reconciliation correctness does not depend on iota ordering.
// If states are ever reordered or a new state is added, this map must be updated,
// and the compiler will not silently produce wrong comparisons.
var statePriority = map[State]int{
	StateClosed:   0,
	StateHalfOpen: 1,
	StateOpen:     2,
}

const (
	defaultInterval               = time.Duration(0)
	defaultTimeout                = 60 * time.Second
	defaultSyncInterval           = 100 * time.Millisecond
	defaultMaxConsecutiveFailures = 10
	defaultBootstrapTimeout       = 3 * time.Second
)

var (
	// ErrOpenState is returned when the circuit breaker is open.
	ErrOpenState = errors.New("circuit breaker is open")
	// ErrProbesFull is returned in HalfOpen when all concurrent probe slots are taken.
	// Unlike ErrOpenState this is transient — slots free up as in-flight probes complete.
	ErrProbesFull = errors.New("circuit breaker half-open: probe slots full")
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
	Driver Driver

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
	expiry     time.Time
}

// CircuitBreaker is a state machine to prevent sending requests that are likely to fail.
//
// # Performance model
//
// Hot path (beforeRequest + afterRequest on every request):
//   - One atomic.Pointer.Load (cache read)     ≈   1 ns   fix #3
//   - One or two sync/atomic operations        ≈  10 ns
//   - Zero Redis I/O
//
// Background sync (syncLoop, runs every SyncInterval, default 100ms):
//   - One GetStateSnapshot round-trip per interval
//   - At 100ms: ~10 Redis reads/sec per instance, regardless of RPS
//
// State transitions (rare — only on trip/recover):
//   - One NewGeneration Lua CAS round-trip     fix #2: was two round-trips
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

	// Fix #3: atomic.Pointer replaces sync.RWMutex + struct field.
	// Reads are a single atomic load (~1 ns); writes (rare, only on transitions) use Store.
	cache atomic.Pointer[stateSnapshot]

	// Local atomic counters — zero Redis writes on the hot path.
	localConsecFail atomic.Int64
	localConsecSucc atomic.Int64

	// Fix #1: halfOpenInFlight enforces MaxRequests as a hard concurrency limit on
	// in-flight probe requests, not just a completion count.
	// Incremented in beforeRequest when state==HalfOpen, decremented via defer in Execute.
	// Reset to 0 on every HalfOpen entry (fix #8).
	halfOpenInFlight atomic.Int32

	// transitioning deduplicates concurrent transition attempts within one process.
	// Cross-instance races are handled by the Lua CAS in NewGeneration.
	transitioning atomic.Bool

	// degraded is true while Redis is unreachable.
	// Logged once when entering, once when leaving — no per-tick spam.
	degraded atomic.Bool

	// Fix #5: stopCancel propagates cancellation into the background goroutine
	// so in-flight Redis calls are cancelled when Stop() is called.
	stopCancel context.CancelFunc

	// Fix #6: wg ensures Stop() blocks until the goroutine has fully exited,
	// so callers can safely tear down the Redis connection after Stop() returns.
	wg sync.WaitGroup
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

	cb.counts = st.Driver
	cb.cache.Store(&stateSnapshot{})

	// Fix #10: 3-second timeout for bootstrap — fail fast into degraded mode
	// rather than blocking service startup for the full Redis dial timeout.
	bootstrapCtx, bootstrapCancel := context.WithTimeout(context.Background(), defaultBootstrapTimeout)
	defer bootstrapCancel()

	expiry := cb.expiryForState(StateClosed, time.Now())
	if _, _, err := cb.counts.NewGeneration(bootstrapCtx, -1, int64(StateClosed), expiry); err != nil {
		cb.degraded.Store(true)
		slog.Warn("circuit_breaker: Redis unavailable at startup, running in local mode",
			"name", cb.name, "error", err)
	} else if err := cb.refreshCache(bootstrapCtx); err != nil {
		cb.degraded.Store(true)
		slog.Warn("circuit_breaker: initial state load failed, running in local mode",
			"name", cb.name, "error", err)
	}

	// Fix #5: cancellable context flows into syncLoop → tick → refreshCache → Redis calls.
	var stopCtx context.Context
	stopCtx, cb.stopCancel = context.WithCancel(context.Background())
	cb.wg.Add(1)
	go cb.syncLoop(stopCtx)

	slog.Info("circuit_breaker: initialized",
		"name", cb.name, "sync_interval", cb.syncInterval, "degraded", cb.degraded.Load())
	return cb
}

// Stop cancels the background goroutine's context and waits for it to exit.
// Fix #6: returns only after the goroutine has fully stopped; callers can safely
// close the Redis connection immediately after Stop() returns.
func (cb *CircuitBreaker) Stop() {
	cb.stopCancel()
	cb.wg.Wait()
	slog.Info("circuit_breaker: stopped", "name", cb.name)
}

func defaultReadyToTrip(counts LocalCounts) bool {
	return counts.ConsecutiveFailures >= defaultMaxConsecutiveFailures
}

func defaultIsSuccessful(err error) bool { return err == nil }

// Name returns the name of the CircuitBreaker.
func (cb *CircuitBreaker) Name() string { return cb.name }

// State returns the current state from the local cache — no Redis I/O.
func (cb *CircuitBreaker) State() State {
	return cb.cache.Load().state
}

// IsDegraded reports whether Redis is currently unreachable.
// In degraded mode the breaker still protects the downstream service;
// state is local-only and not shared with other pods until Redis recovers.
func (cb *CircuitBreaker) IsDegraded() bool { return cb.degraded.Load() }

// Execute runs the given request if the CircuitBreaker accepts it.
// Fix #7: req is func() error — no any boxing, zero allocations on the hot path.
// Panics inside req are caught, counted as failures, and re-panicked.
func (cb *CircuitBreaker) Execute(ctx context.Context, req func() error) error {
	generation, wasHalfOpen, err := cb.beforeRequest()
	if err != nil {
		return err
	}
	// Fix #1: decrement the in-flight probe counter when the probe completes,
	// regardless of success, failure, or panic.
	if wasHalfOpen {
		defer cb.halfOpenInFlight.Add(-1)
	}

	defer func() {
		e := recover()
		if e != nil {
			cb.afterRequest(ctx, generation, false)
			panic(e)
		}
	}()

	err = req()
	cb.afterRequest(ctx, generation, cb.isSuccessful(err))
	return err
}

// beforeRequest is the allow/reject decision — the innermost hot path.
// Fix #3: single atomic.Pointer.Load, no mutex, ~1 ns.
// Fix #1: enforces MaxRequests as a hard in-flight concurrency limit in HalfOpen.
func (cb *CircuitBreaker) beforeRequest() (generation uint64, wasHalfOpen bool, err error) {
	snap := cb.cache.Load()

	switch snap.state {
	case StateOpen:
		return snap.generation, false, ErrOpenState
	case StateHalfOpen:
		// Fix #1: reject if already at the in-flight limit.
		if cb.halfOpenInFlight.Add(1) > int32(cb.maxRequests) {
			cb.halfOpenInFlight.Add(-1)
			return snap.generation, false, ErrProbesFull
		}
		return snap.generation, true, nil
	default:
		return snap.generation, false, nil
	}
}

// afterRequest records the result — also hot path.
// Cost: one atomic.Pointer.Load + one or two atomic ops. Zero Redis I/O in steady state.
func (cb *CircuitBreaker) afterRequest(ctx context.Context, before uint64, success bool) {
	snap := cb.cache.Load()

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

// setState commits a transition to Redis via Lua CAS, then updates the local cache.
// Fix #2: goes directly to NewGeneration — eliminates the redundant GetStateSnapshot
// round-trip. The Lua CAS already reads state_val and returns -1 on mismatch.
// If Redis is unreachable the transition is applied locally.
func (cb *CircuitBreaker) setState(ctx context.Context, from, to State, now time.Time) error {
	newExpiry := cb.expiryForState(to, now)
	gen, ok, err := cb.counts.NewGeneration(ctx, int64(from), int64(to), newExpiry)
	if err != nil {
		cb.enterDegraded(err)
		cb.applyStateLocally(from, to, now)
		return nil
	}
	if !ok {
		slog.Debug("circuit_breaker: setState CAS missed, syncing",
			"name", cb.name, "from", from, "to", to)
		return cb.refreshCache(ctx)
	}

	// Fix #8: reset in-flight probe counter on every HalfOpen entry so a leaked
	// count from the previous cycle cannot starve the new one of probes.
	if to == StateHalfOpen {
		cb.halfOpenInFlight.Store(0)
	}
	cb.cache.Store(&stateSnapshot{state: to, generation: gen, expiry: newExpiry})
	cb.localConsecFail.Store(0)
	cb.localConsecSucc.Store(0)

	slog.Info("circuit_breaker: state changed",
		"name", cb.name, "from", from, "to", to, "generation", gen)
	if cb.onStateChange != nil {
		cb.onStateChange(cb.name, from, to)
	}
	return nil
}

// applyStateLocally updates the local cache without touching Redis.
// Used when Redis is unavailable. Locally increments generation so in-flight
// requests from the previous generation are correctly discarded.
func (cb *CircuitBreaker) applyStateLocally(from, to State, now time.Time) {
	newExpiry := cb.expiryForState(to, now)
	prev := cb.cache.Load()
	newGen := prev.generation + 1
	// Fix #8: same reset on local HalfOpen entry as on Redis-backed entry.
	if to == StateHalfOpen {
		cb.halfOpenInFlight.Store(0)
	}
	cb.cache.Store(&stateSnapshot{state: to, generation: newGen, expiry: newExpiry})
	cb.localConsecFail.Store(0)
	cb.localConsecSucc.Store(0)

	slog.Warn("circuit_breaker: applied transition locally (degraded)",
		"name", cb.name, "from", from, "to", to, "local_generation", newGen)
	if cb.onStateChange != nil {
		cb.onStateChange(cb.name, from, to)
	}
}

// refreshCache reads the authoritative state from Redis and updates the local snapshot.
// On the first successful read after a degraded period, reconcileWithRedis is called.
func (cb *CircuitBreaker) refreshCache(ctx context.Context) error {
	stateVal, expiry, gen, err := cb.counts.GetStateSnapshot(ctx)
	if err != nil {
		cb.enterDegraded(err)
		return err
	}

	if cb.degraded.CompareAndSwap(true, false) {
		slog.Info("circuit_breaker: Redis reconnected, reconciling state", "name", cb.name)
		return cb.reconcileWithRedis(ctx, State(stateVal), expiry, gen)
	}

	cb.applyRedisSnapshot(stateVal, expiry, gen)
	return nil
}

// applyRedisSnapshot updates the local cache from already-read Redis values. No Redis I/O.
func (cb *CircuitBreaker) applyRedisSnapshot(stateVal int64, expiry time.Time, gen uint64) {
	prev := cb.cache.Load()
	newState := State(stateVal)
	// Fix #8: reset probe counter when Redis tells us we entered HalfOpen.
	if newState == StateHalfOpen && prev.state != StateHalfOpen {
		cb.halfOpenInFlight.Store(0)
	}
	cb.cache.Store(&stateSnapshot{state: newState, generation: gen, expiry: expiry})

	if prev.generation != gen {
		cb.localConsecFail.Store(0)
		cb.localConsecSucc.Store(0)
		slog.Info("circuit_breaker: state synced from Redis",
			"name", cb.name, "state", newState, "generation", gen,
			"prev_generation", prev.generation)
	}
}

// reconcileWithRedis resolves state divergence after a degraded period.
// Fix #9: uses statePriority map — correctness does not depend on iota ordering.
//
// Rule: the more protective state wins (Open > HalfOpen > Closed).
func (cb *CircuitBreaker) reconcileWithRedis(ctx context.Context, redisState State, redisExpiry time.Time, redisGen uint64) error {
	localSnap := cb.cache.Load()

	slog.Info("circuit_breaker: reconcile",
		"name", cb.name, "local_state", localSnap.state, "redis_state", redisState)

	if statePriority[localSnap.state] > statePriority[redisState] {
		slog.Info("circuit_breaker: reconcile: pushing local state to Redis",
			"name", cb.name, "local", localSnap.state, "redis", redisState)

		newExpiry := cb.expiryForState(localSnap.state, time.Now())
		gen, ok, err := cb.counts.NewGeneration(ctx, int64(redisState), int64(localSnap.state), newExpiry)
		if err != nil {
			cb.enterDegraded(err)
			return err
		}
		if ok {
			if localSnap.state == StateHalfOpen {
				cb.halfOpenInFlight.Store(0)
			}
			cb.cache.Store(&stateSnapshot{state: localSnap.state, generation: gen, expiry: newExpiry})
			slog.Info("circuit_breaker: reconcile: local state pushed to Redis",
				"name", cb.name, "state", localSnap.state, "generation", gen)
			return nil
		}
		slog.Debug("circuit_breaker: reconcile: push CAS missed, adopting Redis state",
			"name", cb.name)
	}

	// Redis is same or more protective — adopt it.
	if redisState == StateHalfOpen && localSnap.state != StateHalfOpen {
		cb.halfOpenInFlight.Store(0)
	}
	cb.cache.Store(&stateSnapshot{state: redisState, generation: redisGen, expiry: redisExpiry})
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
// Fix #5: takes ctx derived from stopCancel so Redis calls inside tick are cancellable.
// Fix #6: defers wg.Done so Stop() blocks until this goroutine exits.
func (cb *CircuitBreaker) syncLoop(ctx context.Context) {
	defer cb.wg.Done()
	ticker := time.NewTicker(cb.syncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			cb.tick(ctx, now)
		}
	}
}

// Fix #4: removed slog.Info("ticker is running") — was firing 10×/sec in production.
// Fix #5: takes ctx so Redis calls inside refreshCache can be cancelled on Stop().
func (cb *CircuitBreaker) tick(ctx context.Context, now time.Time) {
	snap := cb.cache.Load()

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
