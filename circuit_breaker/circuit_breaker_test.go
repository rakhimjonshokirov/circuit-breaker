package circuitbreaker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	redisdriver "github.com/rakhimjonshokirov/circuit-breaker/circuit_breaker/redis_driver"
	"github.com/redis/go-redis/v9"
)

// ─── fake driver ────────────────────────────────────────────────────────────
// Mirrors the Lua CAS semantics of the real driver without a network hop.

type fakeDriver struct {
	mu          sync.Mutex
	state       int64
	expiry      time.Time
	generation  uint64
	initialized bool
	failing     bool
}

func (f *fakeDriver) setFailing(v bool) {
	f.mu.Lock()
	f.failing = v
	f.mu.Unlock()
}

func (f *fakeDriver) forceState(state int64, gen uint64) {
	f.mu.Lock()
	f.state = state
	f.generation = gen
	f.initialized = true
	f.mu.Unlock()
}

func (f *fakeDriver) GetStateSnapshot(_ context.Context) (int64, time.Time, uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failing {
		return 0, time.Time{}, 0, errors.New("connection refused")
	}
	return f.state, f.expiry, f.generation, nil
}

func (f *fakeDriver) NewGeneration(_ context.Context, fromState, toState int64, expiry time.Time) (uint64, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failing {
		return 0, false, errors.New("connection refused")
	}
	if fromState == -1 {
		if f.initialized {
			return f.generation, true, nil // already seeded; no reset
		}
		f.state, f.expiry, f.generation, f.initialized = toState, expiry, 1, true
		return 1, true, nil
	}
	if f.state != fromState {
		return f.generation, false, nil // CAS miss
	}
	f.generation++
	f.state, f.expiry = toState, expiry
	return f.generation, true, nil
}

// ─── helpers ────────────────────────────────────────────────────────────────

// noSyncCB creates a circuit breaker whose background goroutine never fires
// during a test; tests drive ticks manually via cb.tick().
func noSyncCB(fake *fakeDriver, patch ...func(*Settings)) *CircuitBreaker {
	st := Settings{
		Redis:        fake,
		Name:         "test",
		SyncInterval: time.Hour, // never fires automatically in tests
		ReadyToTrip:  func(c LocalCounts) bool { return c.ConsecutiveFailures >= 3 },
	}
	for _, p := range patch {
		p(&st)
	}
	return NewCircuitBreaker(st)
}

var errDown = errors.New("downstream unavailable")

func executeN(cb *CircuitBreaker, n int, reqErr error) {
	for i := 0; i < n; i++ {
		cb.Execute(context.Background(), func() (any, error) { return nil, reqErr })
	}
}

func assertState(t *testing.T, cb *CircuitBreaker, want State) {
	t.Helper()
	if got := cb.State(); got != want {
		t.Fatalf("state: got %s, want %s", got, want)
	}
}

func waitState(t *testing.T, cb *CircuitBreaker, want State, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cb.State() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for state %s; got %s", want, cb.State())
}

// ─── unit tests ─────────────────────────────────────────────────────────────

func TestInitialState_IsClosed(t *testing.T) {
	cb := noSyncCB(&fakeDriver{})
	defer cb.Stop()
	assertState(t, cb, StateClosed)
}

func TestTrip_ClosedToOpen(t *testing.T) {
	cb := noSyncCB(&fakeDriver{})
	defer cb.Stop()

	executeN(cb, 2, errDown)
	assertState(t, cb, StateClosed) // 2 < threshold(3)

	executeN(cb, 1, errDown)
	assertState(t, cb, StateOpen) // 3 == threshold
}

func TestOpen_RejectsRequests(t *testing.T) {
	cb := noSyncCB(&fakeDriver{})
	defer cb.Stop()

	executeN(cb, 3, errDown)
	assertState(t, cb, StateOpen)

	_, err := cb.Execute(context.Background(), func() (any, error) { return nil, nil })
	if !errors.Is(err, ErrOpenState) {
		t.Fatalf("expected ErrOpenState, got %v", err)
	}
}

func TestOpen_ToHalfOpen_AfterTimeout(t *testing.T) {
	fake := &fakeDriver{}
	cb := noSyncCB(fake, func(s *Settings) { s.Timeout = 50 * time.Millisecond })
	defer cb.Stop()

	executeN(cb, 3, errDown)
	assertState(t, cb, StateOpen)

	// Advance synthetic time past the timeout.
	cb.tick(time.Now().Add(200 * time.Millisecond))
	assertState(t, cb, StateHalfOpen)
}

func TestHalfOpen_ClosesAfterEnoughSuccesses(t *testing.T) {
	fake := &fakeDriver{}
	cb := noSyncCB(fake, func(s *Settings) {
		s.Timeout = 50 * time.Millisecond
		s.MaxRequests = 2
	})
	defer cb.Stop()

	executeN(cb, 3, errDown)
	cb.tick(time.Now().Add(200 * time.Millisecond)) // → HalfOpen
	assertState(t, cb, StateHalfOpen)

	executeN(cb, 1, nil)
	assertState(t, cb, StateHalfOpen) // 1 < maxRequests(2)

	executeN(cb, 1, nil)
	assertState(t, cb, StateClosed) // 2 == maxRequests
}

func TestHalfOpen_ReOpensOnFailure(t *testing.T) {
	fake := &fakeDriver{}
	cb := noSyncCB(fake, func(s *Settings) { s.Timeout = 50 * time.Millisecond })
	defer cb.Stop()

	executeN(cb, 3, errDown)
	cb.tick(time.Now().Add(200 * time.Millisecond)) // → HalfOpen
	assertState(t, cb, StateHalfOpen)

	executeN(cb, 1, errDown) // probe fails → back to Open
	assertState(t, cb, StateOpen)
}

func TestClosedInterval_ResetsGeneration(t *testing.T) {
	fake := &fakeDriver{}
	cb := noSyncCB(fake, func(s *Settings) { s.Interval = 50 * time.Millisecond })
	defer cb.Stop()

	cb.cacheMu.RLock()
	gen1 := cb.cache.generation
	cb.cacheMu.RUnlock()

	cb.tick(time.Now().Add(200 * time.Millisecond))

	cb.cacheMu.RLock()
	gen2 := cb.cache.generation
	cb.cacheMu.RUnlock()

	if gen2 <= gen1 {
		t.Fatalf("generation should advance after interval reset: %d → %d", gen1, gen2)
	}
}

func TestExecute_PanicCountsAsFailure(t *testing.T) {
	cb := noSyncCB(&fakeDriver{})
	defer cb.Stop()

	panicked := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				panicked = true
			}
		}()
		cb.Execute(context.Background(), func() (any, error) {
			panic("boom")
		})
	}()

	if !panicked {
		t.Fatal("panic should re-panic after being recorded")
	}
	if got := cb.localConsecFail.Load(); got != 1 {
		t.Fatalf("consecutive failures after panic: got %d, want 1", got)
	}
}

func TestIsSuccessful_Override(t *testing.T) {
	cb := noSyncCB(&fakeDriver{}, func(s *Settings) {
		s.IsSuccessful = func(error) bool { return true } // everything is a success
	})
	defer cb.Stop()

	executeN(cb, 20, errDown)
	assertState(t, cb, StateClosed) // should never trip
}

func TestReadyToTrip_CustomThreshold(t *testing.T) {
	cb := noSyncCB(&fakeDriver{}, func(s *Settings) {
		s.ReadyToTrip = func(c LocalCounts) bool { return c.ConsecutiveFailures >= 1 }
	})
	defer cb.Stop()

	executeN(cb, 1, errDown)
	assertState(t, cb, StateOpen)
}

func TestOnStateChange_Callback(t *testing.T) {
	fake := &fakeDriver{}
	var mu sync.Mutex
	var transitions []string

	cb := noSyncCB(fake, func(s *Settings) {
		s.Timeout = 50 * time.Millisecond
		s.OnStateChange = func(_ string, from, to State) {
			mu.Lock()
			transitions = append(transitions, fmt.Sprintf("%s→%s", from, to))
			mu.Unlock()
		}
	})
	defer cb.Stop()

	executeN(cb, 3, errDown)                        // Closed → Open
	cb.tick(time.Now().Add(200 * time.Millisecond)) // Open → HalfOpen
	executeN(cb, 1, nil)                            // HalfOpen → Closed

	mu.Lock()
	got := append([]string(nil), transitions...)
	mu.Unlock()

	want := []string{"closed→open", "open→half-open", "half-open→closed"}
	if len(got) != len(want) {
		t.Fatalf("transition count: got %d (%v), want %d (%v)", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("transition[%d]: got %q, want %q", i, got[i], want[i])
		}
	}
}

func TestGenerationGuard_StaleResultDiscarded(t *testing.T) {
	fake := &fakeDriver{}
	cb := noSyncCB(fake)
	defer cb.Stop()

	// Capture generation before tripping.
	cb.cacheMu.RLock()
	stalGen := cb.cache.generation
	cb.cacheMu.RUnlock()

	executeN(cb, 3, errDown) // trips → new generation
	assertState(t, cb, StateOpen)

	// Inject a "late" result from the pre-trip generation.
	cb.afterRequest(context.Background(), stalGen, true)

	// State must still be Open; the stale success should be ignored.
	assertState(t, cb, StateOpen)
}

func TestName(t *testing.T) {
	cb := noSyncCB(&fakeDriver{}, func(s *Settings) { s.Name = "my-svc" })
	defer cb.Stop()
	if got := cb.Name(); got != "my-svc" {
		t.Fatalf("Name: got %q, want %q", got, "my-svc")
	}
}

// ─── degraded-mode unit tests ────────────────────────────────────────────────

// TestDegraded_StartsLocalWhenRedisDown verifies the breaker starts in degraded mode
// and still trips locally when Redis is unreachable at startup.
func TestDegraded_StartsLocalWhenRedisDown(t *testing.T) {
	fake := &fakeDriver{}
	fake.setFailing(true)

	cb := noSyncCB(fake)
	defer cb.Stop()

	if !cb.IsDegraded() {
		t.Fatal("should be degraded when Redis is unreachable at startup")
	}
	assertState(t, cb, StateClosed) // default is Closed

	executeN(cb, 3, errDown)
	assertState(t, cb, StateOpen) // tripped locally despite Redis being down
}

// TestDegraded_ReconcileLocalOpenWins: this pod tripped locally while Redis was down.
// When Redis comes back (with Closed state), the local Open state is pushed to Redis.
func TestDegraded_ReconcileLocalOpenWins(t *testing.T) {
	fake := &fakeDriver{}
	cb := noSyncCB(fake) // Redis is up; initialises with Closed, gen=1
	defer cb.Stop()

	// Redis goes down; mark pod as degraded.
	fake.setFailing(true)
	cb.degraded.Store(true)

	// Trip locally.
	executeN(cb, 3, errDown)
	assertState(t, cb, StateOpen)

	// Redis comes back (still holds Closed, gen=1 from init).
	fake.setFailing(false)
	cb.tick(time.Now()) // triggers refreshCache → reconcileWithRedis

	// Local Open(2) > Redis Closed(0) → pushed Open to Redis.
	assertState(t, cb, StateOpen)
	if cb.IsDegraded() {
		t.Fatal("should not be degraded after Redis recovery")
	}

	stateVal, _, _, err := fake.GetStateSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if State(stateVal) != StateOpen {
		t.Fatalf("Redis state after reconcile: got %s, want open", State(stateVal))
	}
}

// TestDegraded_ReconcileRedisOpenWins: another pod tripped Redis while this pod was
// degraded (local state stayed Closed). On recovery, the pod adopts Redis Open.
func TestDegraded_ReconcileRedisOpenWins(t *testing.T) {
	fake := &fakeDriver{}
	cb := noSyncCB(fake) // initialises Closed, gen=1
	defer cb.Stop()

	// Simulate another pod tripping Redis to Open.
	fake.forceState(int64(StateOpen), 5)

	// This pod enters degraded mode with Closed local state.
	cb.degraded.Store(true)

	cb.tick(time.Now()) // refreshCache → reconcileWithRedis

	// Redis Open(2) > Local Closed(0) → adopt Redis.
	assertState(t, cb, StateOpen)
	if cb.IsDegraded() {
		t.Fatal("should not be degraded after Redis recovery")
	}
}

// TestDegraded_TransitionDuringOutage ensures state transitions work even when Redis
// is unavailable during the entire lifecycle (Open timeout → HalfOpen locally).
func TestDegraded_TransitionDuringOutage(t *testing.T) {
	fake := &fakeDriver{}
	fake.setFailing(true)

	cb := noSyncCB(fake, func(s *Settings) { s.Timeout = 50 * time.Millisecond })
	defer cb.Stop()

	executeN(cb, 3, errDown) // trip locally
	assertState(t, cb, StateOpen)

	// Advance past the Open timeout.
	cb.tick(time.Now().Add(200 * time.Millisecond))
	// Open→HalfOpen is applied locally despite Redis being unreachable.
	assertState(t, cb, StateHalfOpen)
}

// ─── multi-pod unit tests ────────────────────────────────────────────────────

// TestTwoPods_TripPropagates: pod1 trips the shared fake; pod2 learns on its next tick.
func TestTwoPods_TripPropagates(t *testing.T) {
	fake := &fakeDriver{}

	cb1 := noSyncCB(fake, func(s *Settings) { s.Name = "pod1" })
	cb2 := noSyncCB(fake, func(s *Settings) { s.Name = "pod2" })
	defer cb1.Stop()
	defer cb2.Stop()

	executeN(cb1, 3, errDown)
	assertState(t, cb1, StateOpen)
	assertState(t, cb2, StateClosed) // hasn't synced yet

	cb2.tick(time.Now()) // sync from shared fake
	assertState(t, cb2, StateOpen)
}

// TestTwoPods_CASPreventsDoubleFire: both pods try to trip simultaneously;
// only one Lua CAS succeeds, onStateChange fires exactly once across both pods.
func TestTwoPods_CASPreventsDoubleFire(t *testing.T) {
	fake := &fakeDriver{}
	var fires int
	var firesMu sync.Mutex

	onChange := func(_ string, from, to State) {
		if from == StateClosed && to == StateOpen {
			firesMu.Lock()
			fires++
			firesMu.Unlock()
		}
	}

	cb1 := noSyncCB(fake, func(s *Settings) { s.Name = "pod1"; s.OnStateChange = onChange })
	cb2 := noSyncCB(fake, func(s *Settings) { s.Name = "pod2"; s.OnStateChange = onChange })
	defer cb1.Stop()
	defer cb2.Stop()

	// Both pods independently build up 3 failures against the same fake.
	// Since the fake has a shared state and generation, only the first CAS wins.
	executeN(cb1, 3, errDown)
	executeN(cb2, 3, errDown) // cb2 will get CAS miss on setState; cb1 already transitioned

	firesMu.Lock()
	got := fires
	firesMu.Unlock()

	if got != 1 {
		t.Fatalf("onStateChange (Closed→Open) should fire exactly once across pods; got %d", got)
	}
}

// ─── concurrency ────────────────────────────────────────────────────────────

// TestConcurrent_NoDataRace runs 1 000 goroutines and verifies no data races.
// Run with: go test -race ./...
func TestConcurrent_NoDataRace(t *testing.T) {
	fake := &fakeDriver{}
	cb := noSyncCB(fake, func(s *Settings) {
		s.ReadyToTrip = func(c LocalCounts) bool { return c.ConsecutiveFailures >= 50 }
	})
	defer cb.Stop()

	const n = 1000
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			reqErr := error(nil)
			if i%7 == 0 {
				reqErr = errDown
			}
			cb.Execute(context.Background(), func() (any, error) { return nil, reqErr })
		}()
	}
	wg.Wait()
}

// ─── integration tests ──────────────────────────────────────────────────────
// Skipped automatically when Redis is not reachable.

const redisURL = "redis://:123@localhost:6379/0"

func newTestRedis(t testing.TB) *redis.Client {
	t.Helper()
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Skipf("invalid Redis URL: %v", err)
	}
	c := redis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Ping(ctx).Err(); err != nil {
		c.Close()
		t.Skipf("Redis unavailable: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func testKey(t testing.TB) string { return "test:cb:" + t.Name() }

func TestIntegration_FullLifecycle(t *testing.T) {
	rdb := newTestRedis(t)
	key := testKey(t)
	defer rdb.Del(context.Background(), key)

	cb := NewCircuitBreaker(Settings{
		Redis:        redisdriver.New(rdb, key, time.Hour),
		Name:         t.Name(),
		SyncInterval: 20 * time.Millisecond,
		Timeout:      80 * time.Millisecond,
		ReadyToTrip:  func(c LocalCounts) bool { return c.ConsecutiveFailures >= 3 },
	})
	defer cb.Stop()

	assertState(t, cb, StateClosed)

	// Trip.
	executeN(cb, 3, errDown)
	assertState(t, cb, StateOpen)

	// Verify rejection.
	_, err := cb.Execute(context.Background(), func() (any, error) { return nil, nil })
	if !errors.Is(err, ErrOpenState) {
		t.Fatalf("expected ErrOpenState while open, got %v", err)
	}

	// Wait for Open → HalfOpen (timeout 80ms + up to one SyncInterval 20ms).
	waitState(t, cb, StateHalfOpen, 200*time.Millisecond)

	// Probe success → Closed.
	executeN(cb, 1, nil)
	assertState(t, cb, StateClosed)
}

func TestIntegration_TwoPods_ShareState(t *testing.T) {
	rdb := newTestRedis(t)
	key := testKey(t)
	defer rdb.Del(context.Background(), key)

	newPod := func(name string) *CircuitBreaker {
		return NewCircuitBreaker(Settings{
			Redis:        redisdriver.New(rdb, key, time.Hour),
			Name:         name,
			SyncInterval: 20 * time.Millisecond,
			ReadyToTrip:  func(c LocalCounts) bool { return c.ConsecutiveFailures >= 3 },
		})
	}

	pod1 := newPod("pod1")
	pod2 := newPod("pod2")
	defer pod1.Stop()
	defer pod2.Stop()

	executeN(pod1, 3, errDown)
	assertState(t, pod1, StateOpen)

	// pod2 must pick up Open state within SyncInterval.
	waitState(t, pod2, StateOpen, 100*time.Millisecond)
}

func TestIntegration_NewPod_InheritsOpenState(t *testing.T) {
	rdb := newTestRedis(t)
	key := testKey(t)
	defer rdb.Del(context.Background(), key)

	pod1 := NewCircuitBreaker(Settings{
		Redis:        redisdriver.New(rdb, key, time.Hour),
		Name:         "pod1",
		SyncInterval: time.Hour,
		ReadyToTrip:  func(c LocalCounts) bool { return c.ConsecutiveFailures >= 3 },
	})
	defer pod1.Stop()

	executeN(pod1, 3, errDown)
	assertState(t, pod1, StateOpen)

	// A brand-new pod starts after pod1 has already tripped.
	pod2 := NewCircuitBreaker(Settings{
		Redis:        redisdriver.New(rdb, key, time.Hour),
		Name:         "pod2",
		SyncInterval: time.Hour,
		ReadyToTrip:  func(c LocalCounts) bool { return c.ConsecutiveFailures >= 3 },
	})
	defer pod2.Stop()

	// pod2's NewCircuitBreaker calls NewGeneration(-1,...) which finds key exists
	// and returns current gen, then refreshCache loads Open state.
	assertState(t, pod2, StateOpen)
}

func TestIntegration_IsDegraded_FalseWhenRedisUp(t *testing.T) {
	rdb := newTestRedis(t)
	key := testKey(t)
	defer rdb.Del(context.Background(), key)

	cb := NewCircuitBreaker(Settings{
		Redis:        redisdriver.New(rdb, key, time.Hour),
		Name:         t.Name(),
		SyncInterval: time.Hour,
	})
	defer cb.Stop()

	if cb.IsDegraded() {
		t.Fatal("should not be degraded when Redis is available")
	}
}

// ─── benchmarks ─────────────────────────────────────────────────────────────
//
// Run with: go test -bench=. -benchmem ./circuit_breaker/
//
// Expected results (Apple M-class / ~3 GHz):
//
//	BenchmarkHotPath_Closed_Sequential  ~  60 ns/op   0 allocs/op
//	BenchmarkHotPath_Closed_Parallel    ~  30 ns/op   0 allocs/op   (8 cores)
//	BenchmarkHotPath_Open_Parallel      ~  15 ns/op   0 allocs/op   (cache read only)
//	BenchmarkBeforeRequest              ~  15 ns/op   0 allocs/op
//
// The integration benchmark should show the same numbers as the fake benchmarks,
// proving Redis is never touched on the hot path.

func benchCB() *CircuitBreaker {
	return NewCircuitBreaker(Settings{
		Redis:        &fakeDriver{},
		Name:         "bench",
		SyncInterval: time.Hour,                               // no background Redis I/O
		ReadyToTrip:  func(LocalCounts) bool { return false }, // never trip during bench
	})
}

func BenchmarkHotPath_Closed_Sequential(b *testing.B) {
	cb := benchCB()
	defer cb.Stop()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cb.Execute(ctx, func() (any, error) { return nil, nil })
	}
}

func BenchmarkHotPath_Closed_Parallel(b *testing.B) {
	cb := benchCB()
	defer cb.Stop()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cb.Execute(ctx, func() (any, error) { return nil, nil })
		}
	})
}

// BenchmarkHotPath_Open_Parallel measures the fast rejection path.
// The RLock + struct copy + ErrOpenState return should be cheaper than the success path.
func BenchmarkHotPath_Open_Parallel(b *testing.B) {
	cb := benchCB()
	defer cb.Stop()
	// Force Open without touching Redis.
	cb.cacheMu.Lock()
	cb.cache = stateSnapshot{state: StateOpen, generation: 1}
	cb.cacheMu.Unlock()

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cb.Execute(ctx, func() (any, error) { return nil, nil })
		}
	})
}

// BenchmarkBeforeRequest isolates the pure allow/reject cost (no afterRequest).
func BenchmarkBeforeRequest(b *testing.B) {
	cb := benchCB()
	defer cb.Stop()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cb.beforeRequest()
	}
}

// BenchmarkHotPath_MixedFailures tests the realistic case: 10% failure rate,
// threshold high enough never to trip. Exercises the atomic counters on the failure path.
func BenchmarkHotPath_MixedFailures_Parallel(b *testing.B) {
	cb := NewCircuitBreaker(Settings{
		Redis:        &fakeDriver{},
		Name:         "bench-mixed",
		SyncInterval: time.Hour,
		ReadyToTrip:  func(c LocalCounts) bool { return c.ConsecutiveFailures >= 1_000_000 },
	})
	defer cb.Stop()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			i++
			reqErr := error(nil)
			if i%10 == 0 {
				reqErr = errDown
			}
			cb.Execute(ctx, func() (any, error) { return nil, reqErr })
		}
	})
}

// BenchmarkIntegration_Closed_Parallel demonstrates that real Redis behind the Driver
// adds zero latency to the hot path (same numbers as the fake benchmark above).
func BenchmarkIntegration_Closed_Parallel(b *testing.B) {
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		b.Skipf("invalid Redis URL: %v", err)
	}
	rdb := redis.NewClient(opt)
	defer rdb.Close()
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		b.Skipf("Redis unavailable: %v", err)
	}

	key := "bench:cb:integration"
	defer rdb.Del(context.Background(), key)

	cb := NewCircuitBreaker(Settings{
		Redis:        redisdriver.New(rdb, key, time.Hour),
		Name:         "bench-integration",
		SyncInterval: time.Hour,                               // no background reads during bench
		ReadyToTrip:  func(LocalCounts) bool { return false }, // never trip
	})
	defer cb.Stop()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cb.Execute(ctx, func() (any, error) { return nil, nil })
		}
	})
}
