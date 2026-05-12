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
			return f.generation, true, nil
		}
		f.state, f.expiry, f.generation, f.initialized = toState, expiry, 1, true
		return 1, true, nil
	}
	if f.state != fromState {
		return f.generation, false, nil
	}
	f.generation++
	f.state, f.expiry = toState, expiry
	return f.generation, true, nil
}

// ─── helpers ────────────────────────────────────────────────────────────────

func noSyncCB(fake *fakeDriver, patch ...func(*Settings)) *CircuitBreaker {
	st := Settings{
		Driver:       fake,
		Name:         "test",
		SyncInterval: time.Hour,
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
		cb.Execute(context.Background(), func() error { return reqErr })
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
	assertState(t, cb, StateClosed)

	executeN(cb, 1, errDown)
	assertState(t, cb, StateOpen)
}

func TestOpen_RejectsRequests(t *testing.T) {
	cb := noSyncCB(&fakeDriver{})
	defer cb.Stop()

	executeN(cb, 3, errDown)
	assertState(t, cb, StateOpen)

	err := cb.Execute(context.Background(), func() error { return nil })
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

	cb.tick(context.Background(), time.Now().Add(200*time.Millisecond))
	assertState(t, cb, StateHalfOpen)
}

func TestHalfOpen_ClosesAfterEnoughSuccesses(t *testing.T) {
	fake := &fakeDriver{}
	cb := noSyncCB(fake, func(s *Settings) {
		s.Timeout     = 50 * time.Millisecond
		s.MaxRequests = 2
	})
	defer cb.Stop()

	executeN(cb, 3, errDown)
	cb.tick(context.Background(), time.Now().Add(200*time.Millisecond))
	assertState(t, cb, StateHalfOpen)

	executeN(cb, 1, nil)
	assertState(t, cb, StateHalfOpen)

	executeN(cb, 1, nil)
	assertState(t, cb, StateClosed)
}

func TestHalfOpen_ReOpensOnFailure(t *testing.T) {
	fake := &fakeDriver{}
	cb := noSyncCB(fake, func(s *Settings) { s.Timeout = 50 * time.Millisecond })
	defer cb.Stop()

	executeN(cb, 3, errDown)
	cb.tick(context.Background(), time.Now().Add(200*time.Millisecond))
	assertState(t, cb, StateHalfOpen)

	executeN(cb, 1, errDown)
	assertState(t, cb, StateOpen)
}

func TestHalfOpen_LimitsInFlightProbes(t *testing.T) {
	fake := &fakeDriver{}
	cb := noSyncCB(fake, func(s *Settings) {
		s.Timeout     = 50 * time.Millisecond
		s.MaxRequests = 1
	})
	defer cb.Stop()

	executeN(cb, 3, errDown)
	cb.tick(context.Background(), time.Now().Add(200*time.Millisecond))
	assertState(t, cb, StateHalfOpen)

	// First probe is allowed.
	// Second concurrent probe must be rejected with ErrProbesFull.
	cb.halfOpenInFlight.Store(1) // simulate one probe already in flight

	err := cb.Execute(context.Background(), func() error { return nil })
	if !errors.Is(err, ErrProbesFull) {
		t.Fatalf("expected ErrProbesFull when in-flight limit reached, got %v", err)
	}
}

func TestClosedInterval_ResetsGeneration(t *testing.T) {
	fake := &fakeDriver{}
	cb := noSyncCB(fake, func(s *Settings) { s.Interval = 50 * time.Millisecond })
	defer cb.Stop()

	gen1 := cb.cache.Load().generation

	cb.tick(context.Background(), time.Now().Add(200*time.Millisecond))

	gen2 := cb.cache.Load().generation

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
		cb.Execute(context.Background(), func() error {
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
		s.IsSuccessful = func(error) bool { return true }
	})
	defer cb.Stop()

	executeN(cb, 20, errDown)
	assertState(t, cb, StateClosed)
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

	executeN(cb, 3, errDown)
	cb.tick(context.Background(), time.Now().Add(200*time.Millisecond))
	executeN(cb, 1, nil)

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

	stalGen := cb.cache.Load().generation

	executeN(cb, 3, errDown)
	assertState(t, cb, StateOpen)

	cb.afterRequest(context.Background(), stalGen, true)

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

func TestDegraded_StartsLocalWhenRedisDown(t *testing.T) {
	fake := &fakeDriver{}
	fake.setFailing(true)

	cb := noSyncCB(fake)
	defer cb.Stop()

	if !cb.IsDegraded() {
		t.Fatal("should be degraded when Redis is unreachable at startup")
	}
	assertState(t, cb, StateClosed)

	executeN(cb, 3, errDown)
	assertState(t, cb, StateOpen)
}

func TestDegraded_ReconcileLocalOpenWins(t *testing.T) {
	fake := &fakeDriver{}
	cb := noSyncCB(fake)
	defer cb.Stop()

	fake.setFailing(true)
	cb.degraded.Store(true)

	executeN(cb, 3, errDown)
	assertState(t, cb, StateOpen)

	fake.setFailing(false)
	cb.tick(context.Background(), time.Now())

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

func TestDegraded_ReconcileRedisOpenWins(t *testing.T) {
	fake := &fakeDriver{}
	cb := noSyncCB(fake)
	defer cb.Stop()

	fake.forceState(int64(StateOpen), 5)
	cb.degraded.Store(true)

	cb.tick(context.Background(), time.Now())

	assertState(t, cb, StateOpen)
	if cb.IsDegraded() {
		t.Fatal("should not be degraded after Redis recovery")
	}
}

func TestDegraded_TransitionDuringOutage(t *testing.T) {
	fake := &fakeDriver{}
	fake.setFailing(true)

	cb := noSyncCB(fake, func(s *Settings) { s.Timeout = 50 * time.Millisecond })
	defer cb.Stop()

	executeN(cb, 3, errDown)
	assertState(t, cb, StateOpen)

	cb.tick(context.Background(), time.Now().Add(200*time.Millisecond))
	assertState(t, cb, StateHalfOpen)
}

// ─── multi-pod unit tests ────────────────────────────────────────────────────

func TestTwoPods_TripPropagates(t *testing.T) {
	fake := &fakeDriver{}

	cb1 := noSyncCB(fake, func(s *Settings) { s.Name = "pod1" })
	cb2 := noSyncCB(fake, func(s *Settings) { s.Name = "pod2" })
	defer cb1.Stop()
	defer cb2.Stop()

	executeN(cb1, 3, errDown)
	assertState(t, cb1, StateOpen)
	assertState(t, cb2, StateClosed)

	cb2.tick(context.Background(), time.Now())
	assertState(t, cb2, StateOpen)
}

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

	executeN(cb1, 3, errDown)
	executeN(cb2, 3, errDown)

	firesMu.Lock()
	got := fires
	firesMu.Unlock()

	if got != 1 {
		t.Fatalf("onStateChange (Closed→Open) should fire exactly once across pods; got %d", got)
	}
}

// ─── concurrency ────────────────────────────────────────────────────────────

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
			cb.Execute(context.Background(), func() error { return reqErr })
		}()
	}
	wg.Wait()
}

// ─── integration tests ──────────────────────────────────────────────────────

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
		Driver:       redisdriver.New(rdb, key, time.Hour),
		Name:         t.Name(),
		SyncInterval: 20 * time.Millisecond,
		Timeout:      80 * time.Millisecond,
		ReadyToTrip:  func(c LocalCounts) bool { return c.ConsecutiveFailures >= 3 },
	})
	defer cb.Stop()

	assertState(t, cb, StateClosed)

	executeN(cb, 3, errDown)
	assertState(t, cb, StateOpen)

	err := cb.Execute(context.Background(), func() error { return nil })
	if !errors.Is(err, ErrOpenState) {
		t.Fatalf("expected ErrOpenState while open, got %v", err)
	}

	waitState(t, cb, StateHalfOpen, 200*time.Millisecond)

	executeN(cb, 1, nil)
	assertState(t, cb, StateClosed)
}

func TestIntegration_TwoPods_ShareState(t *testing.T) {
	rdb := newTestRedis(t)
	key := testKey(t)
	defer rdb.Del(context.Background(), key)

	newPod := func(name string) *CircuitBreaker {
		return NewCircuitBreaker(Settings{
			Driver:       redisdriver.New(rdb, key, time.Hour),
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

	waitState(t, pod2, StateOpen, 100*time.Millisecond)
}

func TestIntegration_NewPod_InheritsOpenState(t *testing.T) {
	rdb := newTestRedis(t)
	key := testKey(t)
	defer rdb.Del(context.Background(), key)

	pod1 := NewCircuitBreaker(Settings{
		Driver:       redisdriver.New(rdb, key, time.Hour),
		Name:         "pod1",
		SyncInterval: time.Hour,
		ReadyToTrip:  func(c LocalCounts) bool { return c.ConsecutiveFailures >= 3 },
	})
	defer pod1.Stop()

	executeN(pod1, 3, errDown)
	assertState(t, pod1, StateOpen)

	pod2 := NewCircuitBreaker(Settings{
		Driver:       redisdriver.New(rdb, key, time.Hour),
		Name:         "pod2",
		SyncInterval: time.Hour,
		ReadyToTrip:  func(c LocalCounts) bool { return c.ConsecutiveFailures >= 3 },
	})
	defer pod2.Stop()

	assertState(t, pod2, StateOpen)
}

func TestIntegration_IsDegraded_FalseWhenRedisUp(t *testing.T) {
	rdb := newTestRedis(t)
	key := testKey(t)
	defer rdb.Del(context.Background(), key)

	cb := NewCircuitBreaker(Settings{
		Driver:       redisdriver.New(rdb, key, time.Hour),
		Name:         t.Name(),
		SyncInterval: time.Hour,
	})
	defer cb.Stop()

	if cb.IsDegraded() {
		t.Fatal("should not be degraded when Redis is available")
	}
}

// ─── benchmarks ─────────────────────────────────────────────────────────────

func benchCB() *CircuitBreaker {
	return NewCircuitBreaker(Settings{
		Driver:       &fakeDriver{},
		Name:         "bench",
		SyncInterval: time.Hour,
		ReadyToTrip:  func(LocalCounts) bool { return false },
	})
}

func BenchmarkHotPath_Closed_Sequential(b *testing.B) {
	cb := benchCB()
	defer cb.Stop()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cb.Execute(ctx, func() error { return nil })
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
			cb.Execute(ctx, func() error { return nil })
		}
	})
}

func BenchmarkHotPath_Open_Parallel(b *testing.B) {
	cb := benchCB()
	defer cb.Stop()
	cb.cache.Store(&stateSnapshot{state: StateOpen, generation: 1})

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cb.Execute(ctx, func() error { return nil })
		}
	})
}

func BenchmarkBeforeRequest(b *testing.B) {
	cb := benchCB()
	defer cb.Stop()
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		cb.beforeRequest()
	}
}

func BenchmarkHotPath_MixedFailures_Parallel(b *testing.B) {
	cb := NewCircuitBreaker(Settings{
		Driver:       &fakeDriver{},
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
			cb.Execute(ctx, func() error { return reqErr })
		}
	})
}

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
		Driver:       redisdriver.New(rdb, key, time.Hour),
		Name:         "bench-integration",
		SyncInterval: time.Hour,
		ReadyToTrip:  func(LocalCounts) bool { return false },
	})
	defer cb.Stop()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cb.Execute(ctx, func() error { return nil })
		}
	})
}
