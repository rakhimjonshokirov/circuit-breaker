// Package gobreaker implements the Circuit Breaker pattern.
// See https://msdn.microsoft.com/en-us/library/dn589784.aspx.
package circuitbreaker

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
	defaultInterval = time.Duration(0) * time.Second
	defaultTimeout  = time.Duration(60) * time.Second

	ten = 10

	requests             = "requests"
	totalSuccesses       = "total_successes"
	totalFailures        = "total_failures"
	consecutiveSuccesses = "consecutive_successes"
	consecutiveFailures  = "consecutive_failures"
)

var (
	// ErrOpenState is returned when the CB state is open
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

// Settings configures CircuitBreaker:
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
// ReadyToTrip is called with a copy of Counts whenever a request fails in the closed state.
// If ReadyToTrip returns true, the CircuitBreaker will be placed into the open state.
// If ReadyToTrip is nil, default ReadyToTrip is used.
// Default ReadyToTrip returns true when the number of consecutive failures is more than 5.
//
// OnStateChange is called whenever the state of the CircuitBreaker changes.
//
// IsSuccessful is called with the error returned from a request.
// If IsSuccessful returns true, the error is counted as a success.
// Otherwise the error is counted as a failure.
// If IsSuccessful is nil, default IsSuccessful is used, which returns false for all non-nil errors.
type Settings struct {
	Redis Driver

	Name          string
	MaxRequests   uint32
	Interval      time.Duration
	Timeout       time.Duration
	ReadyToTrip   func(counts Driver) bool
	OnStateChange func(name string, from State, to State)
	IsSuccessful  func(err error) bool
}

type Driver interface {
	OnRequest(ctx context.Context) error
	OnSuccess(ctx context.Context) error
	OnFailure(ctx context.Context) error
	Clear(ctx context.Context) error
	GetField(ctx context.Context, field string) (uint32, error)
}

// CircuitBreaker is a state machine to prevent sending requests that are likely to fail.
type CircuitBreaker struct {
	name          string
	maxRequests   uint32
	interval      time.Duration
	timeout       time.Duration
	readyToTrip   func(counts Driver) bool
	isSuccessful  func(err error) bool
	onStateChange func(name string, from State, to State)

	mutex      sync.Mutex
	state      State
	generation uint64
	counts     Driver
	expiry     time.Time
}

// NewCircuitBreaker returns a new CircuitBreaker configured with the given Settings.
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

	cb.readyToTrip = defaultReadyToTrip
	if st.ReadyToTrip != nil {
		cb.readyToTrip = st.ReadyToTrip
	}

	cb.isSuccessful = defaultIsSuccessful
	if st.IsSuccessful != nil {
		cb.isSuccessful = st.IsSuccessful
	}

	cb.counts = st.Redis

	cb.toNewGeneration(context.Background(), time.Now())

	return cb
}

func defaultReadyToTrip(counts Driver) bool {
	consecutiveFailures, _ := counts.GetField(context.Background(), consecutiveFailures)
	return consecutiveFailures > ten
}

func defaultIsSuccessful(err error) bool {
	return err == nil
}

// Name returns the name of the CircuitBreaker.
func (cb *CircuitBreaker) Name() string {
	return cb.name
}

// State returns the current state of the CircuitBreaker.
func (cb *CircuitBreaker) State(ctx context.Context) State {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	state, _ := cb.currentState(ctx, now)
	return state
}

// Execute runs the given request if the CircuitBreaker accepts it.
// Execute returns an error instantly if the CircuitBreaker rejects the request.
// Otherwise, Execute returns the result of the request.
// If a panic occurs in the request, the CircuitBreaker handles it as an error
// and causes the same panic again.
func (cb *CircuitBreaker) Execute(
	ctx context.Context, req func() (interface{}, error),
) (interface{}, error) {
	generation, err := cb.beforeRequest(ctx)
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

func (cb *CircuitBreaker) toNewGeneration(ctx context.Context, now time.Time) {
	cb.generation++
	_ = cb.counts.Clear(ctx)

	var zero time.Time
	switch cb.state {
	case StateClosed:
		cb.expiry = zero
		if cb.interval > 0 {
			cb.expiry = now.Add(cb.interval)
		}
	case StateOpen:
		cb.expiry = now.Add(cb.timeout)
	default: // StateHalfOpen
		cb.expiry = zero
	}
}

func (cb *CircuitBreaker) currentState(ctx context.Context, now time.Time) (State, uint64) { //nolint:gocritic
	switch cb.state { 
	case StateClosed:
		if !cb.expiry.IsZero() && cb.expiry.Before(now) {
			cb.toNewGeneration(ctx, now)
		}
	case StateOpen:
		if cb.expiry.Before(now) {
			cb.setState(ctx, StateHalfOpen, now)
		}
	}
	return cb.state, cb.generation
}

func (cb *CircuitBreaker) setState(ctx context.Context, state State, now time.Time) {
	if cb.state == state {
		return
	}

	prev := cb.state
	cb.state = state

	cb.toNewGeneration(ctx, now)

	if cb.onStateChange != nil {
		cb.onStateChange(cb.name, prev, state)
	}
}

func (cb *CircuitBreaker) beforeRequest(ctx context.Context) (uint64, error) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	state, generation := cb.currentState(ctx, now)

	if state == StateOpen {
		return generation, ErrOpenState
	}

	_ = cb.counts.OnRequest(ctx)
	return generation, nil
}

func (cb *CircuitBreaker) afterRequest(ctx context.Context, before uint64, success bool) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	state, generation := cb.currentState(ctx, now)
	if generation != before {
		return
	}

	if success {
		cb.onSuccess(ctx, state, now)
		return
	}

	cb.onFailure(ctx, state, now)
}

func (cb *CircuitBreaker) onSuccess(ctx context.Context, state State, now time.Time) {
	switch state { 
	case StateClosed:
		_ = cb.counts.OnSuccess(ctx)
	case StateHalfOpen:
		_ = cb.counts.OnSuccess(ctx)
		consecutiveSuccesses, _ := cb.counts.GetField(ctx, consecutiveSuccesses)
		if consecutiveSuccesses >= cb.maxRequests {
			cb.setState(ctx, StateClosed, now)
		}
	}
}

func (cb *CircuitBreaker) onFailure(ctx context.Context, state State, now time.Time) {
	switch state {
	case StateClosed:
		_ = cb.counts.OnFailure(ctx)
		if cb.readyToTrip(cb.counts) {
			cb.setState(ctx, StateOpen, now)
		}
	case StateHalfOpen:
		cb.setState(ctx, StateOpen, now)
	}
}
