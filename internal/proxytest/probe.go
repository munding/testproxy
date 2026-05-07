package proxytest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type DialFunc func(context.Context, ProxyConfig, string, time.Duration) (net.Conn, error)

type MaxConnOptions struct {
	Limit           int
	RatePerSecond   int
	ConnectTimeout  time.Duration
	ConnIdleTimeout time.Duration
	OnStatus        func(MaxConnStatus)
}

type MaxConnStatus struct {
	Attempts    int
	Successful  int
	Failed      int
	InFlight    int
	Live        int
	MaxLive     int
	ErrorCounts map[string]int
	Elapsed     time.Duration
}

type MaxConnResult struct {
	Attempts     int
	Successful   int
	Failed       int
	InFlight     int
	Live         int
	MaxLive      int
	StopError    error
	ErrorCounts  map[string]int
	StartedAt    time.Time
	CompletedAt  time.Time
	Elapsed      time.Duration
	ReachedLimit bool
}

type MaxConnStep struct {
	Attempt    int
	Success    bool
	Live       int
	MaxLive    int
	ErrorKey   string
	Elapsed    time.Duration
	StopReason string
}

type IdleResult struct {
	Duration time.Duration
	Closed   bool
	Reason   string
	Err      error
}

func MaxConnections(ctx context.Context, cfg ProxyConfig, target string, opts MaxConnOptions, dialer DialFunc) (result MaxConnResult) {
	if opts.Limit < 0 {
		opts.Limit = 0
	}
	if opts.RatePerSecond <= 0 {
		opts.RatePerSecond = 1
	}
	if dialer == nil {
		dialer = Dial
	}

	runtime := newMaxConnRuntime(time.Now(), opts.Limit, opts.ConnIdleTimeout)
	stopStatus := startMaxConnStatusReporter(ctx, runtime, opts.OnStatus)

	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		result = runtime.result()
		stopStatus()
		runtime.closeLiveConnections()
	}()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return result
		case <-ticker.C:
			started := runtime.startAttempts(opts.RatePerSecond)
			for i := 0; i < started; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					conn, err := dialer(ctx, cfg, target, opts.ConnectTimeout)
					if err != nil {
						runtime.recordFailure(err)
						return
					}
					runtime.recordSuccess(conn)
				}()
			}
			if opts.Limit > 0 && runtime.atLimit() {
				runtime.markReachedLimit()
				return result
			}
		}
	}
}

type maxConnRuntime struct {
	mu          sync.Mutex
	startedAt   time.Time
	limit       int
	idleTimeout time.Duration
	attempts    int
	successful  int
	failed      int
	live        int
	maxLive     int
	nextConnID  int
	stopError   error
	errorCount  map[string]int
	reached     bool
	liveConns   map[int]net.Conn
}

func newMaxConnRuntime(startedAt time.Time, limit int, idleTimeout time.Duration) *maxConnRuntime {
	return &maxConnRuntime{
		startedAt:   startedAt,
		limit:       limit,
		idleTimeout: idleTimeout,
		errorCount:  make(map[string]int),
		liveConns:   make(map[int]net.Conn),
	}
}

func (r *maxConnRuntime) startAttempts(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.limit > 0 && r.attempts >= r.limit {
		return 0
	}
	if r.limit > 0 && r.attempts+n > r.limit {
		n = r.limit - r.attempts
	}
	r.attempts += n
	return n
}

func (r *maxConnRuntime) recordFailure(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failed++
	key := classifyError(err)
	if r.stopError == nil {
		r.stopError = err
	}
	r.errorCount[key]++
}

func (r *maxConnRuntime) recordSuccess(conn net.Conn) {
	r.mu.Lock()
	r.successful++
	r.nextConnID++
	id := r.nextConnID
	r.liveConns[id] = conn
	r.live = len(r.liveConns)
	if r.live > r.maxLive {
		r.maxLive = r.live
	}
	r.mu.Unlock()
	go monitorConn(id, conn, r, r.idleTimeout)
}

func (r *maxConnRuntime) recordClosed(id int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if conn, ok := r.liveConns[id]; ok {
		conn.Close()
		delete(r.liveConns, id)
		r.live = len(r.liveConns)
	}
}

func (r *maxConnRuntime) atLimit() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.limit > 0 && r.attempts >= r.limit
}

func (r *maxConnRuntime) markReachedLimit() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reached = true
}

func (r *maxConnRuntime) snapshot() MaxConnStatus {
	r.mu.Lock()
	defer r.mu.Unlock()
	counts := make(map[string]int, len(r.errorCount))
	for key, value := range r.errorCount {
		counts[key] = value
	}
	return MaxConnStatus{
		Attempts:    r.attempts,
		Successful:  r.successful,
		Failed:      r.failed,
		InFlight:    r.inFlightLocked(),
		Live:        r.live,
		MaxLive:     r.maxLive,
		ErrorCounts: counts,
		Elapsed:     time.Since(r.startedAt),
	}
}

func (r *maxConnRuntime) result() MaxConnResult {
	r.mu.Lock()
	defer r.mu.Unlock()
	completedAt := time.Now()
	counts := make(map[string]int, len(r.errorCount))
	for key, value := range r.errorCount {
		counts[key] = value
	}
	return MaxConnResult{
		Attempts:     r.attempts,
		Successful:   r.successful,
		Failed:       r.failed,
		InFlight:     r.inFlightLocked(),
		Live:         r.live,
		MaxLive:      r.maxLive,
		StopError:    r.stopError,
		ErrorCounts:  counts,
		StartedAt:    r.startedAt,
		CompletedAt:  completedAt,
		Elapsed:      completedAt.Sub(r.startedAt),
		ReachedLimit: r.reached,
	}
}

func (r *maxConnRuntime) inFlightLocked() int {
	inFlight := r.attempts - r.successful - r.failed
	if inFlight < 0 {
		return 0
	}
	return inFlight
}

func (r *maxConnRuntime) closeLiveConnections() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, conn := range r.liveConns {
		conn.Close()
		delete(r.liveConns, id)
	}
	r.live = 0
}

func startMaxConnStatusReporter(ctx context.Context, state *maxConnRuntime, onStatus func(MaxConnStatus)) func() {
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		if onStatus == nil {
			return
		}
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				onStatus(state.snapshot())
			case <-ctx.Done():
				return
			case <-done:
				return
			}
		}
	}()
	return func() {
		close(done)
		<-stopped
	}
}

func monitorConn(id int, conn net.Conn, runtime *maxConnRuntime, idleTimeout time.Duration) {
	buf := make([]byte, 1)
	for {
		if idleTimeout > 0 {
			conn.SetReadDeadline(time.Now().Add(idleTimeout))
		}
		_, err := conn.Read(buf)
		if err != nil {
			runtime.recordClosed(id)
			return
		}
	}
}

func MeasureIdle(ctx context.Context, conn net.Conn, probeInterval time.Duration) IdleResult {
	if probeInterval <= 0 {
		probeInterval = time.Second
	}
	start := time.Now()
	buf := make([]byte, 1)
	for {
		select {
		case <-ctx.Done():
			return IdleResult{Duration: time.Since(start), Closed: false, Reason: "timeout", Err: ctx.Err()}
		default:
		}

		conn.SetReadDeadline(time.Now().Add(probeInterval))
		n, err := conn.Read(buf)
		if err == nil && n > 0 {
			return IdleResult{Duration: time.Since(start), Closed: false, Reason: "received_data"}
		}
		if ne, ok := err.(net.Error); ok && ne.Timeout() {
			continue
		}
		if errors.Is(err, io.EOF) {
			return IdleResult{Duration: time.Since(start), Closed: true, Reason: "fin", Err: err}
		}
		if err != nil {
			return IdleResult{Duration: time.Since(start), Closed: true, Reason: "closed_or_reset", Err: err}
		}
	}
}

func classifyError(err error) string {
	var httpErr *HTTPRefusalError
	if errors.As(err, &httpErr) {
		return fmt.Sprintf("http_%d", httpErr.StatusCode)
	}
	var socksErr *SOCKS5RefusalError
	if errors.As(err, &socksErr) {
		return fmt.Sprintf("socks5_0x%02x", socksErr.Code)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	if ne, ok := err.(net.Error); ok && ne.Timeout() {
		return "timeout"
	}
	if errors.Is(err, io.EOF) {
		return "eof"
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return "timeout"
	}
	text := strings.TrimSpace(err.Error())
	if text == "" {
		return "unknown"
	}
	lower := strings.ToLower(text)
	switch {
	case strings.Contains(lower, "connection refused"):
		return "connection_refused"
	case strings.Contains(lower, "connection reset"):
		return "connection_reset"
	case strings.Contains(lower, "broken pipe"):
		return "broken_pipe"
	case strings.Contains(lower, "no such host"):
		return "dns_error"
	case strings.Contains(lower, "authentication failed"):
		return "auth_failed"
	case strings.Contains(lower, "timeout") || strings.Contains(lower, "deadline exceeded"):
		return "timeout"
	case strings.Contains(lower, "read connect response"):
		return "http_connect_response_error"
	case strings.Contains(lower, "write connect request"):
		return "http_connect_write_error"
	case strings.Contains(lower, "socks5"):
		return "socks5_protocol_error"
	case strings.Contains(lower, "http"):
		return "http_protocol_error"
	default:
		return "setup_error"
	}
}
