package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"time"

	"testproxy/internal/proxytest"
)

type commandConfig struct {
	command         string
	proxy           proxytest.ProxyConfig
	target          string
	connectTimeout  time.Duration
	idleTimeout     time.Duration
	probeInterval   time.Duration
	limit           int
	rate            int
	connIdleTimeout time.Duration
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	cfg, err := parseCommand(args)
	if err != nil {
		return err
	}

	switch cfg.command {
	case "idle":
		return runIdle(cfg, stdout, stderr)
	case "maxconn":
		return runMaxConn(cfg, stdout)
	default:
		return fmt.Errorf("unknown command %q", cfg.command)
	}
}

func parseCommand(args []string) (commandConfig, error) {
	if len(args) == 0 {
		return commandConfig{}, errors.New("usage: testproxy <idle|maxconn> -x proxy-url -t host:port")
	}

	cfg := commandConfig{
		command:        args[0],
		connectTimeout: 10 * time.Second,
		idleTimeout:    time.Hour,
		probeInterval:  time.Second,
		limit:          500,
		rate:           1,
	}

	fs := flag.NewFlagSet(args[0], flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var proxyURL string
	var target string
	fs.StringVar(&proxyURL, "x", "", "proxy URL")
	fs.StringVar(&target, "t", "", "target host:port")

	switch cfg.command {
	case "idle":
		fs.DurationVar(&cfg.connectTimeout, "connect-timeout", cfg.connectTimeout, "proxy connection setup timeout")
		fs.DurationVar(&cfg.idleTimeout, "timeout", cfg.idleTimeout, "maximum idle wait duration")
		fs.DurationVar(&cfg.probeInterval, "probe-interval", cfg.probeInterval, "read deadline interval")
	case "maxconn":
		fs.DurationVar(&cfg.connectTimeout, "connect-timeout", cfg.connectTimeout, "per-connection setup timeout")
		fs.DurationVar(&cfg.connIdleTimeout, "conn-idle-timeout", cfg.connIdleTimeout, "close established connection after this idle read timeout, 0 disables it")
		fs.IntVar(&cfg.limit, "limit", cfg.limit, "maximum connection attempts")
		fs.IntVar(&cfg.rate, "rate", cfg.rate, "connection attempts to start per second")
	default:
		return commandConfig{}, fmt.Errorf("unknown command %q", cfg.command)
	}

	if err := fs.Parse(args[1:]); err != nil {
		return commandConfig{}, err
	}
	proxyCfg, err := proxytest.ParseProxyURL(proxyURL)
	if err != nil {
		return commandConfig{}, err
	}
	normalizedTarget, err := proxytest.NormalizeTarget(target)
	if err != nil {
		return commandConfig{}, err
	}
	cfg.proxy = proxyCfg
	cfg.target = normalizedTarget

	if cfg.limit < 0 {
		return commandConfig{}, errors.New("--limit must be >= 0")
	}
	if cfg.rate <= 0 {
		return commandConfig{}, errors.New("--rate must be > 0")
	}
	if cfg.connectTimeout < 0 {
		return commandConfig{}, errors.New("--connect-timeout must be >= 0")
	}
	if cfg.connIdleTimeout < 0 {
		return commandConfig{}, errors.New("--conn-idle-timeout must be >= 0")
	}
	if cfg.command == "idle" && cfg.idleTimeout <= 0 {
		return commandConfig{}, errors.New("--timeout must be > 0")
	}
	return cfg, nil
}

func runIdle(cfg commandConfig, stdout, stderr io.Writer) error {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.connectTimeout)
	conn, err := proxytest.Dial(ctx, cfg.proxy, cfg.target, cfg.connectTimeout)
	cancel()
	if err != nil {
		return err
	}
	defer conn.Close()

	idleCtx, idleCancel := context.WithTimeout(context.Background(), cfg.idleTimeout)
	defer idleCancel()
	stopProgress := startIdleProgress(stderr)
	result := proxytest.MeasureIdle(idleCtx, conn, cfg.probeInterval)
	stopProgress()

	fmt.Fprintf(stdout, "proxy=%s://%s target=%s\n", cfg.proxy.Scheme, cfg.proxy.Address, cfg.target)
	fmt.Fprintf(stdout, "idle_duration=%s\n", result.Duration.Round(time.Millisecond))
	fmt.Fprintf(stdout, "closed=%t\n", result.Closed)
	fmt.Fprintf(stdout, "reason=%s\n", result.Reason)
	if result.Err != nil && result.Closed {
		fmt.Fprintf(stdout, "close_error=%v\n", result.Err)
	}
	return nil
}

func startIdleProgress(w io.Writer) func() {
	done := make(chan struct{})
	stopped := make(chan struct{})
	start := time.Now()
	go func() {
		defer close(stopped)
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				fmt.Fprintf(w, "\ridle_elapsed=%s", time.Since(start).Round(time.Second))
			case <-done:
				fmt.Fprint(w, "\r\n")
				return
			}
		}
	}()
	return func() {
		close(done)
		<-stopped
	}
}

func runMaxConn(cfg commandConfig, stdout io.Writer) error {
	fmt.Fprintf(stdout, "proxy=%s://%s target=%s\n", cfg.proxy.Scheme, cfg.proxy.Address, cfg.target)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	result := proxytest.MaxConnections(ctx, cfg.proxy, cfg.target, proxytest.MaxConnOptions{
		Limit:           cfg.limit,
		RatePerSecond:   cfg.rate,
		ConnectTimeout:  cfg.connectTimeout,
		ConnIdleTimeout: cfg.connIdleTimeout,
		OnStatus: func(status proxytest.MaxConnStatus) {
			printMaxConnStatus(stdout, status)
		},
	}, nil)

	fmt.Fprintln(stdout, "----------------------------------------")
	fmt.Fprintf(stdout, "attempts=%d\n", result.Attempts)
	fmt.Fprintf(stdout, "successful=%d\n", result.Successful)
	fmt.Fprintf(stdout, "failed=%d\n", result.Failed)
	fmt.Fprintf(stdout, "live_connections=%d\n", result.Live)
	fmt.Fprintf(stdout, "max_live_connections=%d\n", result.MaxLive)
	fmt.Fprintf(stdout, "elapsed=%s\n", result.Elapsed.Round(time.Millisecond))
	fmt.Fprintf(stdout, "reached_limit=%t\n", result.ReachedLimit)
	if len(result.ErrorCounts) > 0 {
		fmt.Fprintln(stdout, "error_summary:")
		keys := make([]string, 0, len(result.ErrorCounts))
		for key := range result.ErrorCounts {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			fmt.Fprintf(stdout, "  %s=%d\n", key, result.ErrorCounts[key])
		}
	}
	return nil
}

func printMaxConnStatus(stdout io.Writer, status proxytest.MaxConnStatus) {
	fmt.Fprintf(stdout, "[status] elapsed=%s attempts=%d live_connections=%d max_live_connections=%d successful=%d failed=%d",
		status.Elapsed.Round(time.Second),
		status.Attempts,
		status.Live,
		status.MaxLive,
		status.Successful,
		status.Failed,
	)
	fmt.Fprintln(stdout)
}
