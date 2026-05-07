package proxytest

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"time"
)

func Dial(ctx context.Context, cfg ProxyConfig, target string, connectTimeout time.Duration) (net.Conn, error) {
	switch cfg.Scheme {
	case "http":
		return dialHTTPConnect(ctx, cfg, target, connectTimeout)
	case "socks5":
		return dialSOCKS5(ctx, cfg, target, connectTimeout)
	default:
		return nil, fmt.Errorf("unsupported proxy scheme %q", cfg.Scheme)
	}
}

func dialHTTPConnect(ctx context.Context, cfg ProxyConfig, target string, connectTimeout time.Duration) (net.Conn, error) {
	var d net.Dialer
	if connectTimeout > 0 {
		d.Timeout = connectTimeout
	}
	conn, err := d.DialContext(ctx, "tcp", cfg.Address)
	if err != nil {
		return nil, fmt.Errorf("dial proxy: %w", err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		conn.SetDeadline(deadline)
	}
	if connectTimeout > 0 {
		conn.SetDeadline(time.Now().Add(connectTimeout))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodConnect, "http://"+target, nil)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("build CONNECT request: %w", err)
	}
	req.Host = target
	if cfg.Username != "" || cfg.Password != "" {
		token := base64.StdEncoding.EncodeToString([]byte(cfg.Username + ":" + cfg.Password))
		req.Header.Set("Proxy-Authorization", "Basic "+token)
	}

	if err := req.Write(conn); err != nil {
		conn.Close()
		return nil, fmt.Errorf("write CONNECT request: %w", err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), req)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("read CONNECT response: %w", err)
	}
	if resp.Body != nil {
		resp.Body.Close()
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		conn.Close()
		return nil, NewHTTPRefusal(resp)
	}
	conn.SetDeadline(time.Time{})
	return conn, nil
}
