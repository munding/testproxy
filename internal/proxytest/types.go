package proxytest

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
)

type ProxyConfig struct {
	Scheme   string
	Address  string
	Username string
	Password string
}

type HTTPRefusalError struct {
	StatusCode int
	Status     string
}

func (e *HTTPRefusalError) Error() string {
	return fmt.Sprintf("http proxy refused CONNECT: %s", e.Status)
}

func AsHTTPRefusal(err error, target **HTTPRefusalError) bool {
	return errors.As(err, target)
}

func NewHTTPRefusal(resp *http.Response) *HTTPRefusalError {
	return &HTTPRefusalError{StatusCode: resp.StatusCode, Status: resp.Status}
}

type SOCKS5RefusalError struct {
	Code  byte
	Label string
}

func (e *SOCKS5RefusalError) Error() string {
	return fmt.Sprintf("socks5 proxy refused CONNECT: %s (0x%02x)", e.Label, e.Code)
}

func AsSOCKS5Refusal(err error, target **SOCKS5RefusalError) bool {
	return errors.As(err, target)
}

func ParseProxyURL(raw string) (ProxyConfig, error) {
	if raw == "" {
		return ProxyConfig{}, errors.New("proxy URL is required")
	}

	u, err := url.Parse(raw)
	if err != nil {
		return ProxyConfig{}, fmt.Errorf("parse proxy URL: %w", err)
	}

	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "socks5" {
		return ProxyConfig{}, fmt.Errorf("unsupported proxy scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return ProxyConfig{}, errors.New("proxy URL must include host:port")
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil || host == "" || port == "" {
		return ProxyConfig{}, errors.New("proxy URL must include host:port")
	}

	cfg := ProxyConfig{
		Scheme:  scheme,
		Address: net.JoinHostPort(host, port),
	}
	if u.User != nil {
		cfg.Username = u.User.Username()
		cfg.Password, _ = u.User.Password()
	}
	return cfg, nil
}

func NormalizeTarget(target string) (string, error) {
	if target == "" {
		return "", errors.New("target is required")
	}
	host, port, err := net.SplitHostPort(target)
	if err != nil || host == "" || port == "" {
		return "", errors.New("target must be host:port")
	}
	return net.JoinHostPort(host, port), nil
}
