package proxytest

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

const (
	socks5Version       = 0x05
	socks5CmdConnect    = 0x01
	socks5NoAuth        = 0x00
	socks5UserPassAuth  = 0x02
	socks5NoAcceptable  = 0xff
	socks5AtypIPv4      = 0x01
	socks5AtypDomain    = 0x03
	socks5AtypIPv6      = 0x04
	socks5AuthVersion   = 0x01
	socks5AuthSucceeded = 0x00
)

func dialSOCKS5(ctx context.Context, cfg ProxyConfig, target string, connectTimeout time.Duration) (net.Conn, error) {
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

	if err := socks5NegotiateAuth(conn, cfg); err != nil {
		conn.Close()
		return nil, err
	}
	if err := socks5Connect(conn, target); err != nil {
		conn.Close()
		return nil, err
	}
	conn.SetDeadline(time.Time{})
	return conn, nil
}

func socks5NegotiateAuth(conn net.Conn, cfg ProxyConfig) error {
	methods := []byte{socks5NoAuth}
	if cfg.Username != "" || cfg.Password != "" {
		methods = append(methods, socks5UserPassAuth)
	}

	greeting := []byte{socks5Version, byte(len(methods))}
	greeting = append(greeting, methods...)
	if _, err := conn.Write(greeting); err != nil {
		return fmt.Errorf("write socks5 greeting: %w", err)
	}

	var selected [2]byte
	if _, err := io.ReadFull(conn, selected[:]); err != nil {
		return fmt.Errorf("read socks5 method selection: %w", err)
	}
	if selected[0] != socks5Version {
		return fmt.Errorf("invalid socks5 version %d", selected[0])
	}
	switch selected[1] {
	case socks5NoAuth:
		return nil
	case socks5UserPassAuth:
		return socks5UsernamePasswordAuth(conn, cfg)
	case socks5NoAcceptable:
		return fmt.Errorf("socks5 proxy has no acceptable auth method")
	default:
		return fmt.Errorf("socks5 proxy selected unsupported auth method 0x%02x", selected[1])
	}
}

func socks5UsernamePasswordAuth(conn net.Conn, cfg ProxyConfig) error {
	if len(cfg.Username) > 255 || len(cfg.Password) > 255 {
		return fmt.Errorf("socks5 username and password must be <= 255 bytes")
	}
	req := []byte{socks5AuthVersion, byte(len(cfg.Username))}
	req = append(req, cfg.Username...)
	req = append(req, byte(len(cfg.Password)))
	req = append(req, cfg.Password...)
	if _, err := conn.Write(req); err != nil {
		return fmt.Errorf("write socks5 auth request: %w", err)
	}
	var resp [2]byte
	if _, err := io.ReadFull(conn, resp[:]); err != nil {
		return fmt.Errorf("read socks5 auth response: %w", err)
	}
	if resp[0] != socks5AuthVersion || resp[1] != socks5AuthSucceeded {
		return fmt.Errorf("socks5 username/password authentication failed")
	}
	return nil
}

func socks5Connect(conn net.Conn, target string) error {
	host, portText, err := net.SplitHostPort(target)
	if err != nil {
		return fmt.Errorf("split target: %w", err)
	}
	port64, err := strconv.ParseUint(portText, 10, 16)
	if err != nil {
		return fmt.Errorf("parse target port: %w", err)
	}

	req := []byte{socks5Version, socks5CmdConnect, 0x00}
	ip := net.ParseIP(host)
	ip4 := ip.To4()
	ip16 := ip.To16()
	switch {
	case ip4 != nil:
		req = append(req, socks5AtypIPv4)
		req = append(req, ip4...)
	case ip16 != nil:
		req = append(req, socks5AtypIPv6)
		req = append(req, ip16...)
	default:
		if len(host) > 255 {
			return fmt.Errorf("target domain is too long")
		}
		req = append(req, socks5AtypDomain, byte(len(host)))
		req = append(req, host...)
	}
	var port [2]byte
	binary.BigEndian.PutUint16(port[:], uint16(port64))
	req = append(req, port[:]...)

	if _, err := conn.Write(req); err != nil {
		return fmt.Errorf("write socks5 CONNECT request: %w", err)
	}

	var header [4]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return fmt.Errorf("read socks5 CONNECT response: %w", err)
	}
	if header[0] != socks5Version {
		return fmt.Errorf("invalid socks5 response version %d", header[0])
	}
	if header[1] != 0x00 {
		return &SOCKS5RefusalError{Code: header[1], Label: socks5ReplyLabel(header[1])}
	}

	if err := discardSOCKS5BoundAddress(conn, header[3]); err != nil {
		return err
	}
	return nil
}

func discardSOCKS5BoundAddress(conn net.Conn, atyp byte) error {
	switch atyp {
	case socks5AtypIPv4:
		_, err := io.CopyN(io.Discard, conn, 4+2)
		return err
	case socks5AtypIPv6:
		_, err := io.CopyN(io.Discard, conn, 16+2)
		return err
	case socks5AtypDomain:
		var length [1]byte
		if _, err := io.ReadFull(conn, length[:]); err != nil {
			return fmt.Errorf("read socks5 bound domain length: %w", err)
		}
		_, err := io.CopyN(io.Discard, conn, int64(length[0])+2)
		return err
	default:
		return fmt.Errorf("unsupported socks5 bound address type 0x%02x", atyp)
	}
}

func socks5ReplyLabel(code byte) string {
	switch code {
	case 0x01:
		return "general failure"
	case 0x02:
		return "connection not allowed"
	case 0x03:
		return "network unreachable"
	case 0x04:
		return "host unreachable"
	case 0x05:
		return "connection refused"
	case 0x06:
		return "ttl expired"
	case 0x07:
		return "command not supported"
	case 0x08:
		return "address type not supported"
	default:
		return "unknown refusal"
	}
}
