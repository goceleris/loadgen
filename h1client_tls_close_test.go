package loadgen

// The close of a done connection over TLS (loadgen#87, PR #90 review round
// 3). Over TLS, the client's Read returns io.EOF when the server's
// close_notify alert arrives, whether or not its TCP FIN has. A client that
// takes that EOF for the FIN closes first: it keeps the TIME_WAIT, or holds
// its port in FIN_WAIT_2 until the server closes. So the client must wait
// for the TCP FIN itself, and reset a connection whose FIN does not come,
// as it does over plain TCP.

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// tlsTeardown is how tlsCloseServer ends a connection after its response.
type tlsTeardown int

const (
	// tlsNotifyThenFIN sends close_notify, waits up to lateCloseDelay for
	// the client to send or close anything, then closes TCP. Which side
	// acted first is recorded in clientClosedFirst / serverClosedFirst.
	tlsNotifyThenFIN tlsTeardown = iota
	// tlsNotifyAndFIN sends close_notify and a TCP FIN (half-close) at
	// once, then reads until the client closes.
	tlsNotifyAndFIN
	// tlsNotifyKeepTCP sends close_notify and keeps TCP open, reading until
	// the client closes: a bidirectional shutdown (SSL_shutdown waiting for
	// the peer's close_notify) never closes first.
	tlsNotifyKeepTCP
	// tlsKeepOpen sends nothing after the response and keeps the
	// connection open, answering any further request on it, until the
	// client closes: a server that ignores Connection: close.
	tlsKeepOpen
)

// tlsCloseServer is a TLS HTTP/1.1 server that records how the client ended
// each connection. It ends a connection after one response, except in
// tlsKeepOpen, where it answers every request (so a client that wrongly
// reuses the connection gets a response and fails the test on the
// connection count, instead of waiting for a reply that never comes).
type tlsCloseServer struct {
	host, port        string
	accepted          atomic.Int64
	handled           atomic.Int64
	clientClosed      atomic.Int64 // the client ended the connection (FIN or RST) while the server read
	clientReset       atomic.Int64 // ... with an RST
	clientClosedFirst atomic.Int64 // tlsNotifyThenFIN: the client sent or closed before the server's FIN
	serverClosedFirst atomic.Int64 // tlsNotifyThenFIN: the server's FIN went first
}

// testTLSConfig is a server config with a fresh self-signed certificate for
// 127.0.0.1.
func testTLSConfig(t *testing.T) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "loadgen-test"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
}

func startTLSCloseServer(t *testing.T, mode tlsTeardown, resp string) *tlsCloseServer {
	t.Helper()
	s := &tlsCloseServer{}
	cfg := testTLSConfig(t)
	var stop func()
	s.host, s.port, stop = startH1Server(t, func(raw net.Conn) {
		defer func() { _ = raw.Close() }()
		s.accepted.Add(1)
		tc := tls.Server(raw, cfg)
		if err := tc.Handshake(); err != nil {
			return
		}
		r := bufio.NewReader(tc)
		if err := readH1RequestErr(r); err != nil {
			return
		}
		s.handled.Add(1)
		if _, err := tc.Write([]byte(resp)); err != nil {
			return
		}
		if mode == tlsKeepOpen {
			for {
				err := readH1RequestErr(r)
				if err == nil {
					s.handled.Add(1)
					if _, err := tc.Write([]byte(resp)); err != nil {
						return
					}
					continue
				}
				// io.EOF is the client's close_notify (or a FIN at a
				// record boundary); the raw socket then shows whether
				// the TCP close that follows is a FIN or an RST.
				if errors.Is(err, io.EOF) {
					err = drainRaw(raw)
				}
				s.clientGone(err)
				return
			}
		}
		_ = tc.CloseWrite() // close_notify only; the TCP connection stays open
		switch mode {
		case tlsNotifyThenFIN:
			_ = raw.SetReadDeadline(time.Now().Add(lateCloseDelay))
			var b [1]byte
			if _, err := raw.Read(b[:]); errors.Is(err, os.ErrDeadlineExceeded) {
				s.serverClosedFirst.Add(1)
			} else {
				s.clientClosedFirst.Add(1)
			}
		case tlsNotifyAndFIN:
			if tcp, ok := raw.(*net.TCPConn); ok {
				_ = tcp.CloseWrite()
			}
			s.clientGone(drainRaw(raw))
		case tlsNotifyKeepTCP:
			s.clientGone(drainRaw(raw))
		}
	})
	t.Cleanup(stop)
	return s
}

// drainRaw reads the raw TCP connection until the client ends it; it
// returns io.EOF for a FIN and the read error (ECONNRESET) for an RST. The
// client's close_notify, if any, is read and discarded on the way.
func drainRaw(raw net.Conn) error {
	_, err := io.Copy(io.Discard, raw)
	if err == nil {
		return io.EOF
	}
	return err
}

// clientGone records that the client ended a connection; the reset is
// counted before the close, as in rawH1Server.
func (s *tlsCloseServer) clientGone(err error) {
	if errors.Is(err, syscall.ECONNRESET) {
		s.clientReset.Add(1)
	}
	s.clientClosed.Add(1)
}

// tlsCloseClient is an HTTPS h1Client for srv.
func tlsCloseClient(t *testing.T, srv *tlsCloseServer, keepAlive bool) *h1Client {
	t.Helper()
	cfg := testH1Cfg(keepAlive, 1)
	cfg.PoolSize = 1
	cfg.scheme = "https"
	cfg.InsecureSkipVerify = true
	client, err := newH1Client(srv.host, srv.port, "/", cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

// TestH1TLSServerClosesFirst: over TLS, a server that sends close_notify
// and closes TCP a moment later must still be seen closing first. On PR
// #90's round-2 code the client took the close_notify for the FIN and
// closed at once, first.
func TestH1TLSServerClosesFirst(t *testing.T) {
	for _, tc := range []struct {
		name      string
		keepAlive bool
		resp      string
	}{
		{"close", false, respOK},
		{"keep-alive/announced", true, respAnnounceClose},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := startTLSCloseServer(t, tlsNotifyThenFIN, tc.resp)
			client := tlsCloseClient(t, srv, tc.keepAlive)
			for _, hc := range client.conns {
				hc.peerCloseWait = lateCloseClientWait
			}
			const n = 4
			for i := range n {
				if _, err := client.DoRequest(context.Background(), 0); err != nil {
					t.Fatalf("request %d: %v", i+1, err)
				}
			}
			waitFor(func() bool { return srv.clientClosedFirst.Load()+srv.serverClosedFirst.Load() >= n })
			if cf, sf := srv.clientClosedFirst.Load(), srv.serverClosedFirst.Load(); sf != n {
				t.Errorf("server closed first on %d of %d connections, client first on %d: "+
					"the client took the close_notify for the server's FIN", sf, n, cf)
			}
		})
	}
}

// TestH1TLSCloseTeardown: with the shipped 50ms bound, how the client ends
// a done TLS connection, by what the server does after its response.
//   - close_notify and FIN together: the client closes after the FIN, with
//     no reset (the server keeps the TIME_WAIT).
//   - close_notify, TCP kept open (bidirectional shutdown): the client
//     resets once the bound passes. A FIN would put the TIME_WAIT on the
//     loadgen host.
//   - nothing, connection kept open (Connection: close ignored): the client
//     resets once the bound passes, through the TLS layer to the TCP socket.
func TestH1TLSCloseTeardown(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mode      tlsTeardown
		wantReset bool
	}{
		{"close_notify+FIN", tlsNotifyAndFIN, false},
		{"close_notify,TCP-kept-open", tlsNotifyKeepTCP, true},
		{"kept-open", tlsKeepOpen, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := startTLSCloseServer(t, tc.mode, respOK)
			client := tlsCloseClient(t, srv, false)
			const n = 4
			for i := range n {
				if _, err := client.DoRequest(context.Background(), 0); err != nil {
					t.Fatalf("request %d: %v", i+1, err)
				}
			}
			if a := srv.accepted.Load(); a != n {
				t.Errorf("server accepted %d connections for %d close-mode requests, want %d", a, n, n)
			}
			if !waitFor(func() bool { return srv.clientClosed.Load() >= n }) {
				t.Fatalf("client ended %d of %d connections within 2s", srv.clientClosed.Load(), n)
			}
			r := srv.clientReset.Load()
			t.Logf("client ended %d connections, %d of them with a reset", srv.clientClosed.Load(), r)
			if tc.wantReset && r != n {
				t.Errorf("client reset %d of %d connections the server kept open; the rest it closed with a FIN, "+
					"which leaves a TIME_WAIT on the loadgen host", r, n)
			}
			// One reset is tolerated where none is wanted: a server
			// goroutine descheduled for longer than the bound between
			// its close_notify and its FIN on a loaded runner.
			if !tc.wantReset && r > 1 {
				t.Errorf("client reset %d of %d connections whose server sent close_notify and FIN together: "+
					"it did not wait for the TCP FIN", r, n)
			}
		})
	}
}
