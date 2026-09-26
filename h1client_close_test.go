package loadgen

// Connection-lifetime tests for the HTTP/1.1 client (loadgen#87).
//
// A connection the server has closed, or announced it will close, must
// never carry another request: the request would be written into a dead
// socket and the EOF that follows would be counted as a failed request,
// although the server answered every request that reached it. Genuine
// failures (a refused dial, a reset or a truncated body) must still be
// counted, exactly once each.

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// h1Reply is what rawH1Server does with one request.
type h1Reply int

const (
	// replyKeepOpen answers 200 "OK" and keeps the connection open for the
	// next request, whatever the request's Connection header said.
	replyKeepOpen h1Reply = iota
	// replyAnnounceClose answers 200 "OK" with Connection: close, then
	// closes the connection (FIN): what a server that honours a
	// Connection: close request does.
	replyAnnounceClose
	// replyCloseSilently answers 200 "OK" without a Connection header,
	// then closes the connection (FIN): a server that honours a
	// Connection: close request without saying so.
	replyCloseSilently
	// replyAnnounceCloseLate answers with Connection: close, then waits
	// up to lateCloseDelay for the client to close first (recorded in
	// clientClosedFirst / serverClosedFirst), then closes.
	replyAnnounceCloseLate
	// replyCloseSilentlyLate is replyAnnounceCloseLate without the
	// Connection header.
	replyCloseSilentlyLate
	// replyAnnounceKeepOpen answers with Connection: close and keeps the
	// connection open anyway.
	replyAnnounceKeepOpen
	// replyTruncate announces a 100-byte body, sends 10 bytes, then closes
	// the connection (FIN).
	replyTruncate
	// replyReset announces a 100-byte body, sends 10 bytes, then resets
	// the connection (RST).
	replyReset
	// replyHalfClose answers 200 "OK" without a Connection header, then
	// half-closes (FIN) and keeps reading until the client closes: a
	// keep-alive connection the server ends without notice, whose socket
	// stays open, so a request written into it does not provoke an RST.
	replyHalfClose
	// replyErrorAnnounceClose answers 503 with Connection: close, then
	// closes the connection.
	replyErrorAnnounceClose
	// replyErrorTruncate answers 500, announcing a 100-byte body, sends
	// 10 bytes, then closes the connection (FIN).
	replyErrorTruncate
)

// lateCloseDelay is how long replyAnnounceCloseLate and
// replyCloseSilentlyLate hold their close back. It must stay well below
// the client's bound on waiting for the server's FIN, which the tests that
// use it widen to lateCloseClientWait.
const lateCloseDelay = 10 * time.Millisecond

// lateCloseClientWait replaces the client's 50ms bound in the tests that
// assert the server closes first, so a server goroutine delayed on a
// loaded CI runner cannot make the client close first.
const lateCloseClientWait = 2 * time.Second

// fastRequest: a request to a local server that takes less than this did
// not wait for the connection's close. A request that waited would take at
// least the client's 50ms bound on that wait.
const fastRequest = 25 * time.Millisecond

const (
	respOK            = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK"
	respAnnounceClose = "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 2\r\n\r\nOK"
	respTruncated     = "HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n0123456789"
	resp503Close      = "HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 4\r\n\r\nbusy"
	resp500Truncated  = "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 100\r\n\r\n0123456789"
)

// rawH1Server is a raw HTTP/1.1 server that counts the connections it
// accepts, the requests it reads on each, and the connections the CLIENT
// closed (EOF while the server waited for a next request).
type rawH1Server struct {
	host, port string
	stop       func() // closes the listener; established conns stay up

	accepted     atomic.Int64
	handled      atomic.Int64
	clientClosed atomic.Int64

	// replyAnnounceCloseLate: which side closed the connection first.
	clientClosedFirst atomic.Int64
	serverClosedFirst atomic.Int64

	mu      sync.Mutex
	perConn map[int]int // connection ordinal (1-based) -> requests read on it
}

// startRawH1Server serves every connection with reply(connOrdinal,
// requestOrdinalOnConn), both 1-based.
func startRawH1Server(t *testing.T, reply func(conn, req int) h1Reply) *rawH1Server {
	t.Helper()
	s := &rawH1Server{perConn: make(map[int]int)}
	s.host, s.port, s.stop = startH1Server(t, func(c net.Conn) {
		defer func() { _ = c.Close() }()
		id := int(s.accepted.Add(1))
		r := bufio.NewReader(c)
		for req := 1; ; req++ {
			if !readH1Request(r) {
				s.clientClosed.Add(1)
				return
			}
			s.handled.Add(1)
			s.mu.Lock()
			s.perConn[id]++
			s.mu.Unlock()
			switch reply(id, req) {
			case replyKeepOpen:
				if _, err := c.Write([]byte(respOK)); err != nil {
					return
				}
			case replyAnnounceClose:
				_, _ = c.Write([]byte(respAnnounceClose))
				return
			case replyCloseSilently:
				_, _ = c.Write([]byte(respOK))
				return
			case replyAnnounceCloseLate, replyCloseSilentlyLate:
				resp := respAnnounceClose
				if reply(id, req) == replyCloseSilentlyLate {
					resp = respOK
				}
				if _, err := c.Write([]byte(resp)); err != nil {
					return
				}
				_ = c.SetReadDeadline(time.Now().Add(lateCloseDelay))
				if _, err := r.ReadByte(); errors.Is(err, os.ErrDeadlineExceeded) {
					s.serverClosedFirst.Add(1)
				} else {
					s.clientClosedFirst.Add(1)
				}
				return
			case replyAnnounceKeepOpen:
				if _, err := c.Write([]byte(respAnnounceClose)); err != nil {
					return
				}
			case replyTruncate:
				_, _ = c.Write([]byte(respTruncated))
				return
			case replyReset:
				_, _ = c.Write([]byte(respTruncated))
				if tc, ok := c.(*net.TCPConn); ok {
					_ = tc.SetLinger(0) // Close sends RST, not FIN
				}
				return
			case replyHalfClose:
				if _, err := c.Write([]byte(respOK)); err != nil {
					return
				}
				if tc, ok := c.(*net.TCPConn); ok {
					_ = tc.CloseWrite()
				}
				_, _ = io.Copy(io.Discard, r)
				s.clientClosed.Add(1)
				return
			case replyErrorAnnounceClose:
				_, _ = c.Write([]byte(resp503Close))
				return
			case replyErrorTruncate:
				_, _ = c.Write([]byte(resp500Truncated))
				return
			}
		}
	})
	t.Cleanup(s.stop)
	return s
}

func (s *rawH1Server) requestsPerConn() map[int]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[int]int, len(s.perConn))
	for k, v := range s.perConn {
		out[k] = v
	}
	return out
}

// TestH1CloseModeServerCloseIsNotAnError is the loadgen#87 reproduction at
// the client level: in close mode the server closes every connection after
// its response, as asked. Every request must succeed and each must travel
// on its own connection. Before the fix the slot's closed connection was
// reused: the Write succeeded (the kernel accepts it; the peer answers with
// an RST only later), the status read returned EOF, and that EOF was
// returned as a failed request, one per successful one.
func TestH1CloseModeServerCloseIsNotAnError(t *testing.T) {
	for _, tc := range []struct {
		name  string
		reply h1Reply
		pool  int
	}{
		{"announced/PoolSize=1", replyAnnounceClose, 1},
		{"announced/PoolSize=16", replyAnnounceClose, 16},
		{"silent/PoolSize=1", replyCloseSilently, 1},
		{"silent/PoolSize=16", replyCloseSilently, 16},
	} {
		pool := tc.pool
		t.Run(tc.name, func(t *testing.T) {
			srv := startRawH1Server(t, func(_, _ int) h1Reply { return tc.reply })

			cfg := testH1Cfg(false, 1)
			cfg.PoolSize = pool
			client, err := newH1Client(srv.host, srv.port, "/", cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			// 3 full cycles of the worker's pool plus one: every slot is
			// reused at least twice.
			n := 3*pool + 1
			failed := 0
			for i := range n {
				got, err := client.DoRequest(context.Background(), 0)
				if err != nil {
					failed++
					t.Errorf("request %d of %d: %v (the expected close of the previous connection counted as a failed request)", i+1, n, err)
					continue
				}
				if got != 2 {
					t.Errorf("request %d: bytesRead=%d, want 2", i+1, got)
				}
			}
			if h := srv.handled.Load(); h != int64(n) {
				t.Errorf("server handled %d requests, want %d", h, n)
			}
			if a := srv.accepted.Load(); a != int64(n) {
				t.Errorf("server accepted %d connections for %d requests, want one connection per request", a, n)
			}
			t.Logf("%s: %d requests, %d failed, server handled %d on %d connections",
				tc.name, n, failed, srv.handled.Load(), srv.accepted.Load())
		})
	}
}

// TestH1CloseModeServerIgnoringCloseGetsOneConnPerRequest: a server that
// ignores Connection: close keeps the connection open. The client asked
// for close, so it must not send another request on that connection
// (RFC 9112 §9.6): it closes it itself and dials a fresh one. Before the
// fix the client reused the connection, so churn-close measured plain
// keep-alive against such a server.
func TestH1CloseModeServerIgnoringCloseGetsOneConnPerRequest(t *testing.T) {
	srv := startRawH1Server(t, func(_, _ int) h1Reply { return replyKeepOpen })

	cfg := testH1Cfg(false, 1)
	cfg.PoolSize = 1
	client, err := newH1Client(srv.host, srv.port, "/", cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const n = 5
	for i := range n {
		got, err := client.DoRequest(context.Background(), 0)
		if err != nil {
			t.Fatalf("request %d: %v", i+1, err)
		}
		if got != 2 {
			t.Fatalf("request %d: bytesRead=%d, want 2", i+1, got)
		}
	}
	if a := srv.accepted.Load(); a != n {
		t.Errorf("server accepted %d connections for %d close-mode requests, want %d (requests per connection: %v)",
			a, n, n, srv.requestsPerConn())
	}
	for id, reqs := range srv.requestsPerConn() {
		if reqs != 1 {
			t.Errorf("connection %d carried %d requests, want 1", id, reqs)
		}
	}
	// Every connection that carried its request was closed by the client.
	deadline := time.Now().Add(2 * time.Second)
	for srv.clientClosed.Load() < n && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if c := srv.clientClosed.Load(); c < n {
		t.Errorf("client closed %d of %d connections after their response, want all", c, n)
	}
}

// TestH1KeepAliveAnnouncedCloseIsNotAnError: in keep-alive mode a server
// may end a connection by answering with Connection: close (a
// max-requests-per-connection limit, for example). The next request must
// go out on a fresh connection instead of failing on the closed one.
func TestH1KeepAliveAnnouncedCloseIsNotAnError(t *testing.T) {
	// Every connection carries two requests; the second is answered with
	// Connection: close.
	srv := startRawH1Server(t, func(_, req int) h1Reply {
		if req == 2 {
			return replyAnnounceClose
		}
		return replyKeepOpen
	})

	client, err := newH1Client(srv.host, srv.port, "/", testH1Cfg(true, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const n = 6
	for i := range n {
		if _, err := client.DoRequest(context.Background(), 0); err != nil {
			t.Errorf("request %d of %d: %v (the server announced the close; it is not a failed request)", i+1, n, err)
		}
	}
	if h := srv.handled.Load(); h != n {
		t.Errorf("server handled %d requests, want %d", h, n)
	}
	if a := srv.accepted.Load(); a != n/2 {
		t.Errorf("server accepted %d connections, want %d (two requests each)", a, n/2)
	}
}

// TestH1ServerClosesFirst: once a connection is done (close mode, or a
// response that announced Connection: close), the client lets the server
// close first: it waits for the server's FIN before closing, so the server
// holds the TIME_WAIT (at churn rates client-side TIME_WAIT exhausts the
// ephemeral ports of a host that does not reuse them). The wait is off the
// request's measured latency, and bounded: a server that keeps the
// connection open is closed by the client after that bound (50ms).
func TestH1ServerClosesFirst(t *testing.T) {
	for _, tc := range []struct {
		name      string
		keepAlive bool
		reply     h1Reply
	}{
		{"close/announced", false, replyAnnounceCloseLate},
		{"close/silent", false, replyCloseSilentlyLate},
		{"keep-alive/announced", true, replyAnnounceCloseLate},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := startRawH1Server(t, func(_, _ int) h1Reply { return tc.reply })
			cfg := testH1Cfg(tc.keepAlive, 1)
			cfg.PoolSize = 1
			client, err := newH1Client(srv.host, srv.port, "/", cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			for _, hc := range client.conns {
				hc.peerCloseWait = lateCloseClientWait
			}

			const n = 4
			for i := range n {
				if _, err := client.DoRequest(context.Background(), 0); err != nil {
					t.Fatalf("request %d: %v", i+1, err)
				}
			}
			deadline := time.Now().Add(2 * time.Second)
			for srv.clientClosedFirst.Load()+srv.serverClosedFirst.Load() < n && time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
			}
			if cf, sf := srv.clientClosedFirst.Load(), srv.serverClosedFirst.Load(); sf != n {
				t.Errorf("server closed first on %d of %d connections, client first on %d: the client did not wait for the server's close", sf, n, cf)
			}
		})
	}

	t.Run("announced-but-kept-open", func(t *testing.T) {
		srv := startRawH1Server(t, func(_, _ int) h1Reply { return replyAnnounceKeepOpen })
		client, err := newH1Client(srv.host, srv.port, "/", testH1Cfg(true, 1))
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()

		const n = 3
		fast := 0
		for i := range n {
			done := make(chan error, 1)
			start := time.Now()
			go func() {
				_, err := client.DoRequest(context.Background(), 0)
				done <- err
			}()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("request %d: %v", i+1, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("request %d: still waiting after 5s for a close the server announced and never made", i+1)
			}
			elapsed := time.Since(start)
			if elapsed < fastRequest {
				fast++
			}
			t.Logf("request %d took %v", i+1, elapsed)
		}
		if fast == 0 {
			t.Errorf("none of %d requests took less than %v: the wait for the server's close is inside the measured request", n, fastRequest)
		}
		if a := srv.accepted.Load(); a != n {
			t.Errorf("server accepted %d connections for %d requests, want %d", a, n, n)
		}
		// The client closes each connection itself once its bound passes.
		deadline := time.Now().Add(2 * time.Second)
		for srv.clientClosed.Load() < n && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
		if c := srv.clientClosed.Load(); c != n {
			t.Errorf("client closed %d of %d connections the server kept open, want all (the wait for the server's close is unbounded)", c, n)
		}
	})
}

// TestH1TruncatedBodyCountsOnce: a truncated body is a genuine failure and
// must be counted, but once. The connection it leaves behind is dead;
// before the fix the next request was written into it and failed again
// with EOF, so one server fault was counted twice and the request after it
// never reached the server.
func TestH1TruncatedBodyCountsOnce(t *testing.T) {
	for _, tc := range []struct {
		name      string
		keepAlive bool
		first     h1Reply
	}{
		{"keep-alive/200", true, replyTruncate},
		{"close/200", false, replyTruncate},
		{"keep-alive/500", true, replyErrorTruncate},
		{"close/500", false, replyErrorTruncate},
	} {
		keepAlive := tc.keepAlive
		t.Run(tc.name, func(t *testing.T) {
			srv := startRawH1Server(t, func(conn, req int) h1Reply {
				switch {
				case conn == 1 && req == 1:
					return tc.first
				case keepAlive:
					return replyKeepOpen
				default:
					return replyAnnounceClose
				}
			})

			cfg := testH1Cfg(keepAlive, 1)
			cfg.PoolSize = 1
			client, err := newH1Client(srv.host, srv.port, "/", cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			if _, err := client.DoRequest(context.Background(), 0); err == nil {
				t.Fatal("request 1: truncated body (Content-Length 100, 10 bytes sent) returned no error")
			} else {
				t.Logf("request 1 (truncated): %v", err)
			}
			for i := 2; i <= 3; i++ {
				if _, err := client.DoRequest(context.Background(), 0); err != nil {
					t.Errorf("request %d: %v (the truncated response's dead connection was reused)", i, err)
				}
			}
			if h := srv.handled.Load(); h != 3 {
				t.Errorf("server handled %d of 3 requests", h)
			}
		})
	}
}

// TestH1ResetMidResponseCounts: a connection reset in the middle of a
// response is a genuine failure and must be counted; the next request must
// then succeed on a fresh connection.
func TestH1ResetMidResponseCounts(t *testing.T) {
	for _, keepAlive := range []bool{true, false} {
		name := "close"
		if keepAlive {
			name = "keep-alive"
		}
		t.Run(name, func(t *testing.T) {
			srv := startRawH1Server(t, func(conn, req int) h1Reply {
				switch {
				case conn == 1 && req == 1:
					return replyReset
				case keepAlive:
					return replyKeepOpen
				default:
					return replyAnnounceClose
				}
			})

			cfg := testH1Cfg(keepAlive, 1)
			cfg.PoolSize = 1
			client, err := newH1Client(srv.host, srv.port, "/", cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			if _, err := client.DoRequest(context.Background(), 0); err == nil {
				t.Fatal("request 1: reset mid-response returned no error")
			} else {
				t.Logf("request 1 (reset): %v", err)
			}
			if _, err := client.DoRequest(context.Background(), 0); err != nil {
				t.Errorf("request 2: %v", err)
			}
			if h := srv.handled.Load(); h != 2 {
				t.Errorf("server handled %d of 2 requests", h)
			}
		})
	}
}

// TestH1ErrorStatusWithCloseCountsOnce: a keep-alive server answers 503
// with Connection: close and closes. The 503 is counted, once; the next
// request goes out on a fresh connection instead of failing again on the
// closed one.
func TestH1ErrorStatusWithCloseCountsOnce(t *testing.T) {
	srv := startRawH1Server(t, func(conn, req int) h1Reply {
		if conn == 1 && req == 1 {
			return replyErrorAnnounceClose
		}
		return replyKeepOpen
	})
	client, err := newH1Client(srv.host, srv.port, "/", testH1Cfg(true, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.DoRequest(context.Background(), 0); err == nil {
		t.Fatal("request 1: a 503 returned no error")
	} else {
		t.Logf("request 1 (503): %v", err)
	}
	for i := 2; i <= 3; i++ {
		if _, err := client.DoRequest(context.Background(), 0); err != nil {
			t.Errorf("request %d: %v (the 503 announced the close; its connection was reused)", i, err)
		}
	}
	if h := srv.handled.Load(); h != 3 {
		t.Errorf("server handled %d of 3 requests", h)
	}
}

// TestH1UnannouncedCloseCountsOnce: a keep-alive server ends a connection
// without Connection: close (a FIN after a response, socket still open).
// The request that meets the closed connection fails, once; the one after
// it must go out on a fresh connection, not into the same dead socket.
func TestH1UnannouncedCloseCountsOnce(t *testing.T) {
	srv := startRawH1Server(t, func(conn, req int) h1Reply {
		if conn == 1 && req == 1 {
			return replyHalfClose
		}
		return replyKeepOpen
	})
	client, err := newH1Client(srv.host, srv.port, "/", testH1Cfg(true, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.DoRequest(context.Background(), 0); err != nil {
		t.Fatalf("request 1: %v", err)
	}
	if _, err := client.DoRequest(context.Background(), 0); err == nil {
		t.Fatal("request 2: written into a connection the server had closed, returned no error")
	} else {
		t.Logf("request 2 (unannounced close): %v", err)
	}
	for i := 3; i <= 4; i++ {
		if _, err := client.DoRequest(context.Background(), 0); err != nil {
			t.Errorf("request %d: %v (the dead connection was reused)", i, err)
		}
	}
	if a := srv.accepted.Load(); a != 2 {
		t.Errorf("server accepted %d connections, want 2", a)
	}
}

// TestH1CloseModeRefusedDialCounts: once the server is gone, the fresh
// connection a close-mode request needs cannot be dialed. That is a
// genuine failure: the request must fail and the refused dial must land in
// the connect-error class.
func TestH1CloseModeRefusedDialCounts(t *testing.T) {
	srv := startRawH1Server(t, func(_, _ int) h1Reply { return replyAnnounceClose })

	cfg := testH1Cfg(false, 1)
	cfg.PoolSize = 1
	client, err := newH1Client(srv.host, srv.port, "/", cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.DoRequest(context.Background(), 0); err != nil {
		t.Fatalf("priming request: %v", err)
	}

	before := connectErrorsCounter.Swap(0)
	defer connectErrorsCounter.Add(before)

	srv.stop() // listener closed: the port now refuses

	for i := 2; i <= 3; i++ {
		_, err := client.DoRequest(context.Background(), 0)
		if err == nil {
			t.Fatalf("request %d: no error against a refusing port", i)
		}
		if !errors.Is(err, syscall.ECONNREFUSED) {
			t.Errorf("request %d: error %v does not wrap ECONNREFUSED", i, err)
		}
		if got := connectErrorsCounter.Load(); got != uint64(i-1) {
			t.Errorf("after request %d: connect errors = %d, want %d (one per refused dial)", i, got, i-1)
		}
	}
}

// TestH1DialAfterCloseIsNotInstalled: Benchmarker.Run calls Close while a
// worker may still be in DoRequest. A request that needs a fresh
// connection after Close must fail instead of installing, and leaking, a
// connection nobody will close.
func TestH1DialAfterCloseIsNotInstalled(t *testing.T) {
	srv := startRawH1Server(t, func(_, _ int) h1Reply { return replyAnnounceClose })

	cfg := testH1Cfg(false, 1)
	cfg.PoolSize = 1
	client, err := newH1Client(srv.host, srv.port, "/", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.DoRequest(context.Background(), 0); err != nil {
		t.Fatalf("priming request: %v", err)
	}
	client.Close()

	if _, err := client.DoRequest(context.Background(), 0); err == nil {
		t.Error("a request after Close succeeded on a connection it dialed, which Close will never close")
	}
	if h := srv.handled.Load(); h != 1 {
		t.Errorf("server handled %d requests, want 1 (none after Close)", h)
	}
}

// TestBenchmarkerCloseModeCountsNoErrors is loadgen#87 end to end, the
// way probatorium's churn-close runs it (DisableKeepAlive, PoolSize=1) and
// with the default PoolSize. A net/http server honours Connection: close;
// it answers every request that reaches it. Before the fix Errors was
// about equal to Requests (errors/(errors+requests) = 0.5).
func TestBenchmarkerCloseModeCountsNoErrors(t *testing.T) {
	for _, pool := range []int{1, 16} {
		t.Run(fmt.Sprintf("PoolSize=%d", pool), func(t *testing.T) {
			var handled atomic.Int64
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				handled.Add(1)
				_, _ = w.Write([]byte("OK"))
			}))
			defer srv.Close()

			const workers = 2
			b, err := New(Config{
				URL:              srv.URL + "/",
				Method:           "GET",
				Duration:         300 * time.Millisecond,
				Connections:      workers,
				Workers:          workers,
				DisableKeepAlive: true,
				PoolSize:         pool,
				// Paced, so the test opens a few hundred connections, not
				// tens of thousands of TIME_WAIT entries on the test host.
				MaxRPS: 1000,
			})
			if err != nil {
				t.Fatal(err)
			}
			res, err := b.Run(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			h := handled.Load()
			t.Logf("PoolSize=%d: requests=%d errors=%d connect_errors=%d server_handled=%d",
				pool, res.Requests, res.Errors, res.ConnectErrors, h)
			if res.Requests == 0 {
				t.Fatal("no successful requests")
			}
			if res.Errors != 0 {
				t.Errorf("errors=%d against a server that answered every request (requests=%d, server handled %d)",
					res.Errors, res.Requests, h)
			}
			if res.ConnectErrors != 0 {
				t.Errorf("connect_errors=%d, want 0", res.ConnectErrors)
			}
			// The server may have handled the few requests in flight when
			// the run ended, which the client then dropped.
			if d := h - res.Requests; d < 0 || d > workers {
				t.Errorf("server handled %d, loadgen counted %d successes: difference %d is outside [0, %d]",
					h, res.Requests, d, workers)
			}
		})
	}
}
