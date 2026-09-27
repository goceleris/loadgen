package loadgen

// When the HTTP/1.1 client reuses a keep-alive connection, and what a write
// error does (loadgen#87, PR #90 review round 3).
//
//   - A fault in the middle of a response leaves the stream at an unknown
//     position, so the connection must be closed: reusing it makes the next
//     request read the rest of the broken response as its own and fail too,
//     so one fault counts twice.
//   - A complete 4xx/5xx leaves the stream at the next response, so the
//     connection must carry the next request.
//   - A write error on a reused connection is retried once on a fresh one;
//     on a connection the request itself dialed it is not.
//
// This file uses only helpers that main also has (startH1Server,
// testH1Cfg), so it can run against main's h1client.go for the
// failing-first log.

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
)

// scriptedReply is what scriptedH1Server does on a connection.
type scriptedReply struct {
	resp  string // written after each request is read
	close bool   // close (FIN) after resp instead of waiting for the next request
	reset bool   // reset (RST) the connection once the first byte of a request arrives
}

// scriptedH1Server answers request req (1-based) on accepted connection conn
// (1-based) as script says. It reads each request's body (Content-Length),
// so a large request body cannot reset the connection.
type scriptedH1Server struct {
	host, port string
	stop       func()
	accepted   atomic.Int64
	handled    atomic.Int64
}

func startScriptedH1Server(t *testing.T, script func(conn, req int) scriptedReply) *scriptedH1Server {
	t.Helper()
	s := &scriptedH1Server{}
	s.host, s.port, s.stop = startH1Server(t, func(c net.Conn) {
		defer func() { _ = c.Close() }()
		id := int(s.accepted.Add(1))
		if script(id, 1).reset {
			// Wait for the request's first byte: the client's dial has
			// then returned, and it is still writing. A reset during
			// the handshake would instead fail the dial with
			// ECONNRESET, which dialTCPRetry retries on a new
			// connection.
			var b [1]byte
			_, _ = io.ReadFull(c, b[:])
			if tc, ok := c.(*net.TCPConn); ok {
				_ = tc.SetLinger(0)
			}
			return
		}
		r := bufio.NewReader(c)
		for req := 1; ; req++ {
			if err := readScriptedRequest(r); err != nil {
				return
			}
			s.handled.Add(1)
			reply := script(id, req)
			if _, err := c.Write([]byte(reply.resp)); err != nil || reply.close {
				return
			}
		}
	})
	t.Cleanup(s.stop)
	return s
}

// readScriptedRequest reads one request, its body included.
func readScriptedRequest(r *bufio.Reader) error {
	n := 0
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return err
		}
		if line == "\r\n" || line == "\n" {
			break
		}
		if name, v, ok := strings.Cut(line, ":"); ok && strings.EqualFold(name, "Content-Length") {
			n, _ = strconv.Atoi(strings.TrimSpace(v))
		}
	}
	_, err := io.CopyN(io.Discard, r, int64(n))
	return err
}

const scriptedOK = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK"

// TestH1FaultDropsConnection: a keep-alive server answers the first request
// with a response the client cannot finish (a short status line, headers cut
// off by a close, a body over MaxResponseSize), then answers normally. The
// first request fails, once. Its connection must be closed: the next request
// goes out on a fresh one and succeeds. Reusing it would make the next
// request read the rest of the broken response as its own. On main the
// short status line and both MaxResponseSize cases left the connection in
// the slot, and for a Content-Length body drainH1Response read the body as
// header lines. MaxResponseSize bounds a 4xx/5xx body too: the client does
// not read an error body over the limit (a huge or endless one would hold
// the worker), so that connection is closed as well.
func TestH1FaultDropsConnection(t *testing.T) {
	over := "\r\n" + strings.Repeat("x", 98) // 100 bytes; starts with an empty "line"
	for _, tc := range []struct {
		name    string
		first   scriptedReply
		maxResp int64
		wantErr string
	}{
		{"short-status-line", scriptedReply{resp: "HTTP/1.1\r\nContent-Length: 2\r\n\r\nOK"}, 0, "short status line"},
		{"headers-cut-by-close", scriptedReply{resp: "HTTP/1.1 200 OK\r\nContent-Le", close: true}, 0, "read header"},
		{"MaxResponseSize/content-length", scriptedReply{resp: "HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n" + over}, 16, "exceeds MaxResponseSize"},
		{"MaxResponseSize/chunked", scriptedReply{resp: "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n64\r\n" +
			strings.Repeat("x", 100) + "\r\n0\r\n\r\n"}, 16, "exceeds MaxResponseSize"},
		{"MaxResponseSize/error-content-length", scriptedReply{resp: "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 100\r\n\r\n" + over}, 16, "status 500"},
		{"MaxResponseSize/error-chunked", scriptedReply{resp: "HTTP/1.1 503 Service Unavailable\r\nTransfer-Encoding: chunked\r\n\r\n64\r\n" +
			strings.Repeat("x", 100) + "\r\n0\r\n\r\n"}, 16, "status 503"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := startScriptedH1Server(t, func(conn, req int) scriptedReply {
				if conn == 1 && req == 1 {
					return tc.first
				}
				return scriptedReply{resp: scriptedOK}
			})
			cfg := testH1Cfg(true, 1)
			if tc.maxResp > 0 {
				cfg.MaxResponseSize = tc.maxResp
			}
			client, err := newH1Client(srv.host, srv.port, "/", cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			if _, err := client.DoRequest(context.Background(), 0); err == nil {
				t.Fatal("request 1: no error")
			} else if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("request 1: %v, want an error containing %q", err, tc.wantErr)
			} else {
				t.Logf("request 1: %v", err)
			}
			for i := 2; i <= 3; i++ {
				if _, err := client.DoRequest(context.Background(), 0); err != nil {
					t.Errorf("request %d: %v (the broken response's connection was reused: one fault counted twice)", i, err)
				}
			}
			if a := srv.accepted.Load(); a != 2 {
				t.Errorf("server accepted %d connections, want 2: the first response's connection must be closed, not reused", a)
			}
			if h := srv.handled.Load(); h != 3 {
				t.Errorf("server handled %d of 3 requests", h)
			}
		})
	}
}

// TestH1ErrorStatusKeepsConnection: a keep-alive server answers the first
// request with a complete 4xx/5xx and keeps the connection open. The error
// status counts, once; the body is read, so the connection is at the next
// response and carries the next request. Closing it instead would turn an
// error-bearing keep-alive cell into one dial per error. The Content-Length
// 0 case reaches discardH1Body's no-body branch.
func TestH1ErrorStatusKeepsConnection(t *testing.T) {
	for _, tc := range []struct {
		name, resp, wantErr string
	}{
		{"content-length", "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 4\r\n\r\nbusy", "status 500"},
		{"chunked", "HTTP/1.1 503 Service Unavailable\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nbusy\r\n0\r\n\r\n", "status 503"},
		{"empty", "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n", "status 404"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := startScriptedH1Server(t, func(conn, req int) scriptedReply {
				if conn == 1 && req == 1 {
					return scriptedReply{resp: tc.resp}
				}
				return scriptedReply{resp: scriptedOK}
			})
			client, err := newH1Client(srv.host, srv.port, "/", testH1Cfg(true, 1))
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			if _, err := client.DoRequest(context.Background(), 0); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("request 1: %v, want an error containing %q", err, tc.wantErr)
			}
			for i := 2; i <= 3; i++ {
				if _, err := client.DoRequest(context.Background(), 0); err != nil {
					t.Errorf("request %d: %v (the error body was not read to its end)", i, err)
				}
			}
			if a := srv.accepted.Load(); a != 1 {
				t.Errorf("server accepted %d connections for 3 keep-alive requests, want 1 (a complete error response must not end the connection)", a)
			}
			if h := srv.handled.Load(); h != 3 {
				t.Errorf("server handled %d of 3 requests", h)
			}
		})
	}
}

// TestH1WriteError covers the write-error paths of DoRequest.
//
// A write fails deterministically in two ways here: the test shuts down the
// write side of the client's own socket (EPIPE on the next write), or the
// server resets a connection once the request's first byte arrives, while
// the client is still writing a 16 MB body, more than the socket buffers of
// either side hold. The client keeps testH1Cfg's socket buffers: a 4 KB send
// buffer made each 16 MB write take about 10 s on Linux.
func TestH1WriteError(t *testing.T) {
	bigBody := bytes.Repeat([]byte("x"), 16<<20)

	// shutWrite shuts down the write side of the slot's connection, so the
	// next write into it fails: a stale keep-alive connection.
	shutWrite := func(t *testing.T, client *h1Client) {
		t.Helper()
		tc, ok := client.conns[0].conn.(*net.TCPConn)
		if !ok {
			t.Fatalf("slot connection is %T, want *net.TCPConn", client.conns[0].conn)
		}
		if err := tc.CloseWrite(); err != nil {
			t.Fatal(err)
		}
	}

	// A reused keep-alive connection that fails the write is retried once,
	// on a fresh connection: the server never saw the request.
	t.Run("reused/retried-once", func(t *testing.T) {
		srv := startScriptedH1Server(t, func(_, _ int) scriptedReply { return scriptedReply{resp: scriptedOK} })
		client, err := newH1Client(srv.host, srv.port, "/", testH1Cfg(true, 1))
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		if _, err := client.DoRequest(context.Background(), 0); err != nil {
			t.Fatalf("request 1: %v", err)
		}
		shutWrite(t, client)
		if _, err := client.DoRequest(context.Background(), 0); err != nil {
			t.Errorf("request 2: %v (a write error on a reused connection is retried once on a fresh one)", err)
		}
		if a, h := srv.accepted.Load(), srv.handled.Load(); a != 2 || h != 2 {
			t.Errorf("server accepted %d connections and handled %d requests, want 2 and 2", a, h)
		}
	})

	// The retry's dial is refused: the request fails as a connect error.
	t.Run("reused/redial-refused", func(t *testing.T) {
		srv := startScriptedH1Server(t, func(_, _ int) scriptedReply { return scriptedReply{resp: scriptedOK} })
		client, err := newH1Client(srv.host, srv.port, "/", testH1Cfg(true, 1))
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		if _, err := client.DoRequest(context.Background(), 0); err != nil {
			t.Fatalf("request 1: %v", err)
		}
		before := connectErrorsCounter.Swap(0)
		defer connectErrorsCounter.Add(before)
		srv.stop() // the listener is closed: the port refuses
		shutWrite(t, client)
		_, err = client.DoRequest(context.Background(), 0)
		if !errors.Is(err, syscall.ECONNREFUSED) {
			t.Errorf("request 2: %v, want an error wrapping ECONNREFUSED", err)
		}
		if got := connectErrorsCounter.Load(); got != 1 {
			t.Errorf("connect errors = %d, want 1", got)
		}
	})

	// The retry's write fails too: the request fails, and the connection
	// leaves the slot, so the next request dials a fresh one.
	t.Run("reused/retry-write-fails", func(t *testing.T) {
		srv := startScriptedH1Server(t, func(conn, _ int) scriptedReply {
			return scriptedReply{resp: scriptedOK, reset: conn == 2}
		})
		cfg := testH1Cfg(true, 1)
		cfg.Method = "POST"
		cfg.Body = bigBody
		client, err := newH1Client(srv.host, srv.port, "/", cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		if _, err := client.DoRequest(context.Background(), 0); err != nil {
			t.Fatalf("request 1: %v", err)
		}
		shutWrite(t, client)
		_, err = client.DoRequest(context.Background(), 0)
		if err == nil || !strings.Contains(err.Error(), "write after reconnect") {
			t.Fatalf("request 2: %v, want a write-after-reconnect error (connection 2 is reset mid-write)", err)
		}
		t.Logf("request 2: %v", err)
		if c := client.conns[0].conn; c != nil {
			t.Errorf("after a failed write the slot still holds a connection (%v): it must leave the slot", c.LocalAddr())
		}
		if _, err := client.DoRequest(context.Background(), 0); err != nil {
			t.Errorf("request 3: %v", err)
		}
		if a, h := srv.accepted.Load(), srv.handled.Load(); a != 3 || h != 2 {
			t.Errorf("server accepted %d connections and handled %d requests, want 3 and 2", a, h)
		}
	})

	// A connection this request dialed that fails the write is not
	// retried: the server hung up on a fresh connection, and a retry could
	// loop on a server that rejects the request that way.
	t.Run("fresh/not-retried", func(t *testing.T) {
		srv := startScriptedH1Server(t, func(conn, _ int) scriptedReply {
			return scriptedReply{
				resp:  "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 2\r\n\r\nOK",
				close: true,
				reset: conn == 2,
			}
		})
		cfg := testH1Cfg(false, 1)
		cfg.PoolSize = 1
		cfg.Method = "POST"
		cfg.Body = bigBody
		client, err := newH1Client(srv.host, srv.port, "/", cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		if _, err := client.DoRequest(context.Background(), 0); err != nil {
			t.Fatalf("request 1: %v", err)
		}
		_, err = client.DoRequest(context.Background(), 0)
		if err == nil {
			t.Fatal("request 2 succeeded: the write error on the connection it dialed (reset mid-write) was retried")
		}
		if !strings.Contains(err.Error(), "write") || strings.Contains(err.Error(), "after reconnect") {
			t.Errorf("request 2: %v, want the first write's error, not retried", err)
		}
		t.Logf("request 2: %v", err)
		if a := srv.accepted.Load(); a != 2 {
			t.Errorf("server accepted %d connections after request 2, want 2 (no retry dial)", a)
		}
		if _, err := client.DoRequest(context.Background(), 0); err != nil {
			t.Errorf("request 3: %v", err)
		}
	})
}
