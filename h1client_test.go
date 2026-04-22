package loadgen

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// startH1Server starts a TCP listener and handles connections with the given handler.
// Returns host, port, and a cleanup function that shuts down the listener.
func startH1Server(t *testing.T, handler func(conn net.Conn)) (host, port string, cleanup func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go handler(conn)
		}
	}()
	h, p, _ := net.SplitHostPort(ln.Addr().String())
	return h, p, func() { _ = ln.Close(); <-done }
}

// readH1Request reads a full HTTP/1.1 request (request line + headers + optional body).
// Returns false if the connection was closed.
func readH1Request(r *bufio.Reader) bool {
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return false
		}
		if line == "\r\n" || line == "\n" {
			return true
		}
	}
}

// testH1Cfg builds a Config suitable for newH1Client in tests.
func testH1Cfg(keepAlive bool, workers int) Config {
	cfg := Config{
		Method:           "GET",
		DisableKeepAlive: !keepAlive,
		Workers:          workers,
		Connections:      workers,
		DialTimeout:      5 * time.Second,
		ReadBufferSize:   256 * 1024,
		WriteBufferSize:  256 * 1024,
		MaxResponseSize:  10 << 20, // 10MB
		PoolSize:         16,
	}
	return cfg
}

func TestH1KeepAlive(t *testing.T) {
	const numRequests = 5
	connCount := 0
	var mu sync.Mutex

	host, port, cleanup := startH1Server(t, func(conn net.Conn) {
		defer func() { _ = conn.Close() }()
		mu.Lock()
		connCount++
		mu.Unlock()

		reader := bufio.NewReader(conn)
		for {
			if !readH1Request(reader) {
				return
			}
			resp := "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK"
			if _, err := conn.Write([]byte(resp)); err != nil {
				return
			}
		}
	})
	defer cleanup()

	client, err := newH1Client(host, port, "/", testH1Cfg(true, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	for i := range numRequests {
		n, err := client.DoRequest(ctx, 0)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if n != 2 {
			t.Fatalf("request %d: bytesRead=%d, want 2", i, n)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if connCount != 1 {
		t.Errorf("expected 1 connection (keep-alive), got %d", connCount)
	}
}

func TestH1ConnectionClose(t *testing.T) {
	host, port, cleanup := startH1Server(t, func(conn net.Conn) {
		defer func() { _ = conn.Close() }()
		reader := bufio.NewReader(conn)
		if !readH1Request(reader) {
			return
		}
		resp := "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 2\r\n\r\nOK"
		_, _ = conn.Write([]byte(resp))
		// Server closes after one response (Connection: close)
	})
	defer cleanup()

	cfg := testH1Cfg(false, 1)
	client, err := newH1Client(host, port, "/", cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	// Send multiple requests; client should reconnect via round-robin pool
	for i := range 5 {
		n, err := client.DoRequest(ctx, 0)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if n != 2 {
			t.Fatalf("request %d: bytesRead=%d, want 2", i, n)
		}
	}
}

func TestH1ContentLength(t *testing.T) {
	body := "Hello, World! This is a test response body."
	host, port, cleanup := startH1Server(t, func(conn net.Conn) {
		defer func() { _ = conn.Close() }()
		reader := bufio.NewReader(conn)
		for {
			if !readH1Request(reader) {
				return
			}
			resp := fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
			if _, err := conn.Write([]byte(resp)); err != nil {
				return
			}
		}
	})
	defer cleanup()

	client, err := newH1Client(host, port, "/", testH1Cfg(true, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	n, err := client.DoRequest(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(body) {
		t.Errorf("bytesRead=%d, want %d", n, len(body))
	}
}

func TestH1Chunked(t *testing.T) {
	// The chunks: "Hello" (5 bytes) + " World" (6 bytes) = 11 bytes total
	host, port, cleanup := startH1Server(t, func(conn net.Conn) {
		defer func() { _ = conn.Close() }()
		reader := bufio.NewReader(conn)
		for {
			if !readH1Request(reader) {
				return
			}
			resp := "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" +
				"5\r\nHello\r\n" +
				"6\r\n World\r\n" +
				"0\r\n\r\n"
			if _, err := conn.Write([]byte(resp)); err != nil {
				return
			}
		}
	})
	defer cleanup()

	client, err := newH1Client(host, port, "/", testH1Cfg(true, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	n, err := client.DoRequest(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 11 {
		t.Errorf("bytesRead=%d, want 11", n)
	}
}

func TestH1Reconnect(t *testing.T) {
	// Test reconnect via Connection: close mode. The server closes each
	// connection after responding. The client's round-robin pool ensures
	// that when Write() hits a broken pipe on a closed slot, it reconnects
	// and retries successfully. Multiple requests prove the pool recovers.
	var requestCount atomic.Int64

	host, port, cleanup := startH1Server(t, func(conn net.Conn) {
		defer func() { _ = conn.Close() }()
		reader := bufio.NewReader(conn)

		if !readH1Request(reader) {
			return
		}
		requestCount.Add(1)

		resp := "HTTP/1.1 200 OK\r\nConnection: close\r\nContent-Length: 2\r\n\r\nOK"
		_, _ = conn.Write([]byte(resp))
	})
	defer cleanup()

	cfg := testH1Cfg(false, 1) // Connection: close mode with pool
	client, err := newH1Client(host, port, "/", cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	const numRequests = 10
	for i := range numRequests {
		n, err := client.DoRequest(ctx, 0)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		if n != 2 {
			t.Fatalf("request %d: bytesRead=%d, want 2", i, n)
		}
	}

	if n := requestCount.Load(); n < int64(numRequests) {
		t.Errorf("expected at least %d requests served, got %d", numRequests, n)
	}
}

func TestH1LargeResponse(t *testing.T) {
	const bodySize = 1 << 20 // 1MB
	largeBody := bytes.Repeat([]byte("X"), bodySize)

	host, port, cleanup := startH1Server(t, func(conn net.Conn) {
		defer func() { _ = conn.Close() }()
		reader := bufio.NewReader(conn)
		for {
			if !readH1Request(reader) {
				return
			}
			header := fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n", bodySize)
			_, _ = conn.Write([]byte(header))
			// Write body in chunks to avoid buffering the entire thing
			written := 0
			for written < bodySize {
				end := written + 65536
				if end > bodySize {
					end = bodySize
				}
				n, err := conn.Write(largeBody[written:end])
				if err != nil {
					return
				}
				written += n
			}
		}
	})
	defer cleanup()

	client, err := newH1Client(host, port, "/", testH1Cfg(true, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	n, err := client.DoRequest(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != bodySize {
		t.Errorf("bytesRead=%d, want %d", n, bodySize)
	}
}

func TestH1ConcurrentWorkers(t *testing.T) {
	host, port, cleanup := startH1Server(t, func(conn net.Conn) {
		defer func() { _ = conn.Close() }()
		reader := bufio.NewReader(conn)
		for {
			if !readH1Request(reader) {
				return
			}
			resp := "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK"
			if _, err := conn.Write([]byte(resp)); err != nil {
				return
			}
		}
	})
	defer cleanup()

	const workers = 8
	client, err := newH1Client(host, port, "/", testH1Cfg(true, workers))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for range 50 {
				if ctx.Err() != nil {
					return
				}
				_, err := client.DoRequest(ctx, workerID)
				if err != nil && ctx.Err() == nil {
					t.Errorf("worker %d: %v", workerID, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestH1ErrorRecovery(t *testing.T) {
	host, port, cleanup := startH1Server(t, func(conn net.Conn) {
		defer func() { _ = conn.Close() }()
		reader := bufio.NewReader(conn)
		for {
			if !readH1Request(reader) {
				return
			}
			resp := "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 13\r\n\r\nServer Error!"
			if _, err := conn.Write([]byte(resp)); err != nil {
				return
			}
		}
	})
	defer cleanup()

	client, err := newH1Client(host, port, "/", testH1Cfg(true, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	_, err = client.DoRequest(context.Background(), 0)
	if err == nil {
		t.Fatal("expected error for 500 status, got nil")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("expected error to mention status 500, got: %v", err)
	}
}

func TestH1MaxResponseSize(t *testing.T) {
	const bodySize = 1024
	host, port, cleanup := startH1Server(t, func(conn net.Conn) {
		defer func() { _ = conn.Close() }()
		reader := bufio.NewReader(conn)
		if !readH1Request(reader) {
			return
		}
		body := bytes.Repeat([]byte("X"), bodySize)
		resp := fmt.Sprintf("HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n%s", bodySize, body)
		_, _ = conn.Write([]byte(resp))
		// Close connection after sending so drainH1Response gets EOF.
	})
	defer cleanup()

	cfg := testH1Cfg(true, 1)
	cfg.MaxResponseSize = 512 // smaller than body
	client, err := newH1Client(host, port, "/", cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	_, err = client.DoRequest(context.Background(), 0)
	if err == nil {
		t.Fatal("expected error for exceeding MaxResponseSize, got nil")
	}
	if !strings.Contains(err.Error(), "MaxResponseSize") {
		t.Errorf("expected error to mention MaxResponseSize, got: %v", err)
	}
}

func TestH1MaxResponseSizeChunked(t *testing.T) {
	host, port, cleanup := startH1Server(t, func(conn net.Conn) {
		defer func() { _ = conn.Close() }()
		reader := bufio.NewReader(conn)
		if !readH1Request(reader) {
			return
		}
		// Send a chunked response totaling 1100 bytes (exceeding 512 limit).
		// The first chunk (600 bytes) exceeds the limit after being accumulated.
		var sb strings.Builder
		sb.WriteString("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")
		chunk := strings.Repeat("X", 600)
		fmt.Fprintf(&sb, "%x\r\n%s\r\n", len(chunk), chunk)
		chunk2 := strings.Repeat("Y", 500)
		fmt.Fprintf(&sb, "%x\r\n%s\r\n", len(chunk2), chunk2)
		sb.WriteString("0\r\n\r\n")
		_, _ = io.WriteString(conn, sb.String())
		// Close after sending so any drain gets EOF.
	})
	defer cleanup()

	cfg := testH1Cfg(true, 1)
	cfg.MaxResponseSize = 512
	client, err := newH1Client(host, port, "/", cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	_, err = client.DoRequest(context.Background(), 0)
	if err == nil {
		t.Fatal("expected error for exceeding MaxResponseSize in chunked mode, got nil")
	}
	if !strings.Contains(err.Error(), "MaxResponseSize") {
		t.Errorf("expected error to mention MaxResponseSize, got: %v", err)
	}
}

func TestH1ZeroContentLength(t *testing.T) {
	host, port, cleanup := startH1Server(t, func(conn net.Conn) {
		defer func() { _ = conn.Close() }()
		reader := bufio.NewReader(conn)
		for {
			if !readH1Request(reader) {
				return
			}
			resp := "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\n\r\n"
			if _, err := conn.Write([]byte(resp)); err != nil {
				return
			}
		}
	})
	defer cleanup()

	client, err := newH1Client(host, port, "/", testH1Cfg(true, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	n, err := client.DoRequest(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("bytesRead=%d, want 0", n)
	}
}

func TestH1MultipleWorkersConnectionIsolation(t *testing.T) {
	// Each keep-alive worker should have its own connection.
	// We verify by checking that no panics or data races occur.
	host, port, cleanup := startH1Server(t, func(conn net.Conn) {
		defer func() { _ = conn.Close() }()
		reader := bufio.NewReader(conn)
		for {
			if !readH1Request(reader) {
				return
			}
			resp := "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK"
			if _, err := conn.Write([]byte(resp)); err != nil {
				return
			}
		}
	})
	defer cleanup()

	const workers = 4
	client, err := newH1Client(host, port, "/", testH1Cfg(true, workers))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Each worker sends requests concurrently
	var wg sync.WaitGroup
	errCh := make(chan error, workers*10)
	for w := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for range 10 {
				_, err := client.DoRequest(context.Background(), id)
				if err != nil {
					errCh <- fmt.Errorf("worker %d: %w", id, err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}
}
