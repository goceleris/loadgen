package loadgen

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// h2cUpgradePath is the request-target used for the initial H1 GET that
// triggers the h2c upgrade. RFC 7540 §3.2 is agnostic about which resource
// the upgrade happens on; we use "*" semantics via a cheap root path so the
// server can respond to a well-known handler. Callers overriding the target
// path take precedence.
const h2cUpgradeDefaultPath = "/"

// h2cUpgradeSettingsPayload is the wire-format SETTINGS frame payload
// loadgen advertises during an h2c upgrade. Computed once at package init
// from h2HandshakeSettings so every dial reuses the same slice — no
// per-dial encodeH2SettingsPayload call, no per-dial base64 re-encode.
//
// h2cUpgradeSettingsB64 is the base64url (no-pad) encoding of the payload,
// ready to splice into the HTTP2-Settings header without a second allocation.
var (
	h2cUpgradeSettingsPayload []byte
	h2cUpgradeSettingsB64     string
)

func init() {
	h2cUpgradeSettingsPayload = encodeH2SettingsPayload(h2HandshakeSettings)
	h2cUpgradeSettingsB64 = base64.RawURLEncoding.EncodeToString(h2cUpgradeSettingsPayload)
}

// buildH2CUpgradeRequest formats the H1 GET request that triggers the h2c
// upgrade per RFC 7540 §3.2. It carries Connection: Upgrade, HTTP2-Settings
// + Upgrade: h2c + a base64url-encoded (no padding) SETTINGS payload.
//
// The request MUST be a GET with no body: RFC 7540 §3.2 says the request can
// carry a body, but Go's net/http h2c implementation and celeris both reject
// upgrade requests with bodies. Loadgen issues the first request as a
// no-body GET and then multiplexes the real workload on subsequent H2
// streams.
//
// The Host header follows RFC 9110 §7.2: the port is omitted when it is
// the scheme default (80 for http), matching Go's net/http.Request.Write
// canonicalisation. h2c upgrade is http-only (see dialH2CUpgrade), so only
// the port-80 case matters in practice, but the logic is kept
// scheme-aware for future-proofing.
func buildH2CUpgradeRequest(path, host, port, scheme string, settingsPayload []byte) []byte {
	// Size the single-allocation output buffer tightly: 64 bytes of
	// request-line + header-name scaffolding (GET, HTTP/1.1, Host, etc.),
	// plus the variable path/host/port, plus the base64 settings. Fits
	// in one alloc on the happy path.
	b64 := h2cUpgradeSettingsB64
	if settingsPayload != nil && !bytes.Equal(settingsPayload, h2cUpgradeSettingsPayload) {
		// Test-path escape hatch: caller supplied a non-default payload.
		// Pay the per-call base64 cost only here.
		b64 = base64.RawURLEncoding.EncodeToString(settingsPayload)
	}
	portLen := 0
	if !isDefaultPort(scheme, port) {
		portLen = 1 + len(port) // ':' + port
	}
	sz := 4 /* "GET " */ + len(path) + 11 /* " HTTP/1.1\r\n" */ +
		6 /* "Host: " */ + len(host) + portLen + 2 /* "\r\n" */ +
		38 /* "Connection: Upgrade, HTTP2-Settings\r\n" */ +
		15 /* "Upgrade: h2c\r\n" */ +
		16 /* "HTTP2-Settings: " */ + len(b64) + 4 /* "\r\n\r\n" */
	buf := make([]byte, 0, sz)
	buf = append(buf, "GET "...)
	buf = append(buf, path...)
	buf = append(buf, " HTTP/1.1\r\n"...)
	buf = append(buf, "Host: "...)
	buf = append(buf, host...)
	if !isDefaultPort(scheme, port) {
		buf = append(buf, ':')
		buf = append(buf, port...)
	}
	buf = append(buf, "\r\nConnection: Upgrade, HTTP2-Settings\r\nUpgrade: h2c\r\nHTTP2-Settings: "...)
	buf = append(buf, b64...)
	buf = append(buf, "\r\n\r\n"...)
	return buf
}

// isDefaultPort reports whether port is the default for scheme. Empty port
// is treated as default. Used to canonicalise Host headers so we don't emit
// ":80" / ":443" — some strict servers reject non-canonical Host.
func isDefaultPort(scheme, port string) bool {
	if port == "" {
		return true
	}
	switch scheme {
	case "http":
		return port == "80"
	case "https":
		return port == "443"
	}
	return false
}

// encodeH2SettingsPayload serialises the client's SETTINGS frame payload
// (the bytes inside the frame's payload field — not including the 9-byte
// frame header) for use as the HTTP2-Settings header value in the upgrade
// request. Matches the on-wire format: each setting is 2-byte ID + 4-byte
// value (big-endian), concatenated.
func encodeH2SettingsPayload(settings [][2]uint32) []byte {
	out := make([]byte, len(settings)*6)
	for i, s := range settings {
		off := i * 6
		binary.BigEndian.PutUint16(out[off:off+2], uint16(s[0]))
		binary.BigEndian.PutUint32(out[off+2:off+6], s[1])
	}
	return out
}

// dialH2CUpgrade establishes an H2 connection via the RFC 7540 §3.2 cleartext
// upgrade handshake. Only makes sense with the "http" scheme — h2c upgrade
// over TLS is undefined; servers use ALPN instead there. When scheme is
// "https" this returns an error immediately.
//
// Sequence:
//  1. TCP-dial the target (no TLS).
//  2. Send an H1 GET with Connection: Upgrade, HTTP2-Settings + Upgrade: h2c
//     + HTTP2-Settings: <base64url SETTINGS>.
//  3. Parse the H1 response. Require exactly 101 Switching Protocols with
//     Upgrade: h2c and a Connection header listing Upgrade.
//  4. Hand the now-upgraded connection (plus the bufio.Reader positioned past
//     the 101's CRLF CRLF) to completeH2Handshake with initialStreamID=3,
//     because stream 1 is owned by the upgrade GET.
//
// The response to the upgrade GET itself arrives on stream 1; because no
// worker registered a slot for stream 1, readLoop will see chPtr == nil and
// silently discard the frames. That is the correct behaviour — the upgrade
// request is a handshake artefact, not a benchmarked request.
func dialH2CUpgrade(addr, scheme, path, host, port string, maxStreams int, dialTimeout time.Duration, readBufSize, writeBufSize int, _ *tls.Config) (*h2Conn, error) {
	if scheme == "https" {
		return nil, fmt.Errorf("h2c upgrade: not supported over TLS (use -h2 with ALPN instead)")
	}

	conn, err := dialTCPRetry(addr, dialTimeout)
	if err != nil {
		return nil, fmt.Errorf("h2c upgrade: dial: %w", err)
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
		_ = tcpConn.SetReadBuffer(readBufSize)
		_ = tcpConn.SetWriteBuffer(writeBufSize)
	}

	if path == "" {
		path = h2cUpgradeDefaultPath
	}

	req := buildH2CUpgradeRequest(path, host, port, scheme, h2cUpgradeSettingsPayload)

	// Allow up to 10s for the upgrade round-trip. Cleared before returning
	// to the caller so completeH2Handshake can set its own handshake deadline.
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	if _, err := conn.Write(req); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("h2c upgrade: write request: %w", err)
	}

	br := bufio.NewReaderSize(conn, 65536)
	if err := parseH2CUpgradeResponse(br); err != nil {
		_ = conn.Close()
		return nil, err
	}

	// Clear the upgrade-phase deadline; completeH2Handshake manages its own.
	_ = conn.SetDeadline(time.Time{})

	// Stream 1 is owned by the upgrade GET — next client-initiated stream
	// starts at 3 (RFC 7540 §3.2.1).
	return completeH2Handshake(conn, br, addr, maxStreams, 3)
}

// parseH2CUpgradeResponse reads the H1 response to the upgrade request up to
// and including the terminating CRLF CRLF. It validates that the server
// accepted the upgrade: status == 101, Upgrade: h2c, Connection header
// includes "Upgrade". Any deviation returns a specific error naming the
// failure mode — a 200 from a server that ignored the upgrade is NOT silently
// accepted; it is a protocol violation and surfaces as such.
func parseH2CUpgradeResponse(br *bufio.Reader) error {
	statusLine, err := br.ReadSlice('\n')
	if err != nil {
		return fmt.Errorf("h2c upgrade: read status line: %w", err)
	}
	// Trim CR LF
	line := bytes.TrimRight(statusLine, "\r\n")
	// Expect: "HTTP/1.1 101 <reason>"
	if !bytes.HasPrefix(line, []byte("HTTP/1.1 ")) && !bytes.HasPrefix(line, []byte("HTTP/1.0 ")) {
		return fmt.Errorf("h2c upgrade: malformed status line %q", string(line))
	}
	if len(line) < 12 {
		return fmt.Errorf("h2c upgrade: short status line %q", string(line))
	}
	statusBytes := line[9:12]
	status, err := strconv.Atoi(string(statusBytes))
	if err != nil {
		return fmt.Errorf("h2c upgrade: unparseable status %q", string(statusBytes))
	}
	if status != 101 {
		return fmt.Errorf("h2c upgrade: server returned status %d (expected 101 Switching Protocols)", status)
	}

	// Read headers until CRLF CRLF.
	var upgradeVal, connectionVal string
	for {
		headerLine, err := br.ReadSlice('\n')
		if err != nil {
			return fmt.Errorf("h2c upgrade: read header: %w", err)
		}
		trimmed := bytes.TrimRight(headerLine, "\r\n")
		if len(trimmed) == 0 {
			break
		}
		colon := bytes.IndexByte(trimmed, ':')
		if colon < 0 {
			return fmt.Errorf("h2c upgrade: malformed header line %q", string(trimmed))
		}
		name := strings.ToLower(string(bytes.TrimSpace(trimmed[:colon])))
		value := strings.TrimSpace(string(trimmed[colon+1:]))
		switch name {
		case "upgrade":
			upgradeVal = value
		case "connection":
			connectionVal = value
		}
	}

	if !strings.EqualFold(upgradeVal, "h2c") {
		return fmt.Errorf("h2c upgrade: server responded 101 but Upgrade header was %q (expected \"h2c\")", upgradeVal)
	}
	if !connectionListContains(connectionVal, "upgrade") {
		return fmt.Errorf("h2c upgrade: server responded 101 but Connection header %q did not include \"Upgrade\"", connectionVal)
	}
	return nil
}

// connectionListContains reports whether a comma-separated Connection header
// value contains the given token (case-insensitive).
func connectionListContains(header, token string) bool {
	for part := range strings.SplitSeq(header, ",") {
		if strings.EqualFold(strings.TrimSpace(part), token) {
			return true
		}
	}
	return false
}
