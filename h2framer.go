package loadgen

import (
	"bufio"
	"encoding/binary"
	"io"
)

// H2 frame type constants.
const (
	frameData         = 0x0
	frameHeaders      = 0x1
	frameRSTStream    = 0x3
	frameSettings     = 0x4
	framePing         = 0x6
	frameGoAway       = 0x7
	frameWindowUpdate = 0x8
)

// H2 frame flag constants.
const (
	flagEndStream  = 0x1
	flagACK        = 0x1
	flagEndHeaders = 0x4
)

// H2 settings IDs.
const (
	settingHeaderTableSize      = 0x1
	settingEnablePush           = 0x2
	settingMaxConcurrentStreams = 0x3
	settingInitialWindowSize    = 0x4
	settingMaxFrameSize         = 0x5
)

// h2Frame is a value type representing an HTTP/2 frame.
// Unlike http2.Frame (which uses Go interfaces and type assertions),
// this uses uint8 dispatch and direct field access. The payload slice
// is valid only until the next ReadFrame call.
type h2Frame struct {
	Type     uint8
	Flags    uint8
	StreamID uint32
	Length   uint32
	payload  []byte // slice into framer's readBuf
}

// StreamEnded reports whether the END_STREAM flag is set.
func (f *h2Frame) StreamEnded() bool { return f.Flags&flagEndStream != 0 }

// IsAck reports whether the ACK flag is set (for SETTINGS and PING).
func (f *h2Frame) IsAck() bool { return f.Flags&flagACK != 0 }

// HeaderBlockFragment returns the header block fragment from a HEADERS frame.
func (f *h2Frame) HeaderBlockFragment() []byte { return f.payload }

// Data returns the data payload from a DATA frame.
func (f *h2Frame) Data() []byte { return f.payload }

// ErrCode returns the error code from a RST_STREAM frame (payload[0:4]).
func (f *h2Frame) ErrCode() uint32 {
	if len(f.payload) >= 4 {
		return binary.BigEndian.Uint32(f.payload[:4])
	}
	return 0
}

// GoAwayErrCode returns the error code from a GOAWAY frame (payload[4:8]).
func (f *h2Frame) GoAwayErrCode() uint32 {
	if len(f.payload) >= 8 {
		return binary.BigEndian.Uint32(f.payload[4:8])
	}
	return 0
}

// PingData returns the 8-byte ping data.
func (f *h2Frame) PingData() [8]byte {
	var d [8]byte
	if len(f.payload) >= 8 {
		copy(d[:], f.payload[:8])
	}
	return d
}

// ForeachSetting iterates over settings in a SETTINGS frame payload.
func (f *h2Frame) ForeachSetting(fn func(id uint16, val uint32)) {
	p := f.payload
	for len(p) >= 6 {
		id := binary.BigEndian.Uint16(p[:2])
		val := binary.BigEndian.Uint32(p[2:6])
		fn(id, val)
		p = p[6:]
	}
}

// h2Framer is a zero-allocation HTTP/2 frame reader/writer.
// It replaces golang.org/x/net/http2.Framer with:
//   - In-place parsing (no per-frame allocation)
//   - uint8 type dispatch (no Go type assertion)
//   - Single-write optimization for small frames (WINDOW_UPDATE, PING, SETTINGS_ACK)
type h2Framer struct {
	br      *bufio.Reader
	bw      *bufio.Writer
	rhdr    [9]byte  // read: frame header scratch (readLoop goroutine only)
	readBuf []byte   // read: payload buffer (reused, grows lazily)
	whdr    [9]byte  // write: frame header scratch (writeLoop goroutine only)
	wbuf    [17]byte // write: header(9) + small payload(up to 8), single-write for small frames
}

// newH2Framer creates a new h2Framer.
func newH2Framer(bw *bufio.Writer, br *bufio.Reader) *h2Framer {
	return &h2Framer{
		br:      br,
		bw:      bw,
		readBuf: make([]byte, 0, 16384),
	}
}

// ReadFrame reads the next HTTP/2 frame. The returned frame's payload
// is valid only until the next ReadFrame call. Zero alloc in steady state.
func (fr *h2Framer) ReadFrame() (h2Frame, error) {
	if _, err := io.ReadFull(fr.br, fr.rhdr[:]); err != nil {
		return h2Frame{}, err
	}

	length := uint32(fr.rhdr[0])<<16 | uint32(fr.rhdr[1])<<8 | uint32(fr.rhdr[2])
	f := h2Frame{
		Type:     fr.rhdr[3],
		Flags:    fr.rhdr[4],
		StreamID: binary.BigEndian.Uint32(fr.rhdr[5:9]) & 0x7FFFFFFF,
		Length:   length,
	}

	if length == 0 {
		f.payload = fr.readBuf[:0]
		return f, nil
	}

	// Grow readBuf lazily
	if uint32(cap(fr.readBuf)) < length {
		fr.readBuf = make([]byte, length)
	} else {
		fr.readBuf = fr.readBuf[:length]
	}

	if _, err := io.ReadFull(fr.br, fr.readBuf); err != nil {
		return h2Frame{}, err
	}
	f.payload = fr.readBuf
	return f, nil
}

// writeFrame writes a frame header + payload to the buffered writer.
func (fr *h2Framer) writeFrame(ftype uint8, flags uint8, streamID uint32, payload []byte) error {
	length := len(payload)
	fr.whdr[0] = byte(length >> 16)
	fr.whdr[1] = byte(length >> 8)
	fr.whdr[2] = byte(length)
	fr.whdr[3] = ftype
	fr.whdr[4] = flags
	binary.BigEndian.PutUint32(fr.whdr[5:9], streamID)

	if _, err := fr.bw.Write(fr.whdr[:]); err != nil {
		return err
	}
	if length > 0 {
		if _, err := fr.bw.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// WriteHeaders writes a HEADERS frame with END_HEADERS always set.
func (fr *h2Framer) WriteHeaders(streamID uint32, blockFragment []byte, endStream bool) error {
	flags := uint8(flagEndHeaders)
	if endStream {
		flags |= flagEndStream
	}
	return fr.writeFrame(frameHeaders, flags, streamID, blockFragment)
}

// WriteData writes a DATA frame.
func (fr *h2Framer) WriteData(streamID uint32, endStream bool, data []byte) error {
	var flags uint8
	if endStream {
		flags = flagEndStream
	}
	return fr.writeFrame(frameData, flags, streamID, data)
}

// WriteSettings writes a SETTINGS frame. Each setting is [2]uint32{id, value}.
func (fr *h2Framer) WriteSettings(settings [][2]uint32) error {
	payload := make([]byte, len(settings)*6)
	for i, s := range settings {
		off := i * 6
		binary.BigEndian.PutUint16(payload[off:off+2], uint16(s[0]))
		binary.BigEndian.PutUint32(payload[off+2:off+6], s[1])
	}
	return fr.writeFrame(frameSettings, 0, 0, payload)
}

// WriteSettingsAck writes a SETTINGS frame with ACK flag (no payload).
// Single 9-byte write.
func (fr *h2Framer) WriteSettingsAck() error {
	fr.wbuf[0] = 0 // length = 0
	fr.wbuf[1] = 0
	fr.wbuf[2] = 0
	fr.wbuf[3] = frameSettings
	fr.wbuf[4] = flagACK
	fr.wbuf[5] = 0
	fr.wbuf[6] = 0
	fr.wbuf[7] = 0
	fr.wbuf[8] = 0
	_, err := fr.bw.Write(fr.wbuf[:9])
	return err
}

// WritePing writes a PING frame. Single 17-byte write via wbuf.
func (fr *h2Framer) WritePing(ack bool, data [8]byte) error {
	fr.wbuf[0] = 0 // length = 8
	fr.wbuf[1] = 0
	fr.wbuf[2] = 8
	fr.wbuf[3] = framePing
	if ack {
		fr.wbuf[4] = flagACK
	} else {
		fr.wbuf[4] = 0
	}
	fr.wbuf[5] = 0
	fr.wbuf[6] = 0
	fr.wbuf[7] = 0
	fr.wbuf[8] = 0
	copy(fr.wbuf[9:17], data[:])
	_, err := fr.bw.Write(fr.wbuf[:17])
	return err
}

// WriteWindowUpdate writes a WINDOW_UPDATE frame. Single 13-byte write via wbuf.
// This is the hot path — called frequently during data transfer.
func (fr *h2Framer) WriteWindowUpdate(streamID, incr uint32) error {
	fr.wbuf[0] = 0 // length = 4
	fr.wbuf[1] = 0
	fr.wbuf[2] = 4
	fr.wbuf[3] = frameWindowUpdate
	fr.wbuf[4] = 0
	binary.BigEndian.PutUint32(fr.wbuf[5:9], streamID)
	binary.BigEndian.PutUint32(fr.wbuf[9:13], incr)
	_, err := fr.bw.Write(fr.wbuf[:13])
	return err
}

// WriteGoAway writes a GOAWAY frame.
func (fr *h2Framer) WriteGoAway(lastStreamID, errCode uint32, debugData []byte) error {
	if len(debugData) == 0 {
		// Single 17-byte write via wbuf: header(9) + lastStreamID(4) + errCode(4)
		fr.wbuf[0] = 0 // length = 8
		fr.wbuf[1] = 0
		fr.wbuf[2] = 8
		fr.wbuf[3] = frameGoAway
		fr.wbuf[4] = 0
		binary.BigEndian.PutUint32(fr.wbuf[5:9], 0) // stream 0
		binary.BigEndian.PutUint32(fr.wbuf[9:13], lastStreamID)
		binary.BigEndian.PutUint32(fr.wbuf[13:17], errCode)
		_, err := fr.bw.Write(fr.wbuf[:17])
		return err
	}
	payload := make([]byte, 8+len(debugData))
	binary.BigEndian.PutUint32(payload[0:4], lastStreamID)
	binary.BigEndian.PutUint32(payload[4:8], errCode)
	copy(payload[8:], debugData)
	return fr.writeFrame(frameGoAway, 0, 0, payload)
}
