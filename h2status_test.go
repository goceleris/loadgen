package loadgen

import (
	"errors"
	"testing"
)

// TestStatusErrorReuseAndAs verifies that statusError() returns the
// same pre-allocated *HTTP2StatusError instance across calls (zero
// alloc on the hot 4xx path) and that the public errors.As contract
// still works for callers that want to inspect the status field.
func TestStatusErrorReuseAndAs(t *testing.T) {
	for _, code := range []int{200, 400, 404, 429, 500, 503} {
		a := statusError(code)
		b := statusError(code)
		if a != b {
			t.Errorf("statusError(%d) returned different instances", code)
		}
		var se *HTTP2StatusError
		if !errors.As(a, &se) {
			t.Errorf("errors.As(statusError(%d)) failed", code)
			continue
		}
		if se.Status != code {
			t.Errorf("HTTP2StatusError.Status = %d, want %d", se.Status, code)
		}
	}
	// Out-of-range still returns a valid error (just freshly allocated).
	got := statusError(999)
	var se *HTTP2StatusError
	if !errors.As(got, &se) || se.Status != 999 {
		t.Errorf("out-of-range statusError(999) lost status info")
	}
}

// TestResetErrorReuseAndAs same contract for stream-reset codes.
func TestResetErrorReuseAndAs(t *testing.T) {
	for _, code := range []uint32{0, 7, 8, 11, 14} {
		a := resetError(code)
		b := resetError(code)
		if a != b {
			t.Errorf("resetError(%d) returned different instances", code)
		}
		var re *HTTP2ResetError
		if !errors.As(a, &re) {
			t.Errorf("errors.As(resetError(%d)) failed", code)
			continue
		}
		if re.Code != code {
			t.Errorf("HTTP2ResetError.Code = %d, want %d", re.Code, code)
		}
	}
}

// BenchmarkStatusError documents the per-call cost: should be 0 B/op,
// 0 allocs/op, since pre-allocated table entries are returned by
// pointer.
func BenchmarkStatusError(b *testing.B) {
	for b.Loop() {
		_ = statusError(404)
	}
}

func BenchmarkResetError(b *testing.B) {
	for b.Loop() {
		_ = resetError(7)
	}
}

func TestExtractStatus(t *testing.T) {
	tests := []struct {
		name   string
		input  []byte
		expect int
	}{
		// Fast path: indexed static table entries
		{"200 indexed", []byte{0x88}, 200},
		{"204 indexed", []byte{0x89}, 204},
		{"206 indexed", []byte{0x8A}, 206},
		{"304 indexed", []byte{0x8B}, 304},
		{"400 indexed", []byte{0x8C}, 400},
		{"404 indexed", []byte{0x8D}, 404},
		{"500 indexed", []byte{0x8E}, 500},

		// With dynamic table size update prefix (server sends 0x20 for size=0)
		{"200 with table size update", []byte{0x20, 0x88}, 200},
		{"404 with table size update", []byte{0x20, 0x8D}, 404},

		// Literal without indexing, name index 8 (:status), non-Huffman value
		{"201 literal without indexing", []byte{0x08, 0x03, '2', '0', '1'}, 201},
		{"301 literal without indexing", []byte{0x08, 0x03, '3', '0', '1'}, 301},
		{"503 literal without indexing", []byte{0x08, 0x03, '5', '0', '3'}, 503},

		// Literal never indexed, name index 8
		{"201 literal never indexed", []byte{0x18, 0x03, '2', '0', '1'}, 201},

		// Literal with incremental indexing, name index 8
		{"201 literal incremental", []byte{0x48, 0x03, '2', '0', '1'}, 201},

		// Edge cases
		{"empty", []byte{}, 0},
		{"unknown indexed", []byte{0x80}, 0},                       // index 0 (not valid)
		{"wrong name index", []byte{0x07, 0x03, '2', '0', '0'}, 0}, // name index 7, not :status
		{"truncated literal", []byte{0x08, 0x03, '2', '0'}, 0},     // missing last byte
		{"huffman value", []byte{0x08, 0x82, 0x00, 0x00}, 0},       // huffman flag set

		// Table size update followed by literal
		{"table update + literal 201", []byte{0x20, 0x08, 0x03, '2', '0', '1'}, 201},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractStatus(tt.input)
			if got != tt.expect {
				t.Errorf("extractStatus(%x) = %d, want %d", tt.input, got, tt.expect)
			}
		})
	}
}

func BenchmarkExtractStatus200(b *testing.B) {
	block := []byte{0x88}
	for b.Loop() {
		extractStatus(block)
	}
}

func BenchmarkExtractStatusLiteral(b *testing.B) {
	block := []byte{0x08, 0x03, '2', '0', '1'}
	for b.Loop() {
		extractStatus(block)
	}
}

func BenchmarkExtractStatusWithTableUpdate(b *testing.B) {
	block := []byte{0x20, 0x88}
	for b.Loop() {
		extractStatus(block)
	}
}
