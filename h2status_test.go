package loadgen

import "testing"

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
