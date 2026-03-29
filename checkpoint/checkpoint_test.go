package checkpoint

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/goceleris/loadgen"
)

func TestCheckpointSaveLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checkpoint.json")

	output := &BenchmarkOutput{
		Timestamp:    "2026-03-29T10:00:00Z",
		Architecture: "arm64",
		Config: BenchmarkConfig{
			Duration:     "30s",
			DurationSecs: 30,
			Connections:  256,
			Workers:      64,
		},
		Results: []ServerResult{
			{
				Server:         "celeris",
				Benchmark:      "h1-keepalive",
				Method:         "GET",
				Path:           "/",
				RequestsPerSec: 100000,
				TotalRequests:  3000000,
				DurationSecs:   30,
				Latency: LatencyResult{
					Avg: "50us",
					P99: "200us",
				},
			},
		},
	}

	cp := NewCheckpoint(output)
	cp.MarkCompleted("celeris", "h1-keepalive")

	if err := cp.Save(path); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := LoadCheckpoint(path)
	if err != nil {
		t.Fatalf("LoadCheckpoint: %v", err)
	}

	if loaded.Timestamp != cp.Timestamp {
		t.Errorf("Timestamp: got %q, want %q", loaded.Timestamp, cp.Timestamp)
	}
	if loaded.Architecture != cp.Architecture {
		t.Errorf("Architecture: got %q, want %q", loaded.Architecture, cp.Architecture)
	}
	if loaded.Config.Duration != cp.Config.Duration {
		t.Errorf("Config.Duration: got %q, want %q", loaded.Config.Duration, cp.Config.Duration)
	}
	if loaded.Config.Connections != cp.Config.Connections {
		t.Errorf("Config.Connections: got %d, want %d", loaded.Config.Connections, cp.Config.Connections)
	}
	if loaded.Config.Workers != cp.Config.Workers {
		t.Errorf("Config.Workers: got %d, want %d", loaded.Config.Workers, cp.Config.Workers)
	}
	if len(loaded.Results) != 1 {
		t.Fatalf("Results: got %d, want 1", len(loaded.Results))
	}
	r := loaded.Results[0]
	if r.Server != "celeris" {
		t.Errorf("Server: got %q, want %q", r.Server, "celeris")
	}
	if r.Benchmark != "h1-keepalive" {
		t.Errorf("Benchmark: got %q, want %q", r.Benchmark, "h1-keepalive")
	}
	if r.RequestsPerSec != 100000 {
		t.Errorf("RequestsPerSec: got %f, want 100000", r.RequestsPerSec)
	}
	if !loaded.IsCompleted("celeris", "h1-keepalive") {
		t.Error("expected celeris:h1-keepalive to be completed")
	}
}

func TestCheckpointMergeResults(t *testing.T) {
	cp1 := NewCheckpoint(&BenchmarkOutput{
		Timestamp: "2026-03-29T10:00:00Z",
		Results: []ServerResult{
			{Server: "celeris", Benchmark: "h1-keepalive", RequestsPerSec: 100000},
		},
	})
	cp1.MarkCompleted("celeris", "h1-keepalive")

	cp2 := NewCheckpoint(&BenchmarkOutput{
		Results: []ServerResult{
			{Server: "celeris", Benchmark: "h1-keepalive", RequestsPerSec: 110000}, // duplicate
			{Server: "celeris", Benchmark: "h2-multiplex", RequestsPerSec: 200000}, // new
		},
	})
	cp2.MarkCompleted("celeris", "h1-keepalive")
	cp2.MarkCompleted("celeris", "h2-multiplex")

	cp1.MergeResults(cp2)

	if len(cp1.Results) != 2 {
		t.Fatalf("Results after merge: got %d, want 2", len(cp1.Results))
	}

	// First result should be original (not overwritten)
	if cp1.Results[0].RequestsPerSec != 100000 {
		t.Errorf("first result RPS: got %f, want 100000 (should not be overwritten)", cp1.Results[0].RequestsPerSec)
	}

	// Second result should be the new one
	if cp1.Results[1].Server != "celeris" || cp1.Results[1].Benchmark != "h2-multiplex" {
		t.Errorf("second result: got %s:%s, want celeris:h2-multiplex", cp1.Results[1].Server, cp1.Results[1].Benchmark)
	}

	if !cp1.IsCompleted("celeris", "h1-keepalive") {
		t.Error("expected celeris:h1-keepalive to be completed")
	}
	if !cp1.IsCompleted("celeris", "h2-multiplex") {
		t.Error("expected celeris:h2-multiplex to be completed")
	}
}

func TestCheckpointIsCompleted(t *testing.T) {
	cp := NewCheckpoint(&BenchmarkOutput{})

	if cp.IsCompleted("celeris", "h1-keepalive") {
		t.Error("expected not completed for fresh checkpoint")
	}

	cp.MarkCompleted("celeris", "h1-keepalive")
	if !cp.IsCompleted("celeris", "h1-keepalive") {
		t.Error("expected completed after marking")
	}

	if cp.IsCompleted("celeris", "h2-multiplex") {
		t.Error("expected not completed for unmarked combination")
	}
	if cp.IsCompleted("fiber", "h1-keepalive") {
		t.Error("expected not completed for different server")
	}
}

func TestNewServerResult(t *testing.T) {
	result := &loadgen.Result{
		Requests:       1000000,
		Errors:         5,
		Duration:       30 * time.Second,
		RequestsPerSec: 33333.33,
		ThroughputBPS:  1024 * 1024 * 10,
		Latency: loadgen.Percentiles{
			Avg:   50 * time.Microsecond,
			Min:   10 * time.Microsecond,
			Max:   5 * time.Millisecond,
			P50:   45 * time.Microsecond,
			P75:   80 * time.Microsecond,
			P90:   150 * time.Microsecond,
			P99:   500 * time.Microsecond,
			P999:  2 * time.Millisecond,
			P9999: 4 * time.Millisecond,
		},
		ClientCPUPercent: 25.5,
		Timeseries: []loadgen.TimeseriesPoint{
			{TimestampSec: 1.0, RequestsPerSec: 30000},
			{TimestampSec: 2.0, RequestsPerSec: 35000},
		},
	}

	sr := NewServerResult(result, "celeris", "h1-keepalive", "GET", "/json")

	if sr.Server != "celeris" {
		t.Errorf("Server: got %q, want %q", sr.Server, "celeris")
	}
	if sr.Benchmark != "h1-keepalive" {
		t.Errorf("Benchmark: got %q, want %q", sr.Benchmark, "h1-keepalive")
	}
	if sr.Method != "GET" {
		t.Errorf("Method: got %q, want %q", sr.Method, "GET")
	}
	if sr.Path != "/json" {
		t.Errorf("Path: got %q, want %q", sr.Path, "/json")
	}
	if sr.RequestsPerSec != result.RequestsPerSec {
		t.Errorf("RequestsPerSec: got %f, want %f", sr.RequestsPerSec, result.RequestsPerSec)
	}
	if sr.ThroughputBPS != result.ThroughputBPS {
		t.Errorf("ThroughputBPS: got %f, want %f", sr.ThroughputBPS, result.ThroughputBPS)
	}
	if sr.DurationSecs != 30 {
		t.Errorf("DurationSecs: got %f, want 30", sr.DurationSecs)
	}
	if sr.TotalRequests != 1000000 {
		t.Errorf("TotalRequests: got %d, want 1000000", sr.TotalRequests)
	}
	if sr.Errors != 5 {
		t.Errorf("Errors: got %d, want 5", sr.Errors)
	}

	// Latency fields
	if sr.Latency.Avg != result.Latency.Avg.String() {
		t.Errorf("Latency.Avg: got %q, want %q", sr.Latency.Avg, result.Latency.Avg.String())
	}
	if sr.Latency.P99 != result.Latency.P99.String() {
		t.Errorf("Latency.P99: got %q, want %q", sr.Latency.P99, result.Latency.P99.String())
	}

	// System metrics
	if sr.Metrics == nil {
		t.Fatal("expected Metrics to be non-nil")
	}
	if sr.Metrics.ClientCPUPercent != 25.5 {
		t.Errorf("ClientCPUPercent: got %f, want 25.5", sr.Metrics.ClientCPUPercent)
	}
	if len(sr.Metrics.Timeseries) != 2 {
		t.Errorf("Timeseries: got %d entries, want 2", len(sr.Metrics.Timeseries))
	}
}

func TestNewServerResultNoMetrics(t *testing.T) {
	result := &loadgen.Result{
		Requests:       100,
		Duration:       1 * time.Second,
		RequestsPerSec: 100,
	}

	sr := NewServerResult(result, "test", "bench", "GET", "/")

	if sr.Metrics != nil {
		t.Error("expected Metrics to be nil when no CPU/timeseries data")
	}
}

func TestCheckpointFileError(t *testing.T) {
	// Save to invalid path
	cp := NewCheckpoint(&BenchmarkOutput{})
	err := cp.Save("/nonexistent/dir/checkpoint.json")
	if err == nil {
		t.Error("expected error saving to invalid path")
	}

	// Load from non-existent file
	_, err = LoadCheckpoint("/nonexistent/file.json")
	if err == nil {
		t.Error("expected error loading non-existent file")
	}
}

func TestCheckpointCorruptData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "corrupt.json")

	// Write invalid JSON
	if err := os.WriteFile(path, []byte("not valid json{{{"), 0644); err != nil {
		t.Fatal(err)
	}

	_, err := LoadCheckpoint(path)
	if err == nil {
		t.Error("expected error loading corrupt JSON")
	}
}

func TestCheckpointAddResult(t *testing.T) {
	cp := NewCheckpoint(&BenchmarkOutput{})

	sr := ServerResult{
		Server:         "celeris",
		Benchmark:      "h1-close",
		RequestsPerSec: 50000,
	}

	cp.AddResult(sr)

	if len(cp.Results) != 1 {
		t.Fatalf("Results: got %d, want 1", len(cp.Results))
	}
	if !cp.IsCompleted("celeris", "h1-close") {
		t.Error("expected celeris:h1-close to be completed after AddResult")
	}
}

func TestCheckpointToBenchmarkOutput(t *testing.T) {
	cp := NewCheckpoint(&BenchmarkOutput{
		Timestamp:    "2026-03-29T10:00:00Z",
		Architecture: "amd64",
		Config: BenchmarkConfig{
			Duration:    "30s",
			Connections: 256,
		},
		Results: []ServerResult{
			{Server: "celeris", Benchmark: "h1-keepalive"},
		},
	})
	cp.MarkCompleted("celeris", "h1-keepalive")

	output := cp.ToBenchmarkOutput()

	if output.Timestamp != "2026-03-29T10:00:00Z" {
		t.Errorf("Timestamp: got %q", output.Timestamp)
	}
	if output.Architecture != "amd64" {
		t.Errorf("Architecture: got %q", output.Architecture)
	}
	if len(output.Results) != 1 {
		t.Errorf("Results: got %d, want 1", len(output.Results))
	}
}

func TestCheckpointToJSON(t *testing.T) {
	output := &BenchmarkOutput{
		Timestamp:    "2026-03-29T10:00:00Z",
		Architecture: "arm64",
		Config: BenchmarkConfig{
			Duration:    "30s",
			Connections: 256,
		},
	}

	data, err := output.ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Error("expected non-empty JSON output")
	}

	// Verify it contains expected fields
	s := string(data)
	if !contains(s, "2026-03-29T10:00:00Z") {
		t.Error("JSON missing timestamp")
	}
	if !contains(s, "arm64") {
		t.Error("JSON missing architecture")
	}
}

func TestCheckpointSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "roundtrip.json")

	cp := NewCheckpoint(&BenchmarkOutput{
		Timestamp:    "2026-03-29T12:00:00Z",
		Architecture: "arm64",
		Config: BenchmarkConfig{
			Duration:      "15s",
			DurationSecs:  15,
			WarmupSecs:    5,
			Connections:   128,
			Workers:       32,
			CPUs:          8,
			H2Connections: 16,
			H2MaxStreams:  100,
			BodySizeBytes: 1024,
		},
	})

	sr1 := ServerResult{
		Server:         "celeris",
		Benchmark:      "h1-keepalive",
		Method:         "GET",
		Path:           "/",
		RequestsPerSec: 150000,
		ThroughputBPS:  1024 * 1024,
		TransferPerSec: "1.00MB/s",
		DurationSecs:   15,
		TotalRequests:  2250000,
		Errors:         0,
		Latency: LatencyResult{
			Avg: "30us", Max: "2ms", P50: "25us", P75: "40us",
			P90: "80us", P99: "200us", P999: "1ms", P9999: "1.5ms",
		},
	}
	cp.AddResult(sr1)

	sr2 := ServerResult{
		Server:    "fiber",
		Benchmark: "h1-keepalive",
	}
	cp.AddResult(sr2)

	if err := cp.Save(path); err != nil {
		t.Fatal(err)
	}

	loaded, err := LoadCheckpoint(path)
	if err != nil {
		t.Fatal(err)
	}

	if len(loaded.Results) != 2 {
		t.Fatalf("Results: got %d, want 2", len(loaded.Results))
	}
	if !loaded.IsCompleted("celeris", "h1-keepalive") {
		t.Error("expected celeris:h1-keepalive completed")
	}
	if !loaded.IsCompleted("fiber", "h1-keepalive") {
		t.Error("expected fiber:h1-keepalive completed")
	}
	if loaded.Config.CPUs != 8 {
		t.Errorf("Config.CPUs: got %d, want 8", loaded.Config.CPUs)
	}
	if loaded.Config.H2Connections != 16 {
		t.Errorf("Config.H2Connections: got %d, want 16", loaded.Config.H2Connections)
	}
}

func TestCheckpointEmptyCompleted(t *testing.T) {
	// Verify that loading a checkpoint with null Completed map initializes it
	dir := t.TempDir()
	path := filepath.Join(dir, "empty_completed.json")

	// Write JSON without Completed field
	data := `{"timestamp":"test","architecture":"amd64","config":{"duration":"30s","connections":256,"workers":64},"results":[]}`
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}

	cp, err := LoadCheckpoint(path)
	if err != nil {
		t.Fatal(err)
	}

	// Completed should be initialized (not nil)
	if cp.Completed == nil {
		t.Fatal("expected Completed map to be initialized")
	}

	// Should be able to use it without panic
	cp.MarkCompleted("test", "bench")
	if !cp.IsCompleted("test", "bench") {
		t.Error("expected completed after marking on loaded checkpoint")
	}
}

// contains is a helper to check for substring presence.
func contains(s, substr string) bool {
	for i := range len(s) - len(substr) + 1 {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
