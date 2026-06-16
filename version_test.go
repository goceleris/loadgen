package loadgen

import (
	"runtime/debug"
	"testing"
)

func TestVersionFromBuildInfo(t *testing.T) {
	tests := []struct {
		name string
		bi   *debug.BuildInfo
		ok   bool
		want string
	}{
		{"no build info", nil, false, fallbackVersion},
		{
			"main module tagged",
			&debug.BuildInfo{Main: debug.Module{Path: modulePath, Version: "v1.9.3"}},
			true,
			"1.9.3",
		},
		{
			"main module devel falls back",
			&debug.BuildInfo{Main: debug.Module{Path: modulePath, Version: "(devel)"}},
			true,
			fallbackVersion,
		},
		{
			"consumed as dependency",
			&debug.BuildInfo{
				Main: debug.Module{Path: "github.com/goceleris/probatorium", Version: "(devel)"},
				Deps: []*debug.Module{
					{Path: "github.com/HdrHistogram/hdrhistogram-go", Version: "v1.2.0"},
					{Path: modulePath, Version: "v1.4.8"},
				},
			},
			true,
			"1.4.8",
		},
		{
			"dependency with versioned replace",
			&debug.BuildInfo{
				Main: debug.Module{Path: "github.com/goceleris/probatorium"},
				Deps: []*debug.Module{
					{Path: modulePath, Version: "v1.4.7", Replace: &debug.Module{Path: modulePath, Version: "v1.4.7-hotfix"}},
				},
			},
			true,
			"1.4.7-hotfix",
		},
		{
			"dependency with directory replace falls back",
			&debug.BuildInfo{
				Main: debug.Module{Path: "github.com/goceleris/probatorium"},
				Deps: []*debug.Module{
					{Path: modulePath, Version: "v1.4.7", Replace: &debug.Module{Path: "../loadgen"}},
				},
			},
			true,
			fallbackVersion,
		},
		{
			"unrelated build falls back",
			&debug.BuildInfo{Main: debug.Module{Path: "example.com/other", Version: "v9.9.9"}},
			true,
			fallbackVersion,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			if got := versionFromBuildInfo(tt.bi, tt.ok); got != tt.want {
				st.Errorf("versionFromBuildInfo() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestVersionNonEmpty(t *testing.T) {
	if Version == "" {
		t.Fatal("Version must never be empty")
	}
	if Version == "(devel)" {
		t.Fatalf("Version = %q — (devel) must fall back to the release constant", Version)
	}
}
