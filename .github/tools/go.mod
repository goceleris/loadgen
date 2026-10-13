// Versions of the Go tools CI runs, kept out of the loadgen module so they
// add nothing to its dependency graph. Each is a `tool` directive, pinned by
// go.sum and bumped by Dependabot (.github/dependabot.yml); a workflow runs
// one with `go tool -modfile=$GITHUB_WORKSPACE/.github/tools/go.mod <name>`.
// This module is never imported or released (loadgen#103).
module loadgen-ci-tools

go 1.27.2

tool github.com/rhysd/actionlint/cmd/actionlint

require (
	github.com/bmatcuk/doublestar/v4 v4.10.3 // indirect
	github.com/clipperhouse/uax29/v2 v2.7.0 // indirect
	github.com/fatih/color v1.19.0 // indirect
	github.com/mattn/go-colorable v0.1.16 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/mattn/go-runewidth v0.0.31 // indirect
	github.com/mattn/go-shellwords v1.0.16 // indirect
	github.com/rhysd/actionlint v1.7.12 // indirect
	github.com/robfig/cron/v3 v3.0.1 // indirect
	go.yaml.in/yaml/v4 v4.0.0-rc.3 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
)
