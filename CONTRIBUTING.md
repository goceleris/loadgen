# Contributing to loadgen

Thanks for helping improve loadgen, the load generator behind the celeris
benchmark suite. loadgen is deliberately dependency-light (two direct Go
dependencies) and is exercised on real hardware by
[goceleris/probatorium](https://github.com/goceleris/probatorium), so
changes are held to the same standard as the engine they measure.

## Prerequisites

- Go **1.27.0** (the version pinned in `go.mod` and in CI)
- [golangci-lint](https://golangci-lint.run/) v2.13+ (the CI pin)
- Linux or macOS. The client itself is portable; the process-CPU sampler
  and recv-queue probe have Linux implementations and `_other.go`
  fallbacks.

## Build and test

```bash
go build ./...                                   # compile everything, including cmd/loadgen
go vet ./...
golangci-lint run                                # same config CI uses (.golangci.yml)
go test -race -count=1 -timeout 120s ./...       # unit tests, exactly as CI runs them

# Integration matrix against a live celeris server. The testserver helper is
# a nested module that imports celeris; the tests spawn it as a subprocess.
go test -tags integration -race -count=1 -timeout 180s -v -run TestIntegrationH2CMatrix .
```

`gofmt` and `goimports` (with `github.com/goceleris/loadgen` as the local
prefix) are enforced by golangci-lint, so run it before pushing.

## Pull request flow

1. Fork the repository and create a topic branch from `main`
   (`feat/…`, `fix/…`, `perf/…`, `chore/…`).
2. Keep each PR focused on a single change and include tests — a
   correctness fix without a regression test is not complete.
3. Write commit messages in the `type: description` format
   (`feat:`, `fix:`, `perf:`, `security:`, `test:`, `ci:`, `docs:`, `chore:`)
   and explain *why* in the body when it is not obvious from the diff.
4. Fill in the pull request template (Summary, Changes, Test Plan,
   `Closes #…`). Changes to the JSON result schema or the CLI flags must
   also update the README's cluster-bench contract section, because
   probatorium parses that output.
5. Make sure the CI workflow (lint, actionlint, unit tests, integration
   matrix) is green.

## Merge rule

loadgen follows the same governance as celeris — see
[celeris/GOVERNANCE.md](https://github.com/goceleris/celeris/blob/main/GOVERNANCE.md).
In short: `main` is protected, every change lands through a pull request
that passes CI and is approved by a code owner (see `.github/CODEOWNERS`),
and pull requests are merged by a maintainer — never force-pushed. Releases
are tagged from `main` and the release workflow builds, attests and
publishes the binaries.

## Reporting security issues

Do not open a public issue. Follow [SECURITY.md](SECURITY.md).

## License

By contributing you agree that your contributions are licensed under the
[Apache License 2.0](LICENSE) that covers the project.
