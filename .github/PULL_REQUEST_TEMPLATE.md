## Summary

Brief description of the change and why it is needed.

## Changes

-

## Test Plan

- [ ] Unit tests added/updated (`go test -race -count=1 ./...`)
- [ ] `golangci-lint run` and `go vet ./...` pass
- [ ] Integration matrix passes if the H1/H2/WS/SSE client paths changed (`go test -tags integration -run TestIntegrationH2CMatrix .`)
- [ ] README cluster-bench contract updated if the JSON result schema or CLI flags changed

Closes #
