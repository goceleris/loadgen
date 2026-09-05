# Security Policy

loadgen is a load-generation *client*: it opens outbound connections to a
target you name on the command line and never listens on a socket in
production use. Its security surface is therefore small — a malicious
server could still try to exploit the HTTP/1.1, HTTP/2, WebSocket and SSE
response parsers, and the JSON result written to stdout is consumed by
orchestrators — so we treat parser and output-handling bugs as security
issues.

## Reporting a Vulnerability

**Please do not open a public issue for security problems.**

Report privately, in order of preference:

1. **GitHub private vulnerability reporting (preferred)** — open the
   repository's **Security** tab and click **"Report a vulnerability"**.
   Private vulnerability reporting is enabled on this repository; the report
   is visible only to the maintainers and becomes the draft advisory that
   ships with the fix.
2. **Email** — [security@goceleris.dev](mailto:security@goceleris.dev).

Include a description of the issue, the loadgen version (the release tag, or
the `loadgen_version` field of a result), reproduction steps or a
proof-of-concept, and the impact you believe it has.

You will receive an **acknowledgement within 72 hours**. We will keep you
informed as we triage, fix and disclose, and we credit reporters in the
release notes unless they prefer otherwise.

## Supported Versions

Only the latest release of loadgen receives security fixes. Pre-built
binaries and the Go module are cut from the same tag, so upgrade to the most
recent `v1.x` tag to remain covered.

## Scope

In scope:

- The HTTP/1.1, HTTP/2 (prior-knowledge and h2c upgrade), WebSocket and SSE
  client implementations and their response parsers
- TLS configuration handling (`-insecure`, custom `tls.Config`)
- The federation peer protocol
- The JSON result output consumed by orchestrators
- The release pipeline (workflows, published binaries and their provenance)

Out of scope:

- Vulnerabilities in the server under test
- Denial of service of a target caused by *intended* use of a load generator

## The celeris engine

Vulnerabilities in the HTTP server framework that loadgen is built to test
belong to the [goceleris/celeris](https://github.com/goceleris/celeris)
repository — see
[celeris/SECURITY.md](https://github.com/goceleris/celeris/blob/main/SECURITY.md)
for its supported-versions table and reporting channels.
