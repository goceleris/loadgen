#!/usr/bin/env sh
# Every hand-written version stamp in this module must agree: the
# fallbackVersion constant in version.go (what a (devel) or test build
# reports, and what probatorium records for such a build) and the TAG in the
# README's download example. With an argument (vX.Y.Z or X.Y.Z) each must
# equal it; without, they must equal each other. CI runs the second form on
# every PR; the Release workflow runs the first before it creates a tag.
set -eu
cd "$(dirname "$0")/.."
want="${1:-}"; want="${want#v}"
const=$(grep -E '^const fallbackVersion = "[0-9]+\.[0-9]+\.[0-9]+[^"]*"$' version.go | sed -E 's/.*"([^"]+)".*/\1/')
readme=$(grep -E '^TAG=v[0-9]+\.[0-9]+\.[0-9]+' README.md | sed -E 's/^TAG=v//')
[ -n "$const" ] || { echo "version.go: no 'const fallbackVersion = \"X.Y.Z\"' line" >&2; exit 1; }
[ "$(printf '%s\n' "$readme" | wc -l | tr -d ' ')" = 1 ] || { echo "README.md: want exactly one TAG=vX.Y.Z line, got: $readme" >&2; exit 1; }
[ -n "$want" ] || want="$const"
case "$want" in
  *[!0-9.a-z-]*|"") echo "version '$want' is not X.Y.Z" >&2; exit 1;;
esac
rc=0
for pair in "version.go fallbackVersion:$const" "README.md TAG:$readme"; do
  name=${pair%%:*}; got=${pair#*:}
  if [ "$got" = "$want" ]; then echo "  ok        $name $got"; else echo "  MISMATCH  $name carries $got, want $want"; rc=1; fi
done
[ $rc -eq 0 ] && echo "release stamps agree: $want" || echo "fix: set both stamps to $want (sed -i 's/fallbackVersion = \".*\"/fallbackVersion = \"$want\"/' version.go; sed -i 's/^TAG=v.*/TAG=v$want/' README.md)" >&2
exit $rc
