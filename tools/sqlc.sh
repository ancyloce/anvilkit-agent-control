#!/bin/sh
# Control's data-access generation (A06): sqlc 1.27.0 renders
# internal/adapters/postgres/sqlc from the reviewed queries
# (internal/adapters/postgres/queries) and the service-owned schema
# (internal/migrate/sql, the migrations the migration Job applies).
#
#   sh tools/sqlc.sh            # regenerate in place
#   sh tools/sqlc.sh --check    # regenerate into a scratch tree and fail on drift
#
# sqlc is taken from GOPATH/bin or PATH and must report the pinned version
# (go install github.com/sqlc-dev/sqlc/cmd/sqlc@v1.27.0). The parent
# repository's tools/generate-contracts.py delegates its sqlc step here.
set -eu
ROOT=$(cd "$(dirname "$0")/.." && pwd)
PIN=1.27.0
OUT=internal/adapters/postgres/sqlc
SQLC="$(go env GOPATH)/bin/sqlc"
if [ ! -x "$SQLC" ]; then
  SQLC=$(command -v sqlc 2>/dev/null || true)
fi
if [ -z "$SQLC" ]; then
  echo "FAIL: sqlc is not installed (go install github.com/sqlc-dev/sqlc/cmd/sqlc@v$PIN)" >&2
  exit 1
fi
VERSION=$("$SQLC" version 2>&1 | sed -n 's/.*v\([0-9][0-9.]*\).*/\1/p' | head -1)
if [ "$VERSION" != "$PIN" ]; then
  echo "FAIL: sqlc at $SQLC reports '$VERSION'; pinned $PIN" >&2
  exit 1
fi
if [ "${1:-}" != "--check" ]; then
  (cd "$ROOT" && "$SQLC" generate -f sqlc.yaml)
  echo "generated: $OUT"
  exit 0
fi
# The check mirrors the configuration, the queries and the schema into a
# scratch tree (sqlc resolves paths relative to its configuration file) so
# nothing in the checkout is touched, then compares the rendered files.
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/internal/adapters/postgres" "$WORK/internal/migrate"
cp "$ROOT/sqlc.yaml" "$WORK/sqlc.yaml"
cp -R "$ROOT/internal/adapters/postgres/queries" "$WORK/internal/adapters/postgres/queries"
cp -R "$ROOT/internal/migrate/sql" "$WORK/internal/migrate/sql"
(cd "$WORK" && "$SQLC" generate -f sqlc.yaml)
if diff -r "$WORK/$OUT" "$ROOT/$OUT" >"$WORK/diff.txt" 2>&1; then
  echo "sqlc check: 0 differences"
  exit 0
fi
echo "FAIL sqlc check: $OUT differs from the generated output" >&2
sed 's/^/  /' "$WORK/diff.txt" | head -50 >&2
exit 1
