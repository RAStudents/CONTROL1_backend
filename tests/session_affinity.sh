#!/usr/bin/env bash
set -euo pipefail
output=$(mktemp)
trap 'rm -f "$output"' EXIT
# The test proxy deliberately resets session state after every transaction.
# A mislabeled transaction-pool route must fail the actual affinity probe.
docker compose --profile contract up --build -d --wait pgbouncer
if docker compose run --rm --no-deps -e PGHOST=pgbouncer -e PGPORT=6432 \
    -e DATABASE_SESSION_MODE=session api create-account >"$output" 2>&1; then
    echo 'Transaction-pool route was incorrectly accepted' >&2
    exit 1
fi
if ! grep -q 'PostgreSQL session affinity check failed' "$output"; then
    cat "$output" >&2
    echo 'Proxy rejected for an unexpected reason; affinity regression is not proven' >&2
    exit 1
fi
if docker compose run --rm --no-deps -e DATABASE_SESSION_MODE=transaction \
    api create-account >"$output" 2>&1; then
    echo 'Transaction session mode was incorrectly accepted' >&2
    exit 1
fi
grep -q 'set DATABASE_SESSION_MODE to direct or session' "$output"
echo 'Session affinity passed: actual transaction pool and forbidden configuration are rejected'
