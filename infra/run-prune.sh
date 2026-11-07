#!/usr/bin/env bash
# Delete Postgres rows now covered by published history. Runs the osmsg CLI in the compose network
# (to reach the db service); a no-op until the frontier advances after a maintain publish.
set -euo pipefail

cd /opt/osmsg/infra

dsn="${OSMSG_PSQL_DSN:-postgresql://osmsg:osmsg@db:5432/osmsg}"
repo="${OSMSG_HISTORY_REPO:-kshitijrajsharma/osmsg-history}"
# Keep ~a month beyond the frontier so global recent-window queries (<=30d) retain their pre-frontier days.
overlap_days="${OSMSG_PRUNE_OVERLAP_DAYS:-40}"
prune_timeout="${OSMSG_PRUNE_TIMEOUT:-30m}"
run_name="osmsg-prune-run"

docker rm -f "${run_name}" >/dev/null 2>&1 || true

echo "[prune] pruning Postgres covered by ${repo}, keeping ${overlap_days}d beyond the frontier (timeout ${prune_timeout})"
set +e
timeout --signal=TERM --kill-after=2m "${prune_timeout}" \
  docker compose run --rm --name "${run_name}" -e PGOPTIONS="-c lock_timeout=120s -c statement_timeout=25min" --entrypoint osmsg worker \
    maintain prune-pg --psql-dsn "${dsn}" --history-url "hf://datasets/${repo}" --overlap-days "${overlap_days}"
rc=$?
set -e

if [ "${rc}" -ne 0 ]; then
  echo "[prune] exited rc=${rc}; removing any lingering ${run_name} container"
  docker rm -f "${run_name}" >/dev/null 2>&1 || true
fi
exit "${rc}"
