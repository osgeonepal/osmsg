#!/usr/bin/env bash
# Advance the local history artifact to the newest published month, then prune the Postgres rows it covers.
# The API restarts in between so a stale cached frontier never expects the pruned rows from Postgres.
set -euo pipefail

cd /opt/osmsg/infra

artifact_host="${OSMSG_ARTIFACT_DIR:-/mnt/mnt/osmsg/artifact}"
dsn="${OSMSG_PSQL_DSN:-postgresql://osmsg:osmsg@db:5432/osmsg}"
repo="${OSMSG_HISTORY_REPO:-kshitijrajsharma/osmsg-history}"
# Keep ~a month beyond the frontier so global recent-window queries (<=30d) always find their
# pre-frontier days in Postgres; anything older is served from published history.
overlap_days="${OSMSG_PRUNE_OVERLAP_DAYS:-40}"
# Hard cap so a hung prune cannot hold Postgres connections/locks and stall the live worker.
prune_timeout="${OSMSG_PRUNE_TIMEOUT:-30m}"
run_name="osmsg-prune-run"

echo "[artifact-refresh] advancing ${artifact_host} from ${repo}"
# Cap Xet download concurrency so its reconstruction buffers stay well within the container memory limit.
docker compose run --rm -e HF_XET_CLIENT_AC_MAX_DOWNLOAD_CONCURRENCY=4 -v "${artifact_host}:/artifact" --entrypoint osmsg worker \
  maintain refresh --artifact-dir /artifact --repo "${repo}"

echo "[artifact-refresh] reloading the API onto the advanced frontier"
docker compose restart api

echo "[artifact-refresh] pruning Postgres, keeping ${overlap_days}d beyond the frontier (timeout ${prune_timeout})"
if [ -n "$(docker ps -aq --filter "name=^${run_name}$")" ]; then docker rm -f "${run_name}"; fi
set +e
timeout --signal=TERM --kill-after=2m "${prune_timeout}" \
  docker compose run --rm --name "${run_name}" -e PGOPTIONS="-c lock_timeout=120s -c statement_timeout=25min" -v "${artifact_host}:/artifact" --entrypoint osmsg worker \
    maintain prune-pg --psql-dsn "${dsn}" --history-url /artifact --overlap-days "${overlap_days}"
rc=$?
set -e
if [ "${rc}" -ne 0 ]; then
  echo "[artifact-refresh] prune exited rc=${rc}; removing any lingering ${run_name} container"
  if [ -n "$(docker ps -aq --filter "name=^${run_name}$")" ]; then docker rm -f "${run_name}"; fi
fi
exit "${rc}"
