#!/usr/bin/env bash
# Backfill 'stub' changesets (edits captured but metadata missing) from the OSM API. Runs inside the live
# worker container so it uses the deployed osmsg build; idempotent and only touches closed changesets.
set -euo pipefail

dsn="${OSMSG_PSQL_DSN:-postgresql://osmsg:osmsg@db:5432/osmsg}"

echo "[check] backfilling stub changesets"
# timeout runs inside the container: systemd stopping the exec client alone leaves the process running.
exec docker exec -e PGOPTIONS="-c lock_timeout=120s -c statement_timeout=20min" infra-worker-1 \
  timeout --kill-after=1m 25m osmsg maintain check --psql-dsn "${dsn}" --fix
