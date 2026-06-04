#!/bin/sh
set -e
SCHEDULE="${OSMSG_SCHEDULE:-*/2 * * * *}"
echo "$SCHEDULE /app/.venv/bin/python -m osmsg._tick" > /app/crontab
echo "[worker] schedule: $SCHEDULE"
echo "[worker] initial tick"
/app/.venv/bin/python -m osmsg._tick || echo "[worker] initial tick exit=$?"
exec /usr/local/bin/supercronic /app/crontab
