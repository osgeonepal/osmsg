# Self-hosting osmsg

This guide covers running osmsg continuously on a server: a Postgres database, a Litestar REST API, and a worker that keeps OSM stats refreshed on a cron schedule.

## Stack overview

| Service | Image target | Role |
| --- | --- | --- |
| `db` | `postgres:17-alpine` | Persistent stats store |
| `api` | `Dockerfile → api` | Litestar REST API at `:8000` |
| `worker` | `Dockerfile → worker` | osmsg cron worker (supercronic) |

The worker bootstraps on first run (no existing state → `--last <period>`) and switches to `--update` automatically on subsequent ticks.

## Quick start : planet, every 2 minutes

```bash
docker compose up -d
curl 'http://localhost:8000/health'
curl 'http://localhost:8000/api/v1/user-stats?start=2026-05-07T00:00:00Z&end=2026-05-08T00:00:00Z&limit=20'
```

No `.env` needed : defaults are planet replication, `*/2 * * * *` schedule, bootstrap from last hour.

## Country mode (Geofabrik)

Create a `.env` file (copy `.env.example` and edit):

```bash
OSMSG_NAME=nepal
OSMSG_COUNTRY=nepal          # any Geofabrik region id
OSMSG_BOOTSTRAP=day          # first-run window
OSMSG_SCHEDULE=0 * * * *     # hourly
OSM_USERNAME=you
OSM_PASSWORD=secret
```

Then:

```bash
docker compose up -d
```

The worker fetches Nepal-specific replication diffs from Geofabrik.
Changesets are filtered to those whose bounding box intersects the Nepal polygon (auto-derived from the Geofabrik index).
A custom boundary GeoJSON can override this via `OSMSG_BOUNDARY`.

### Geofabrik credentials

Geofabrik sub-daily replication uses your OSM account credentials directly via OAuth 2.0.
Set `OSM_USERNAME` and `OSM_PASSWORD` in `.env` — no browser opt-in or separate Geofabrik registration required.

## Environment variables

All variables are optional; defaults target the planet at minute granularity.

| Variable | Default | Notes |
| --- | --- | --- |
| `OSMSG_NAME` | `stats` | DuckDB / output file basename |
| `OSMSG_URL` | `minute` | `minute`/`hour`/`day` shortcut or full replication URL. Ignored when `OSMSG_COUNTRY` is set |
| `OSMSG_COUNTRY` | _unset_ | Geofabrik region id (e.g. `nepal`). Needs `OSM_USERNAME`/`OSM_PASSWORD` |
| `OSMSG_BOOTSTRAP` | `hour` | First-run window: `hour`/`day`/`week`/`month`/`year` |
| `OSMSG_BOOTSTRAP_DAYS` | _unset_ | Exact day count for first-run bootstrap (alternative to `OSMSG_BOOTSTRAP`) |
| `OSMSG_BOUNDARY` | _unset_ | Path to a GeoJSON file. Overrides auto-derived country geometry |
| `OSMSG_SCHEDULE` | `*/2 * * * *` | supercronic cron expression for the worker tick |
| `DATABASE_URL` | (compose default) | libpq DSN; worker mirrors each tick to Postgres |
| `OSM_USERNAME` | _unset_ | OSM account username (Geofabrik auth) |
| `OSM_PASSWORD` | _unset_ | OSM account password (Geofabrik auth) |

## API endpoints

```
GET /health
GET /api/v1/user-stats?start=<ISO8601>&end=<ISO8601>[&hashtag=<tag>][&limit=N][&offset=N]
GET /docs           (Swagger UI)
```

## Populate all-time stats (backfill)

For a long historical backfill, run osmsg directly before starting the continuous worker.
The worker will resume from where the backfill left off.

**Example : all Nepal stats since 2012:**

```bash
docker compose up -d db    # start only the database

docker compose run --rm worker python -m osmsg \
    --name nepal \
    --country nepal \
    --start "2012-09-12" \
    --end "2026-01-01" \
    --format psql \
    --psql-dsn "postgresql://osmsg:osmsg@db:5432/osmsg"

docker compose up -d       # start api + worker; worker resumes from last backfill seq
```

The `state` table records the last processed sequence per source URL.
When the worker starts, it detects existing state and switches to `--update` automatically.

**Example : last 90 days then keep refreshing:**

```bash
OSMSG_BOOTSTRAP_DAYS=90 docker compose up -d
```

## Run the API standalone (without compose)

Push stats into Postgres first, then start litestar:

```bash
uv run osmsg --last day --format psql --psql-dsn "$DATABASE_URL" --name api_last_day
uv run --group api litestar --app api.app:app run --host 0.0.0.0 --port 8000
```

## Run as a systemd service

Drop the project on the server and let systemd manage the compose stack across reboots.

**1. Place files:**

```bash
mkdir -p /opt/osmsg
cp docker-compose.yml Dockerfile worker-entrypoint.sh /opt/osmsg/
cp .env.example /opt/osmsg/.env
# edit /opt/osmsg/.env with your values
```

**2. Create the unit file** at `/etc/systemd/system/osmsg.service`:

```ini
[Unit]
Description=osmsg stats stack
Requires=docker.service
After=docker.service network-online.target
Wants=network-online.target

[Service]
Type=simple
Restart=on-failure
RestartSec=10
WorkingDirectory=/opt/osmsg
EnvironmentFile=/opt/osmsg/.env
ExecStart=/usr/bin/docker compose up
ExecStop=/usr/bin/docker compose down
TimeoutStartSec=300
TimeoutStopSec=60

[Install]
WantedBy=multi-user.target
```

**3. Enable and start:**

```bash
systemctl daemon-reload
systemctl enable --now osmsg
```

**Useful commands:**

```bash
systemctl status osmsg
journalctl -u osmsg -f          # follow logs (all three containers)
systemctl restart osmsg         # pick up .env changes
systemctl stop osmsg            # brings the full stack down cleanly
```

> `EnvironmentFile=/opt/osmsg/.env` loads your env vars into the service environment.
> Docker Compose inherits them, so `${OSMSG_COUNTRY}` and friends resolve without a separate
> `--env-file` flag.

## Volumes

| Volume | Mount | Contents |
| --- | --- | --- |
| `pgdata` | `/var/lib/postgresql/data` | Postgres data directory |
| `osmsg-data` | `/var/lib/osmsg` | DuckDB state files + parquet output |
| `osmsg-cache` | `/var/cache/osmsg` | Downloaded replication diff cache |
