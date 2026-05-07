# Self-hosting osmsg

This guide covers running osmsg continuously on a server: a Postgres database, a Litestar REST API, and a worker that keeps OSM stats refreshed on a cron schedule.

## Stack overview

| Service | Image target | Role |
| --- | --- | --- |
| `db` | `postgres:17-alpine` | Persistent stats store |
| `api` | `Dockerfile → api` | Litestar REST API (internal, port 8000) |
| `worker` | `Dockerfile → worker` | osmsg cron worker (supercronic) |
| `caddy` | `caddy:2-alpine` | Reverse proxy — HTTP/HTTPS termination |

The worker bootstraps on first run (no existing state → `--last <period>`) and switches to `--update` automatically on subsequent ticks.

## Quick start

```bash
docker compose up -d
curl 'http://localhost/health'
curl 'http://localhost/api/v1/user-stats?start=2026-05-07T00:00:00Z&end=2026-05-08T00:00:00Z&limit=20'
```

No config needed: defaults are planet replication, `*/2 * * * *` schedule, bootstrap from last hour.

## Configuration

All deployment environment variables live in `infra/.env.example`.
Copy it to `/opt/osmsg/.env` and edit:

```bash
cp infra/.env.example /opt/osmsg/.env
$EDITOR /opt/osmsg/.env
```

| Variable | Default | Notes |
| --- | --- | --- |
| `OSMSG_DOMAIN` | `localhost` | Caddy server name — set to your domain for automatic HTTPS |
| `OSMSG_NAME` | `stats` | DuckDB / output file basename |
| `OSMSG_URL` | `minute` | `minute`/`hour`/`day` shortcut or full replication URL. Ignored when `OSMSG_COUNTRY` is set |
| `OSMSG_COUNTRY` | _unset_ | Geofabrik region id (e.g. `nepal`). Needs `OSM_USERNAME`/`OSM_PASSWORD` |
| `OSMSG_BOOTSTRAP` | `hour` | First-run window: `hour`/`day`/`week`/`month`/`year` |
| `OSMSG_BOOTSTRAP_DAYS` | _unset_ | Exact day count for first-run bootstrap (alternative to `OSMSG_BOOTSTRAP`) |
| `OSMSG_BOUNDARY` | _unset_ | GeoJSON path or Geofabrik region name — overrides auto-derived country geometry |
| `OSMSG_SCHEDULE` | `*/2 * * * *` | supercronic cron expression for the worker tick |
| `OSM_USERNAME` | _unset_ | OSM account username (Geofabrik auth) |
| `OSM_PASSWORD` | _unset_ | OSM account password (Geofabrik auth) |

### Geofabrik credentials

Geofabrik sub-daily replication uses your OSM account credentials directly via OAuth 2.0.
Set `OSM_USERNAME` and `OSM_PASSWORD` — no browser opt-in or separate Geofabrik registration required.

## Country mode example

```bash
# infra/.env (or /opt/osmsg/.env on the server)
OSMSG_NAME=nepal
OSMSG_COUNTRY=nepal
OSMSG_BOOTSTRAP=day
OSMSG_SCHEDULE=0 * * * *
OSM_USERNAME=you
OSM_PASSWORD=secret
```

```bash
docker compose up -d
```

The worker fetches Nepal-specific replication diffs from Geofabrik.
Changesets are filtered to those whose bounding box intersects the Nepal polygon.
Override with a GeoJSON file or a Geofabrik region name via `OSMSG_BOUNDARY`.

## Populate all-time stats (backfill)

Run osmsg directly before starting the continuous worker; it will resume from where the backfill left off.

**All Nepal stats since 2012 — then keep updating:**

```bash
docker compose up -d db    # start only the database

docker compose run --rm worker python -m osmsg \
    --name nepal \
    --country nepal \
    --start "2012-09-12" \
    --end "2026-01-01" \
    --format psql \
    --psql-dsn "postgresql://osmsg:osmsg@db:5432/osmsg"

docker compose up -d       # api + worker resume from last backfill seq
```

**Last 90 days then keep refreshing:**

```bash
OSMSG_BOOTSTRAP_DAYS=90 docker compose up -d
```

## API endpoints

```text
GET /health
GET /api/v1/user-stats?start=<ISO8601>&end=<ISO8601>[&hashtag=<tag>][&limit=N][&offset=N]
GET /docs           (Swagger UI)
```

## Run as a systemd service

**1. Place files on the server:**

```bash
mkdir -p /opt/osmsg
cp -r docker-compose.yml Dockerfile infra worker-entrypoint.sh /opt/osmsg/
cp infra/.env.example /opt/osmsg/.env
$EDITOR /opt/osmsg/.env     # set OSMSG_DOMAIN and other vars
```

**2. Install the unit file:**

```bash
cp infra/osmsg.service /etc/systemd/system/osmsg.service
```

**3. Enable and start:**

```bash
systemctl daemon-reload
systemctl enable --now osmsg
```

**Useful commands:**

```bash
systemctl status osmsg
journalctl -u osmsg -f          # follow logs (all containers)
systemctl restart osmsg         # pick up .env changes
systemctl stop osmsg            # brings the full stack down cleanly
```

> `EnvironmentFile=/opt/osmsg/.env` loads your env vars into the service environment.
> Docker Compose inherits them, so `${OSMSG_COUNTRY}` and friends resolve without a separate
> `--env-file` flag.

## Run the API standalone (without compose)

Push stats into Postgres first, then start litestar:

```bash
uv run osmsg --last day --format psql --psql-dsn "$DATABASE_URL" --name api_last_day
uv run --group api litestar --app api.app:app run --host 0.0.0.0 --port 8000
```

## Volumes

| Volume | Mount | Contents |
| --- | --- | --- |
| `pgdata` | `/var/lib/postgresql/data` | Postgres data directory |
| `osmsg-data` | `/var/lib/osmsg` | DuckDB state files + parquet output |
| `osmsg-cache` | `/var/cache/osmsg` | Downloaded replication diff cache |
| `caddy-data` | `/data` | Caddy TLS certificates |
