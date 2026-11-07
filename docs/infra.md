# Self-hosting osmsg

Runs osmsg continuously: Postgres store, REST API, replication worker, and Caddy for HTTPS plus the
static leaderboard.

## Stack

| Service | Image | Role |
| --- | --- | --- |
| `db` | `postgis/postgis:17-3.5-alpine` | Stats store |
| `api` | `ghcr.io/osgeonepal/osmsg-api:latest` | REST API |
| `worker` | `ghcr.io/osgeonepal/osmsg-worker:latest` | Replication worker |
| `caddy` | `caddy:2-alpine` | HTTPS proxy: API + leaderboard |

Frontend and API use two subdomains: `OSMSG_FRONTEND_DOMAIN` and `OSMSG_API_DOMAIN`.

## Disk

Keep growing data off the root disk. Attach a block volume at `/mnt/mnt`; the data volumes and the history
artifact live on it. Create the directories and swap before the first start:

```bash
sudo mkdir -p /mnt/mnt/osmsg/{pgdata,cache,data,artifact}
sudo fallocate -l 4G /mnt/mnt/swapfile && sudo chmod 600 /mnt/mnt/swapfile
sudo mkswap /mnt/mnt/swapfile && sudo swapon /mnt/mnt/swapfile
echo '/mnt/mnt/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

## DNS

Point both subdomains at the server as `A` records. On Cloudflare, set them DNS-only (grey cloud) so
Caddy's Let's Encrypt challenge reaches the host.

```text
osmsg.<domain>      A   <server-ip>
api.osmsg.<domain>  A   <server-ip>
```

## Configure and deploy

The frontend ships in this repo under `frontend/`, and Caddy serves it from `../frontend` relative to
the compose file. Clone the repo to `/opt/osmsg` so `infra/` and `frontend/` sit side by side. Copy
`.env.example` to `.env` and set the two domains and the ACME email.

```bash
sudo git clone https://github.com/osgeonepal/osmsg.git /opt/osmsg
cd /opt/osmsg
cp infra/.env.example infra/.env && $EDITOR infra/.env
sudo cp infra/osmsg.service /etc/systemd/system/ && sudo systemctl daemon-reload
sudo systemctl enable --now osmsg
```

Set `GA_MEASUREMENT_ID` in `infra/.env` to enable Google Analytics on the frontend; leave it empty to
serve the site without analytics. The `/mnt/mnt` bind paths in `infra/docker-compose.yml` match the
reference deployment's block volume; on a single-disk host, create those directories (above) or replace
the binds with plain named volumes.

Deploy updates with `git -C /opt/osmsg pull` (refreshes both the frontend and the compose files);
`infra/.env` and any `infra/docker-compose.override.yml` are gitignored and stay untouched.

`OSMSG_EXTRA_ARGS` runs every tick. Do not put `--last`, `--days`, `--update`, or `--url` there: the
worker adds `--update` and auto-selects granularity from the gap (day to hour to minute). Pinning
`--url minute` over a large gap crawls tens of thousands of files and fills the disk.

## Seed history, then follow live

Seed the last published month into Postgres, then let the worker follow live. Use the worker's store
params so resume state lines up.

```bash
cd /opt/osmsg/infra && docker compose up -d db
docker compose run --rm --entrypoint osmsg worker \
    --insert --start 2026-05-01 --end 2026-06-01 \
    --name stats --output-dir /var/lib/osmsg --cache-dir /var/cache/osmsg \
    --format psql --psql-dsn postgresql://osmsg:osmsg@db:5432/osmsg --all
docker compose up -d
```

Manifest max month: `.../osmsg-history/resolve/main/manifest.json`.

## Timers

```bash
sudo cp infra/osmsg-cache-prune.{service,timer} /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now osmsg-cache-prune.timer
```

The GitHub Actions workflow `.github/workflows/maintain-rollup.yml` publishes each finished month to
HuggingFace on the 4th. The box pulls it the next day:

```bash
sudo cp infra/osmsg-artifact-refresh.{service,timer} infra/osmsg-check.{service,timer} /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now osmsg-artifact-refresh.timer osmsg-check.timer
```

`osmsg-artifact-refresh` runs at 04:00 UTC on the 5th. It downloads the newest published month into the
local history artifact at `/mnt/mnt/osmsg/artifact`, restarts the API so it reads the new frontier, then
prunes the Postgres rows that history now covers, keeping 40 days beyond the frontier (`OSMSG_PRUNE_OVERLAP_DAYS`). The prune deletes in
batches of 50,000 changesets, each committed separately, so a run stopped by its timeout keeps the batches
it finished and the next run continues from there. Run it by hand with
`sudo systemctl start osmsg-artifact-refresh.service`.

The API reads history from HuggingFace by default. To serve it from the local artifact, add a
`docker-compose.override.yml` that mounts `/mnt/mnt/osmsg/artifact` read-only at `/artifact` in the `api`
service and sets `OSMSG_HISTORY_URL=/artifact`, `OSMSG_HASHTAG_CHANGESET=/artifact/hashtag_changeset.parquet`
and `OSMSG_USERS=/artifact/users.parquet`.

`osmsg-check` runs daily at 05:30 UTC and fills changesets whose edits arrived without their metadata,
fetching the metadata from the OSM API.

## Postgres timeouts

Timeouts are set through `PGOPTIONS`, which libpq applies to every connection DuckDB opens. The worker
waits at most 120 s for each lock, and the prune and check jobs also cap each statement below their
script timeout. An `options=` parameter in `OSMSG_PSQL_DSN` overrides `PGOPTIONS`. The `db` service sets
`client_connection_check_interval`, so Postgres cancels a query, including a lock wait, within about
10 s after its client exits.

## Operate

Follow the container logs, or pull new images and restart onto them:

```bash
journalctl -u osmsg -f
cd /opt/osmsg/infra && docker compose pull && docker compose up -d
```

Recover a full root disk (cache or images landed on root). This keeps the `pgdata` volume:

```bash
systemctl stop osmsg && cd /opt/osmsg/infra && docker compose down
docker volume rm infra_osmsg-cache infra_osmsg-data
docker image prune -af && docker builder prune -af
```

## API

All-time hashtag stats, combining the published history rollup with the live tail at query time (see
`docs/rollups.md`).

```text
GET /health
GET /api/v2/hashtag/{hashtag}/summary
GET /api/v2/hashtag/{hashtag}/leaderboard[?page=N&page_size=N&sort=<column>&order=asc|desc&q=<name>&detail=true]
GET /api/v2/hashtag/{hashtag}/user/{uid}
GET /api/v2/hashtag/{hashtag}/tags[?limit=N]
GET /api/v2/hashtag/{hashtag}/editors
GET /api/v2/hashtag/{hashtag}/hashtags[?limit=N]
GET /api/v2/hashtag/{hashtag}/trends[?interval=day|week|month]
GET /api/v2/hashtag/{hashtag}/map[?limit=N]
GET /api/v2/global/{summary,leaderboard,user/{uid},editors,tags,trending}[?window=1h|24h|7d|30d]
GET /docs/swagger
```

`{hashtag}` is matched as a prefix (`hotosm` covers `#hotosm-project-1`, `#hotosm-fanclub`, and so on),
deduped by changeset so a changeset carrying two matching hashtags counts once. Pass several
comma-separated (`/hashtag/hotosm,osmnepal/summary`) to scope to their union. Every hashtag endpoint
accepts an optional half-open `[start, end)` UTC window as `start` and `end` (ISO 8601); omit both for
all-time. A range before the published frontier reads only the history rollup, a range after it reads
only the live tail, and a straddling range reads both. `exact=true` matches whole hashtags.

- `summary` returns totals: users, changesets, the element breakdown and map_changes.
- `leaderboard` returns one page of contributors, sorted, searched and paged on the server. `sort` is one
  of `map_changes`, `changesets`, `created`, `modified`, `deleted` or `name`. Per-user tags, editors and
  hashtags are attached only with `detail=true`.
- `user/{uid}` returns one contributor's tags, editors and hashtags.
- `tags` is the key/value breakdown and `editors` the editor breakdown.
- `hashtags` lists the other hashtags used on the matched changesets, ranked by contributors.
- `trends` buckets changesets, users and map_changes by day, week or month.
- `map` returns a GeoJSON FeatureCollection of changeset centroids, up to `limit`. Its history side reads
  the rollup's `lon`/`lat` columns.

The `global` endpoints cover all of OSM over a recent window, given as `window` or as `start` and `end`,
up to `OSMSG_GLOBAL_MAX_DAYS` (30 by default).

## Run the API standalone (without compose)

```bash
uv run osmsg --last day --format psql --psql-dsn "$DATABASE_URL" --name api_last_day
uv run --group api litestar --app api.app:app run --host 0.0.0.0 --port 8000
```

## Volumes

| Volume | Contents |
| --- | --- |
| `pgdata` | Postgres data |
| `osmsg-data` | DuckDB state files + parquet output |
| `osmsg-cache` | Downloaded replication diff cache |
| `caddy-data` | TLS certificates |
