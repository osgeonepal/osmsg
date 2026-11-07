# Way length (`l`) and the history backfill

osmsg records the length of open ways per tag. Each tag in the `changeset_stats.tags` struct carries
`l`, the way length in metres (`STRUCT(k, v, c, m, l)`). The API `/tags` endpoint returns `length_m`
per tag and the leaderboard UI shows it as a kilometre badge.

## What gets a length

A way gets a length when it is **open** (its first and last node references differ) and on **create**.
The length is the haversine distance along the way's nodes, attached to every tag of that way. Closed
ways (buildings, areas, roundabouts) don't get a length. A computed length above `MAX_WAY_LENGTH_M`
(2,000 km) is dropped as a geometry error (for example an early node first uploaded at (0,0) and later
corrected).

Geometry source: each node is measured at its first-seen coordinates (the version-1 location in the
history conversion, the first location osmium indexes on the live path), so a way is measured with
near-creation-time geometry. On a sample of real ways this sits within about 2% of the exact
creation-time length in aggregate.

## Accuracy: length is a lower bound

Measuring an open way needs the coordinates of every node it references. A diff carries coordinates
only for nodes touched in that diff, so a way that connects to a pre-existing node (a junction it was
drawn onto) has an unresolved node. osmium raises `InvalidLocationError` for the whole way and it is
skipped, contributing zero length. The reported length is therefore a lower bound of the true
geometry length.

The gap depends on how much new geometry connects into existing data. Remote greenfield lines, whose
nodes are all new in the same diff, are measured almost completely; urban lines wired into existing
streets lose more. Against full-geometry tools such as ohsome, osmsg highway kilometres run below
their road length, and by a wider margin for urban work. The difference follows from the lower-bound
definition above.

The history conversion in the next section resolves pre-existing nodes, because it reads the
coordinates of every node in the history file. The live tick and the monthly `maintain month` append
both read diffs and share the lower-bound behaviour.

## Live path

The worker computes length on every tick: it applies changefiles with node locations on, and measures
open-way creates whose nodes are present in the same diff. A way whose nodes come from an earlier diff
lacks a location in the current diff and is skipped.

## History conversion

`osmsg maintain convert` (and `convert()` in `osmsg/maintain/convert.py`) measures open-way lengths
for the published history in two stages. The streaming stage writes the version-1 coordinates of every
node in the history file, independent of the window, and the node references of every open way created
in the window, to parquet shards. The aggregation stage joins those references to the coordinates in
DuckDB and sums the haversine distance per way. It applies the same rules as the live path: a way with
a node that lacks coordinates is skipped, and a length above `MAX_WAY_LENGTH_M` is dropped.

`--parts N` splits the history file into N parts at PBF blob boundaries and streams each part in its
own process. The streaming stage does not build a node-location index, so the parts are independent; the join reads the shards of all parts together, which measures a way whose nodes were
streamed by a different part.

The command takes the history file, the changeset dump, the window start and end (`YYYY-MM-DD`), and
the work directory as positional arguments. The datasets land in `<work_dir>/out`. Run it on a large
machine:

```bash
osmsg maintain convert history-latest.osh.pbf changesets-latest.osm.bz2 2005-01-01 2026-06-01 \
  /data/convert --parts 24
```

Then republish the two datasets and the rollup to HuggingFace, and write the manifest with
`osmsg maintain publish`.

### Cost

- Disk: the work directory holds the raw shards, including one coordinate row per node in the planet,
  and DuckDB spills the aggregation to `<work_dir>/out/duckdb_tmp`, capped at 680 GB
  (`DUCKDB_MAX_TEMP`). Keep the work directory on a large volume, separate from the OS disk.
- Memory and CPU: the aggregation runs with a 48 GB DuckDB memory limit and 32 threads
  (`DUCKDB_MEMORY_LIMIT`, `DUCKDB_THREADS` in `osmsg/maintain/convert.py`).
- Time: the streaming stage scales with `--parts` up to the number of cores. Plan for hours on the full
  planet, plus aggregation and export.

## Upgrading a store that carries `len_m`

The tag field is `l`. A reader fails on data whose tag field is still named `len_m`, so the data and
the images change together, in this order:

1. Rebuild and republish the rollup, changefiles, and changesets so they carry `l`.
2. Migrate Postgres: run `docs/migrate_pg_len_to_l.sql` once, which renames `len_m` to `l` in the
   `osmsg_tag` composite type. A fresh store creates the type with `l`.
3. Deploy the worker and API images that read `l`.
