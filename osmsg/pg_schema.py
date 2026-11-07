# Native tag type for changeset_stats.tags, created before the schema when absent. A plain CREATE TYPE
# because DuckDB's postgres_execute runs a DO block in a read-only transaction.
from .stats import PG_TAG_TYPE

PG_TAG_TYPE_EXISTS_SQL = f"SELECT 1 FROM pg_type WHERE typname = '{PG_TAG_TYPE}'"
PG_TAG_TYPE_SQL = f"CREATE TYPE {PG_TAG_TYPE} AS (k text, v text, c bigint, m bigint, l double precision)"

PG_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS users (
    uid      BIGINT PRIMARY KEY,
    username TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS changesets (
    changeset_id BIGINT PRIMARY KEY,
    uid          BIGINT NOT NULL REFERENCES users(uid),
    created_at   TIMESTAMPTZ,
    hashtags     TEXT[],
    editor       TEXT,
    min_lon      DOUBLE PRECISION,
    min_lat      DOUBLE PRECISION,
    max_lon      DOUBLE PRECISION,
    max_lat      DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_changesets_created_at ON changesets USING BTREE (created_at);
CREATE INDEX IF NOT EXISTS idx_changesets_uid_created ON changesets USING BTREE (uid, created_at);
CREATE INDEX IF NOT EXISTS idx_changesets_hashtags ON changesets USING GIN (hashtags);
CREATE INDEX IF NOT EXISTS idx_changesets_editor ON changesets USING BTREE (editor);
CREATE INDEX IF NOT EXISTS idx_changesets_bbox ON changesets USING GIST (
    box(point(min_lon, min_lat), point(max_lon, max_lat))
);
CREATE TABLE IF NOT EXISTS changeset_stats (
    changeset_id   BIGINT NOT NULL REFERENCES changesets(changeset_id),
    seq_id         BIGINT NOT NULL,
    uid            BIGINT NOT NULL REFERENCES users(uid),
    nodes_created  INTEGER DEFAULT 0,
    nodes_modified INTEGER DEFAULT 0,
    nodes_deleted  INTEGER DEFAULT 0,
    ways_created   INTEGER DEFAULT 0,
    ways_modified  INTEGER DEFAULT 0,
    ways_deleted   INTEGER DEFAULT 0,
    rels_created   INTEGER DEFAULT 0,
    rels_modified  INTEGER DEFAULT 0,
    rels_deleted   INTEGER DEFAULT 0,
    poi_created    INTEGER DEFAULT 0,
    poi_modified   INTEGER DEFAULT 0,
    tags           {PG_TAG_TYPE}[],
    PRIMARY KEY (seq_id, changeset_id)
);
CREATE INDEX IF NOT EXISTS idx_changeset_stats_uid ON changeset_stats USING BTREE (uid);
CREATE INDEX IF NOT EXISTS idx_changeset_stats_changeset_id ON changeset_stats USING BTREE (changeset_id);
CREATE TABLE IF NOT EXISTS changeset_hashtag (
    hashtag      TEXT NOT NULL,
    changeset_id BIGINT NOT NULL REFERENCES changesets(changeset_id) ON DELETE CASCADE,
    created_at   TIMESTAMPTZ,
    PRIMARY KEY (hashtag, changeset_id)
);
CREATE INDEX IF NOT EXISTS idx_changeset_hashtag_lookup ON changeset_hashtag
    USING BTREE (hashtag COLLATE "C", created_at);
CREATE INDEX IF NOT EXISTS idx_changeset_hashtag_changeset_id ON changeset_hashtag USING BTREE (changeset_id);
CREATE TABLE IF NOT EXISTS state (
    source_url  TEXT PRIMARY KEY,
    last_seq    BIGINT NOT NULL,
    last_ts     TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL
);
"""
