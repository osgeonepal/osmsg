# Roadmap

osmsg (OpenStreetMap Stats Generator) turns OSM history into per-user, per-hashtag statistics. It started
as a small command-line tool to monitor mapathons at the user level and has grown into a CLI, a Python
library, an API, and a hosted leaderboard.

## Background

- 2022: development started, focused on user-level hashtag statistics for mapathons.
- 2024: development paused.
- 2026: development resumed, supported by an OpenStreetMap Foundation microgrant, with contributors from
  OSGeo Nepal and OSM Nepal.

## Shipped

- Hashtag user-stats leaderboard aggregation (summary, leaderboard, tags, editors).
- Cloud-native history: the full OSM history is published as time-partitioned parquet on Hugging Face and
  read remotely, so past windows are served without re-downloading diffs.
- Hybrid history read: covered months come from the published dataset, the recent tail from live diffs,
  merged at the frontier.
- Two-minute replication with automatic granularity handoff (day to hour to minute) as the backlog shrinks.
- Storage choices: DuckDB and Postgres, plus parquet, csv, json, and markdown exports.
- API (v2) with time windows, multi-hashtag union, trending hashtags, per-editor stats, and a map endpoint.
- Web leaderboard UI and a Windows desktop application.
- Exact-match hashtag search alongside prefix search.
- Monthly dataset maintenance (`osmsg maintain`) run from GitHub Actions.
- Reliability: a diff that fails to parse aborts the run before the resume position advances, `/health`
  reports the database state, and timezone-less API query parameters are read as UTC.
- Packaging safeguard: CI and the release workflow verify that the built wheel is complete and importable
  before publish.

## Planned

- Distribution: publish to conda-forge and a Homebrew tap.
- Documentation: a generic guide to deploying on your own infrastructure, written for any server
  layout.
- Compare hashtags in the UI and a live tracking view.
- Performance work for the largest hashtags.

## Get involved

- Star the repository and open issues or pull requests.
- Join the OSGeo Nepal community.
- Contact: info@osgeonepal.org.
