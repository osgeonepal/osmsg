"""Pluggable exporters consuming canonical query rows."""

from .csv import to_csv
from .json import to_json
from .markdown import summary_markdown, table_markdown
from .parquet import to_parquet
from .psql import to_psql

EXPORT_FORMATS = ("parquet", "csv", "json", "markdown", "psql")

__all__ = [
    "EXPORT_FORMATS",
    "summary_markdown",
    "table_markdown",
    "to_csv",
    "to_json",
    "to_parquet",
    "to_psql",
]
