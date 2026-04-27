"""Package-level smoke tests."""

from __future__ import annotations

from osmsg.__version__ import __version__


def test_version_is_populated():
    assert isinstance(__version__, str)
    assert __version__


def test_package_modules_import():
    from osmsg import (  # noqa: F401
        auth,
        boundary,
        cli,
        db,
        export,
        fetch,
        geofabrik,
        handlers,
        models,
        pipeline,
        replication,
        tm,
        ui,
        workers,
    )
