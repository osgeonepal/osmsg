"""Advancing the published manifest must never overwrite it from a failed read."""

import pytest
import requests

from osmsg.maintain import manifest


class _Response:
    def __init__(self, status_code, body=None):
        self.status_code = status_code
        self._body = body

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}")

    def json(self):
        return self._body


def _stub(monkeypatch, response):
    uploads = []
    monkeypatch.setattr(manifest.requests, "get", lambda url, timeout: response)
    monkeypatch.setattr(manifest, "_upload", lambda repo, path: uploads.append(path.read_text()))
    return uploads


def test_bump_manifest_raises_on_server_error_without_uploading(monkeypatch):
    uploads = _stub(monkeypatch, _Response(503))
    with pytest.raises(requests.HTTPError):
        manifest.bump_manifest("org/repo", "2026-11")
    assert uploads == []


def test_bump_manifest_keeps_min_month_when_advancing(monkeypatch):
    published = {"schema_version": 1, "min_month": "2005-04", "max_month": "2026-10"}
    uploads = _stub(monkeypatch, _Response(200, dict(published)))
    result = manifest.bump_manifest("org/repo", "2026-11")
    assert result["min_month"] == "2005-04" and result["max_month"] == "2026-11"
    assert len(uploads) == 1
