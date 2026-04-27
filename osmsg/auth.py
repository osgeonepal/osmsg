"""OAuth 2.0 cookie client for Geofabrik internal download server.

Mirrors https://github.com/geofabrik/sendfile_osm_oauth_protector
"""

from __future__ import annotations

import re
import urllib.parse

import requests

from ._http import session as shared_session
from .exceptions import GeofabrikAuthError

DEFAULT_OSM_HOST = "https://www.openstreetmap.org"
DEFAULT_CONSUMER_URL = "https://osm-internal.download.geofabrik.de/get_cookie"
CSRF_RE = re.compile(r'name="csrf-token"\s+content="([^"]+)"')


def _csrf(html: str) -> str:
    m = CSRF_RE.search(html)
    if m is None:
        raise GeofabrikAuthError("authenticity_token not found in OSM response")
    return m.group(1)


def get_geofabrik_cookie(
    username: str,
    password: str,
    osm_host: str = DEFAULT_OSM_HOST,
    consumer_url: str = DEFAULT_CONSUMER_URL,
) -> str:
    if not username or not password:
        raise GeofabrikAuthError("OSM username and password required")

    r = shared_session.post(f"{consumer_url}?action=get_authorization_url", timeout=30)
    if r.status_code != 200:
        raise GeofabrikAuthError(f"get_authorization_url returned HTTP {r.status_code}")
    payload = r.json()
    try:
        authz_url = payload["authorization_url"]
        state = payload["state"]
        redirect_uri = payload["redirect_uri"]
        client_id = payload["client_id"]
    except KeyError as exc:
        raise GeofabrikAuthError(f"missing field in authorization response: {exc}") from exc

    s = requests.Session()
    s.headers.update(shared_session.headers)

    r = s.get(f"{osm_host}/login?cookie_test=true", timeout=30)
    if r.status_code != 200:
        raise GeofabrikAuthError(f"GET /login returned HTTP {r.status_code}")

    r = s.post(
        f"{osm_host}/login",
        data={
            "username": username,
            "password": password,
            "referer": "/",
            "commit": "Login",
            "authenticity_token": _csrf(r.text),
        },
        allow_redirects=False,
        timeout=30,
    )
    if r.status_code != 302:
        raise GeofabrikAuthError(f"OSM login failed (HTTP {r.status_code}); check credentials")

    r = s.get(authz_url, allow_redirects=False, timeout=30)
    if r.status_code != 302:
        if r.status_code != 200:
            raise GeofabrikAuthError(f"GET authorize returned HTTP {r.status_code}")
        r = s.post(
            authz_url,
            data={
                "client_id": client_id,
                "redirect_uri": redirect_uri,
                "authenticity_token": _csrf(r.text),
                "state": state,
                "response_type": "code",
                "scope": "read_prefs",
                "nonce": "",
                "code_challenge": "",
                "code_challenge_method": "",
                "commit": "Authorize",
            },
            allow_redirects=False,
            timeout=30,
        )
        if r.status_code != 302:
            raise GeofabrikAuthError(f"POST authorize returned HTTP {r.status_code}")

    location = r.headers.get("location") or ""
    if "?" not in location:
        raise GeofabrikAuthError("authorization redirect missing query string")

    s.get(f"{osm_host}/logout", timeout=30)

    final_url = f"{location}&{urllib.parse.urlencode({'format': 'http'})}"
    r = shared_session.get(final_url, timeout=30)
    if r.status_code != 200 or not r.text.strip():
        raise GeofabrikAuthError(f"cookie exchange failed (HTTP {r.status_code})")
    return r.text.strip()
