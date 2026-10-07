"""
Offline stand-in for the claims REST API so the ingestion notebook can practise
pagination, retries, timeouts and watermarks without network access.

    from fake_api import ClaimsAPI
    api = ClaimsAPI(fail_every=7, timeout_every=11)
    resp = api.get(page=1, limit=500)

The transport is deterministic: call N fails according to fixed rules, so every run
of the notebook exercises the same retry paths.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

import requests
from requests.models import Response

from paths import LANDING

BASE_URL = "http://claims-api.internal/v1/claims"


class ClaimsAPI:
    """Pages are served from data/landing/api/claims_page_*.json."""

    def __init__(self, fail_every: int = 0, timeout_every: int = 0, latency_ms: int = 0):
        self.pages = []
        for path in sorted(Path(LANDING).glob("claims_page_*.json")):
            self.pages.append(json.loads(path.read_text()))
        self.fail_every = fail_every
        self.timeout_every = timeout_every
        self.latency_ms = latency_ms
        self.calls = 0
        self.failures_injected = 0

    # -- transport ---------------------------------------------------------------
    def _simulate(self) -> None:
        self.calls += 1
        if self.latency_ms:
            time.sleep(self.latency_ms / 1000)
        if self.timeout_every and self.calls % self.timeout_every == 0:
            self.failures_injected += 1
            raise requests.exceptions.Timeout(f"read timeout after 30s (call {self.calls})")
        if self.fail_every and self.calls % self.fail_every == 0:
            self.failures_injected += 1
            status = 429 if self.calls % 2 else 503
            raise _http_error(status)

    def get(self, page: int = 1, limit: int = 500, since: str | None = None,
            headers: dict | None = None, timeout: int = 30) -> Response:
        self._simulate()
        if page < 1 or page > len(self.pages):
            return _json_response({"page": page, "limit": limit, "total": 0, "data": []})
        payload = self.pages[page - 1]
        records = payload["data"]
        if since:
            records = [r for r in records if str(r.get("updated_at", "")) > since]
        return _json_response({"page": page, "limit": limit, "total": payload["total"],
                               "generated_at": payload["generated_at"], "data": records[:limit]})

    def iter_records(self, limit: int = 500, since: str | None = None):
        """Generator version - no retries, caller decides what to do with failures."""
        page = 1
        while True:
            resp = self.get(page=page, limit=limit, since=since)
            batch = resp.json()["data"]
            if not batch:
                return
            yield from batch
            page += 1


def _json_response(payload: dict, status: int = 200) -> Response:
    resp = Response()
    resp.status_code = status
    resp.url = BASE_URL
    resp.headers["Content-Type"] = "application/json"
    resp._content = json.dumps(payload).encode()
    return resp


def _http_error(status: int) -> requests.exceptions.HTTPError:
    resp = _json_response({"error": "service unavailable" if status >= 500 else "rate limited"}, status)
    return requests.exceptions.HTTPError(f"{status} from {BASE_URL}", response=resp)
