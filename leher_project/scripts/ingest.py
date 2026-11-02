#!/usr/bin/env python3
"""Leher demo: pull 3 real keyless APIs + the farm registry into one raw lake.

Layout written (the same layout is used for S3 / a Unity Catalog Volume):

    <out>/source=<source>/date=<YYYY-MM-DD>/<source>_<timestamp>.json
    <out>/source=farms/date=<YYYY-MM-DD>/farms_<timestamp>.csv
    <out>/source=drone/farm_id=<id>/date=<YYYY-MM-DD>/flight_id=<id>/_manifest.json
    <out>/_manifests/date=<YYYY-MM-DD>/run_<timestamp>.json   # run bookkeeping

Sources (all keyless):
    open_meteo_weather     daily weather per farm (lat/lon)      api.open-meteo.com
    nasa_power_agro        rain / humidity / root-zone wetness   power.larc.nasa.gov
    planetary_computer_s2  Sentinel-2 scene metadata per farm    planetarycomputer...

The raw file is the API response plus a small envelope (`_meta` + per-farm
request context) so a day can be replayed without calling the API again.

Usage:
    python scripts/ingest.py --date 2026-10-15 --out data/raw
    python scripts/ingest.py --date 2026-10-15 --from-fixtures fixtures/raw
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
import sys
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests

IST = ZoneInfo("Asia/Kolkata")
WEATHER_URL = "https://api.open-meteo.com/v1/forecast"
AGRO_URL = "https://power.larc.nasa.gov/api/temporal/daily/point"
STAC_URL = "https://planetarycomputer.microsoft.com/api/stac/v1/search"

# One obs_date per run, so every request asks for exactly one day.
WEATHER_DAILY = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "wind_speed_10m_max",
    "relative_humidity_2m_mean",
    "et0_fao_evapotranspiration",
]
AGRO_PARAMS = ["T2M", "PRECTOTCORR", "RH2M", "GWETROOT"]
SATELLITE_LOOKBACK_DAYS = 7  # Sentinel-2 revisit is ~5 days, so search a week back
RETRYABLE_HTTP = {429, 500, 502, 503, 504}


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def utc_now() -> datetime:
    return datetime.now(UTC)


def stamp(dt: datetime) -> str:
    return dt.strftime("%Y%m%dT%H%M%SZ")


def request_json(
    method: str,
    url: str,
    *,
    params: dict | None = None,
    body: dict | None = None,
    attempts: int = 4,
    timeout: int = 30,
) -> dict:
    """GET/POST JSON with retries + exponential backoff (rate limits and 5xx)."""
    for attempt in range(attempts):
        try:
            resp = requests.request(method, url, params=params, json=body, timeout=timeout)
        except requests.RequestException as exc:
            if attempt == attempts - 1:
                raise RuntimeError(f"{url} unreachable: {exc}") from exc
            time.sleep(2**attempt)
            continue
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code in RETRYABLE_HTTP and attempt < attempts - 1:
            time.sleep(2**attempt)
            continue
        raise RuntimeError(f"{url} -> HTTP {resp.status_code}: {resp.text[:300]}")
    raise RuntimeError(f"{url}: gave up after {attempts} attempts")


def load_farms(path: Path) -> list[dict]:
    if not path.exists():
        # On Databricks the job passes the synced workspace path; if the bundle
        # was slimmed down, fall back to the copy next to this script.
        bundled = Path(__file__).resolve().parent.parent / "fixtures" / "farms.csv"
        if not bundled.exists():
            raise SystemExit(f"farm registry not found: {path} (or {bundled})")
        print(f"[ingest] {path} missing, using bundled {bundled}", file=sys.stderr)
        path = bundled
    with path.open(newline="") as fh:
        farms = list(csv.DictReader(fh))
    for farm in farms:
        farm["lat"] = float(farm["lat"])
        farm["lon"] = float(farm["lon"])
        farm["area_ha"] = float(farm["area_ha"])
    if not farms:
        raise SystemExit(f"no farms in {path}")
    return farms


# --------------------------------------------------------------------------- #
# source connectors: each returns one per-farm envelope entry
# --------------------------------------------------------------------------- #
def fetch_weather(farm: dict, obs_date: date) -> dict:
    request = {
        "latitude": farm["lat"],
        "longitude": farm["lon"],
        "start_date": obs_date.isoformat(),
        "end_date": obs_date.isoformat(),
        "daily": ",".join(WEATHER_DAILY),
        "timezone": "Asia/Kolkata",
    }
    return {
        "farm_id": farm["farm_id"],
        "request": request,
        "response": request_json("GET", WEATHER_URL, params=request),
    }


def fetch_agro(farm: dict, obs_date: date) -> dict:
    ymd = obs_date.strftime("%Y%m%d")
    request = {
        "parameters": ",".join(AGRO_PARAMS),
        "community": "AG",
        "latitude": farm["lat"],
        "longitude": farm["lon"],
        "start": ymd,
        "end": ymd,
        "format": "JSON",
    }
    return {
        "farm_id": farm["farm_id"],
        "request": request,
        "response": request_json("GET", AGRO_URL, params=request),
    }


def fetch_satellite(farm: dict, obs_date: date) -> dict:
    """Search STAC for Sentinel-2 scenes over the farm; keep the fields we use.

    We do not download pixels here. The CV pipeline downloads scene assets for
    fields that need a fresh look; the unified conditions table only needs
    "was there a usable scene and how cloudy was it".
    """
    pad = 0.03  # ~3 km box around the farm centroid
    request = {
        "collections": ["sentinel-2-l2a"],
        "bbox": [farm["lon"] - pad, farm["lat"] - pad, farm["lon"] + pad, farm["lat"] + pad],
        "datetime": (
            f"{(obs_date - timedelta(days=SATELLITE_LOOKBACK_DAYS)).isoformat()}T00:00:00Z/"
            f"{obs_date.isoformat()}T23:59:59Z"
        ),
        "limit": 5,
        "sortby": [{"field": "properties.datetime", "direction": "desc"}],
    }
    payload = request_json("POST", STAC_URL, body=request)
    features = [
        {
            "id": item.get("id"),
            "collection": item.get("collection"),
            "datetime": item.get("properties", {}).get("datetime"),
            "cloud_cover": item.get("properties", {}).get("eo:cloud_cover"),
            "bbox": item.get("bbox"),
        }
        for item in payload.get("features", [])
    ]
    return {
        "farm_id": farm["farm_id"],
        "request": {k: request[k] for k in ("bbox", "datetime")},
        "response": {"type": "FeatureCollection", "features": features},
    }


SOURCES = {
    "weather": ("open_meteo_weather", fetch_weather),
    "agro": ("nasa_power_agro", fetch_agro),
    "satellite": ("planetary_computer_s2", fetch_satellite),
}


# --------------------------------------------------------------------------- #
# raw lake writes
# --------------------------------------------------------------------------- #
def write_source_file(
    out: Path, source: str, obs_date: date, entries: list[dict], fetched_at: datetime
) -> Path:
    target = out / f"source={source}" / f"date={obs_date.isoformat()}"
    target.mkdir(parents=True, exist_ok=True)
    path = target / f"{source}_{stamp(fetched_at)}.json"
    envelope = {
        "_meta": {
            "source": SOURCES[source][0],
            "obs_date": obs_date.isoformat(),
            "fetched_at": fetched_at.isoformat(),
            "api": {"weather": WEATHER_URL, "agro": AGRO_URL, "satellite": STAC_URL}[source],
        },
        "farms": entries,
    }
    path.write_text(json.dumps(envelope, separators=(",", ":")))
    return path


def write_farms_snapshot(out: Path, farms_csv: Path, obs_date: date, fetched_at: datetime) -> Path:
    target = out / "source=farms" / f"date={obs_date.isoformat()}"
    target.mkdir(parents=True, exist_ok=True)
    path = target / f"farms_{stamp(fetched_at)}.csv"
    shutil.copyfile(farms_csv, path)
    return path


def write_run_manifest(out: Path, obs_date: date, fetched_at: datetime, report: dict) -> Path:
    target = out / "_manifests" / f"date={obs_date.isoformat()}"
    target.mkdir(parents=True, exist_ok=True)
    path = target / f"run_{stamp(fetched_at)}.json"
    manifest = {
        "obs_date": obs_date.isoformat(),
        "started_at": fetched_at.isoformat(),
        "finished_at": utc_now().isoformat(),
        "sources": report,
    }
    path.write_text(json.dumps(manifest, indent=2))
    return path


# --------------------------------------------------------------------------- #
# modes
# --------------------------------------------------------------------------- #
def ingest_live(out: Path, obs_date: date, farms_csv: Path) -> int:
    farms = load_farms(farms_csv)
    started = utc_now()
    report: dict[str, dict] = {}

    # The pipelines attach Auto Loader to these prefixes. Create them even when a
    # source has no files yet (e.g. no drone flight this week) so the first run
    # does not fail on a missing path.
    for source in (*SOURCES, "drone"):
        (out / f"source={source}").mkdir(parents=True, exist_ok=True)

    registry_path = write_farms_snapshot(out, farms_csv, obs_date, started)
    report["farms"] = {"status": "ok", "farms": len(farms), "files": [str(registry_path)]}

    for source, (api_name, fetch) in SOURCES.items():
        rows, errors = [], []
        for farm in farms:
            try:
                rows.append(fetch(farm, obs_date))
            except Exception as exc:  # one bad farm must not sink the morning run
                errors.append(f"{farm['farm_id']}: {exc}")
                print(f"[ingest] {source} {farm['farm_id']} FAILED: {exc}", file=sys.stderr)
        if not rows:
            report[source] = {"status": "failed", "errors": errors}
            continue
        path = write_source_file(out, source, obs_date, rows, utc_now())
        report[source] = {
            "status": "ok" if not errors else "partial",
            "api": api_name,
            "farms": len(rows),
            "files": [str(path)],
            "errors": errors,
        }

    manifest = write_run_manifest(out, obs_date, started, report)
    ok = [s for s, r in report.items() if r["status"] in ("ok", "partial")]
    print(
        f"[ingest] {obs_date} -> {out}  ({len(ok)}/{len(report)} sources ok)  manifest={manifest}"
    )
    return 0 if "weather" in ok or "agro" in ok else 2


def copy_fixtures(fixtures_root: Path, out: Path, obs_date: date) -> int:
    """Replay a previously captured day: copy the raw files without any HTTP."""
    if not fixtures_root.exists():
        raise SystemExit(f"fixtures not found: {fixtures_root}")
    files = [
        p
        for p in fixtures_root.rglob("*")
        if p.is_file() and f"date={obs_date.isoformat()}" in p.parts and "_manifests" not in p.parts
    ]
    if not files:
        raise SystemExit(f"no fixtures for {obs_date} under {fixtures_root}")
    for src in files:
        dest = out / src.relative_to(fixtures_root)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(src, dest)
    print(f"[ingest] {obs_date} replayed {len(files)} raw files from {fixtures_root} -> {out}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", default=None, help="obs_date (default: today, Asia/Kolkata)")
    parser.add_argument("--out", default="data/raw", type=Path, help="raw lake root")
    parser.add_argument(
        "--farms", default="fixtures/farms.csv", type=Path, help="farm registry snapshot"
    )
    parser.add_argument(
        "--from-fixtures",
        default=None,
        type=Path,
        help="replay fixture raw files for --date instead of calling APIs",
    )
    args = parser.parse_args()

    obs_date = date.fromisoformat(args.date) if args.date else datetime.now(IST).date()
    if args.from_fixtures:
        return copy_fixtures(args.from_fixtures, args.out, obs_date)
    return ingest_live(args.out, obs_date, args.farms)


if __name__ == "__main__":
    sys.exit(main())
