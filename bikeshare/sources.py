"""Extract layer: GBFS (discovery-first) + Open-Meteo weather.

Everything here returns raw payloads untouched. Parsing and typing happen in
silver so we can always replay from bronze if a transform has a bug.
"""
from __future__ import annotations

import logging
import time

import requests

from .config import SETTINGS, Settings

log = logging.getLogger(__name__)


def http_get_json(url: str, params: dict | None = None, s: Settings = SETTINGS) -> dict:
    """GET with timeout + exponential backoff. Retries on network errors, 429 and 5xx."""
    delay = 2.0
    for attempt in range(1, s.http_retries + 1):
        try:
            r = requests.get(url, params=params, timeout=s.http_timeout_seconds,
                             headers={"User-Agent": "xyz-bikeshare-pipeline/1.0"})
            if r.status_code == 429 or r.status_code >= 500:
                raise requests.HTTPError(f"retryable status {r.status_code}")
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, ValueError) as exc:
            if attempt == s.http_retries:
                raise
            log.warning("GET %s failed (%s), retry %d in %.0fs", url, exc, attempt, delay)
            time.sleep(delay)
            delay *= 2
    raise RuntimeError("unreachable")


def discover_feeds(s: Settings = SETTINGS) -> dict[str, str]:
    """Read gbfs.json and return {feed_name: url}. Source of truth for URLs."""
    doc = http_get_json(s.gbfs_discovery_url, s=s)
    data = doc.get("data", {})
    lang = data.get(s.gbfs_language) or next(iter(data.values()))  # fall back to first language
    feeds = {f["name"]: f["url"] for f in lang["feeds"]}
    for new_name, old_name in s.feed_aliases.items():           # GBFS 3.0 compatibility
        if new_name in feeds and old_name not in feeds:
            feeds[old_name] = feeds[new_name]
    return feeds


def fetch_feed(url: str, s: Settings = SETTINGS) -> dict:
    return http_get_json(url, s=s)


def fetch_weather(past_days: int = 2, s: Settings = SETTINGS) -> dict:
    """Hourly observed (past_days) + short forecast for central DC, timestamps in UTC."""
    params = {
        "latitude": s.weather_lat,
        "longitude": s.weather_lon,
        "hourly": ",".join(s.weather_vars),
        "past_days": past_days,
        "forecast_days": 1,
        "timezone": "GMT",
    }
    return http_get_json(s.weather_url, params=params, s=s)
