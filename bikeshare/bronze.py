"""Bronze: land every API response exactly as received, plus an envelope with
lineage metadata. Files are immutable and partitioned by feed and date, so the
whole pipeline can be rebuilt from here (same pattern as an ADLS/S3 landing
zone read by Auto Loader).

    data/bronze/<feed>/dt=YYYY-MM-DD/<feed>_<last_updated>_<ingested_epoch>.json
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

from .config import SETTINGS, Settings


def land(feed: str, payload: dict, source_url: str, ingested_at: float | None = None,
         s: Settings = SETTINGS) -> Path:
    ingested_at = ingested_at or time.time()
    last_updated = int(payload.get("last_updated") or ingested_at)
    dt = datetime.fromtimestamp(last_updated, tz=timezone.utc).strftime("%Y-%m-%d")
    out_dir = s.bronze_dir / feed / f"dt={dt}"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{feed}_{last_updated}_{int(ingested_at * 1000)}.json"
    envelope = {
        "_meta": {
            "feed": feed,
            "source_url": source_url,
            "ingested_at": ingested_at,
            "feed_last_updated": last_updated,
            "ttl": payload.get("ttl"),
            "gbfs_version": payload.get("version"),
        },
        "payload": payload,
    }
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(envelope))
    tmp.replace(path)          # atomic rename: readers never see half-written files
    return path


def list_files(feed: str, s: Settings = SETTINGS) -> list[Path]:
    return sorted((s.bronze_dir / feed).glob("dt=*/*.json"))
