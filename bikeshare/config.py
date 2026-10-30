"""Central settings. Everything a reviewer might want to tweak lives here."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Settings:
    # --- Sources -------------------------------------------------------
    # Discovery-first: only the gbfs.json URL is hardcoded. Every other feed
    # URL is read from its `feeds` array, so path/version changes don't break us.
    gbfs_discovery_url: str = "https://gbfs.lyft.com/gbfs/2.3/dca-cabi/gbfs.json"
    gbfs_language: str = "en"
    realtime_feeds: tuple[str, ...] = ("station_status", "free_bike_status")
    reference_feeds: tuple[str, ...] = ("station_information", "vehicle_types", "system_regions")
    # GBFS 3.0 renames free_bike_status -> vehicle_status. We accept either.
    feed_aliases: dict = field(default_factory=lambda: {"vehicle_status": "free_bike_status"})

    # Open-Meteo: free, no API key. Centre of DC.
    weather_url: str = "https://api.open-meteo.com/v1/forecast"
    weather_lat: float = 38.9072
    weather_lon: float = -77.0369
    weather_vars: tuple[str, ...] = ("temperature_2m", "precipitation", "wind_speed_10m", "weather_code")

    # --- Polling -------------------------------------------------------
    # Feeds publish a ttl (usually 60s). We poll at max(ttl, poll_floor) to be a
    # polite client; 5-minute resolution is plenty for hourly availability.
    poll_floor_seconds: int = int(os.getenv("POLL_SECONDS", "300"))
    http_timeout_seconds: int = 20
    http_retries: int = 4

    # --- Storage -------------------------------------------------------
    data_dir: Path = Path(os.getenv("BIKESHARE_DATA_DIR", ROOT / "data"))

    # --- Business rules ------------------------------------------------
    local_tz: ZoneInfo = ZoneInfo("America/New_York")
    # Bounding box for the CaBi service area (DC, MD and VA suburbs). Points
    # outside are quarantined (covers 0,0 and swapped lat/lon).
    bbox: tuple[float, float, float, float] = (38.70, -77.45, 39.20, -76.85)  # min_lat, min_lon, max_lat, max_lon
    # Walkable zone: ~0.005 deg lat x 0.006 deg lon ~= 550m x 520m in DC,
    # about a 5-7 minute walk across. Simple grid, no geo library needed.
    zone_lat_deg: float = 0.005
    zone_lon_deg: float = 0.006
    # An e-bike with less than this range isn't a realistic last-mile option.
    min_usable_range_m: int = 3000
    # A station that hasn't reported in this long is treated as offline, not empty.
    station_stale_minutes: int = 60

    @property
    def bronze_dir(self) -> Path:
        return self.data_dir / "bronze"

    @property
    def warehouse_path(self) -> Path:
        return self.data_dir / "warehouse.duckdb"

    @property
    def serving_dir(self) -> Path:
        return self.data_dir / "serving"


SETTINGS = Settings()
