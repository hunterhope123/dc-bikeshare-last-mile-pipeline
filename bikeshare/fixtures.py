"""Synthetic GBFS + weather data for offline runs and tests.

Writes bronze files through the SAME `bronze.land()` function the live poller
uses, so everything downstream is exercised exactly as in production.
Payload shapes follow the GBFS 2.3 JSON schemas (MobilityData/gbfs-json-schema).

Numbers are made up. Use `--mode live` for real findings.

Built-in data problems (so the quality checks have something to catch):
  * station 9999 appears in station_status but not station_information (orphan)
  * one station stops reporting on day 2 (stale -> offline, not empty)
  * one station has is_renting = false for a few hours
  * a few free bikes at (0, 0) and some reserved/disabled/docked bikes
  * one station_status payload landed twice (polled faster than ttl)
  * bike_ids are re-randomised every snapshot (GBFS privacy rotation)
"""
from __future__ import annotations

import math
import random
import uuid
from datetime import datetime, timedelta, timezone

from . import bronze
from .config import SETTINGS, Settings

START_UTC = datetime(2026, 10, 23, 4, 0, tzinfo=timezone.utc)   # Fri 00:00 EDT
DAYS = 4
STEP_MIN = 5
BASE = "https://gbfs.lyft.com/gbfs/2.3/dca-cabi/en/"


def _stations(rnd: random.Random) -> list[dict]:
    specs = [("Downtown", 22, (38.900, -77.032), 0.010), ("Residential", 22, (38.930, -77.025), 0.018),
             ("Tourist", 14, (38.889, -77.025), 0.012)]
    out, i = [], 1
    for kind, n, (clat, clon), spread in specs:
        for _ in range(n):
            out.append(dict(station_id=str(1000 + i), name=f"Synthetic {kind} Station {i:02d}",
                            short_name=f"SYN{i:02d}", kind=kind,
                            lat=round(clat + rnd.uniform(-spread, spread), 6),
                            lon=round(clon + rnd.uniform(-spread * 1.3, spread * 1.3), 6),
                            capacity=rnd.choice([11, 15, 15, 19, 19, 23, 27, 31]), region_id="42",
                            phase=rnd.uniform(-0.6, 0.6)))
            i += 1
    return out


def _is_wet(local: datetime) -> bool:
    return (local.day == 24 and 13 <= local.hour <= 19) or (local.day == 26 and 6 <= local.hour <= 10)


def _fill_fraction(st: dict, local: datetime, rnd: random.Random) -> float:
    h = local.hour + local.minute / 60
    weekend = local.weekday() >= 5
    damp = 0.4 if _is_wet(local) else 1.0                  # rain = fewer trips = flatter curve
    bump = lambda c, w: math.exp(-((h - c - st["phase"]) ** 2) / (2 * w ** 2))  # noqa: E731
    if st["kind"] == "Residential":
        dev = 0.0 if weekend else -0.62 * bump(8.2, 1.1) - 0.25 * bump(9.5, 1.5)
        base = 0.62
    elif st["kind"] == "Downtown":
        dev = 0.0 if weekend else 0.50 * bump(11, 2.5) - 0.42 * bump(18.3, 1.0)
        base = 0.30 if not weekend else 0.45
    else:  # Tourist
        dev = -(0.55 if weekend else 0.30) * bump(14, 2.2)
        base = 0.50
    return base + damp * dev + rnd.gauss(0, 0.03)


def _weather_payload(rnd: random.Random) -> dict:
    times, temp, precip, wind, code = [], [], [], [], []
    t = START_UTC - timedelta(hours=4)
    while t < START_UTC + timedelta(days=DAYS, hours=24):           # + 1 day "forecast"
        local = t.astimezone(SETTINGS.local_tz)
        wet = _is_wet(local)
        times.append(t.strftime("%Y-%m-%dT%H:00"))
        temp.append(round(12 + 6 * math.sin((local.hour - 9) / 24 * 2 * math.pi) + rnd.gauss(0, 0.8), 1))
        precip.append(round(rnd.uniform(0.4, 3.0), 1) if wet else 0.0)
        wind.append(round(rnd.uniform(4, 18), 1))
        code.append(61 if wet else rnd.choice([0, 1, 2, 3]))
        t += timedelta(hours=1)
    return {"latitude": 38.9072, "longitude": -77.0369, "timezone": "GMT",
            "hourly_units": {"temperature_2m": "°C", "precipitation": "mm", "wind_speed_10m": "km/h"},
            "hourly": {"time": times, "temperature_2m": temp, "precipitation": precip,
                       "wind_speed_10m": wind, "weather_code": code}}


def generate(s: Settings = SETTINGS, seed: int = 42, days: int = DAYS) -> dict:
    rnd = random.Random(seed)
    stations = _stations(rnd)
    st_by_kind = {k: [x for x in stations if x["kind"] == k] for k in ("Downtown", "Residential", "Tourist")}
    offline_id, not_renting_id = stations[5]["station_id"], stations[30]["station_id"]
    counts = {"station_status": 0, "free_bike_status": 0}

    # reference feeds (daily)
    bronze.land("system_regions", {"last_updated": int(START_UTC.timestamp()), "ttl": 86400, "version": "2.3",
                "data": {"regions": [{"region_id": "42", "name": "Washington, DC"},
                                     {"region_id": "41", "name": "Arlington, VA"}]}},
                BASE + "system_regions.json", s=s)
    bronze.land("vehicle_types", {"last_updated": int(START_UTC.timestamp()), "ttl": 86400, "version": "2.3",
                "data": {"vehicle_types": [
                    {"vehicle_type_id": "1", "form_factor": "bicycle", "propulsion_type": "human", "name": "Classic"},
                    {"vehicle_type_id": "2", "form_factor": "bicycle", "propulsion_type": "electric_assist",
                     "max_range_meters": 60000, "name": "E-bike"}]}},
                BASE + "vehicle_types.json", s=s)
    for d in range(days):
        lu = int((START_UTC + timedelta(days=d)).timestamp())
        bronze.land("station_information", {"last_updated": lu, "ttl": 86400, "version": "2.3", "data": {
            "stations": [{k: v for k, v in st.items() if k not in ("kind", "phase")} for st in stations]}},
            BASE + "station_information.json", s=s)

    frozen_report = None
    t = START_UTC
    end = START_UTC + timedelta(days=days)
    while t < end:
        lu = int(t.timestamp())
        local = t.astimezone(s.local_tz)

        # ---- station_status
        rows = []
        for st in stations:
            cap = st["capacity"]
            frac = min(1.0, max(0.0, _fill_fraction(st, local, rnd)))
            bikes = int(round(frac * cap))
            disabled = 1 if rnd.random() < 0.05 else 0
            docks = max(0, cap - bikes - disabled)
            reported = lu - rnd.randint(0, 120)
            renting = True
            if st["station_id"] == offline_id and t >= START_UTC + timedelta(days=1, hours=10):
                frozen_report = frozen_report or reported
                reported = frozen_report                     # stops reporting
            if st["station_id"] == not_renting_id and local.day == 25 and 9 <= local.hour < 13:
                renting = False                              # maintenance window
            rows.append({"station_id": st["station_id"], "num_bikes_available": bikes,
                         "num_ebikes_available": int(bikes * rnd.uniform(0.15, 0.45)),
                         "num_docks_available": docks, "num_bikes_disabled": disabled, "num_docks_disabled": 0,
                         "is_installed": True, "is_renting": renting, "is_returning": True,
                         "last_reported": reported})
        rows.append({"station_id": "9999", "num_bikes_available": 3, "num_docks_available": 8,
                     "is_installed": True, "is_renting": True, "is_returning": True, "last_reported": lu})
        payload = {"last_updated": lu, "ttl": 60, "version": "2.3", "data": {"stations": rows}}
        bronze.land("station_status", payload, BASE + "station_status.json", ingested_at=lu + 3, s=s)
        counts["station_status"] += 1
        if t == START_UTC + timedelta(hours=30):             # duplicate poll of the same feed version
            bronze.land("station_status", payload, BASE + "station_status.json", ingested_at=lu + 45, s=s)

        # ---- free_bike_status (dockless e-bikes cluster where demand is)
        bikes = []
        downtown_weight = 2.0 if 9 <= local.hour <= 17 and local.weekday() < 5 else 1.0
        tourist_weight = 2.0 if local.weekday() >= 5 and 10 <= local.hour <= 18 else 0.8
        for _ in range(rnd.randint(110, 150)):
            kind = rnd.choices(["Downtown", "Residential", "Tourist"], [downtown_weight, 1.0, tourist_weight])[0]
            anchor = rnd.choice(st_by_kind[kind])
            docked = rnd.random() < 0.08
            bikes.append({"bike_id": uuid.UUID(int=rnd.getrandbits(128)).hex,  # rotates every snapshot
                          "lat": round(anchor["lat"] + rnd.gauss(0, 0.003), 6),
                          "lon": round(anchor["lon"] + rnd.gauss(0, 0.003), 6),
                          "is_reserved": rnd.random() < 0.03, "is_disabled": rnd.random() < 0.03,
                          "vehicle_type_id": "2", "current_range_meters": rnd.randint(500, 48000),
                          "station_id": anchor["station_id"] if docked else None, "last_reported": lu})
        if rnd.random() < 0.1:
            bikes.append({"bike_id": uuid.UUID(int=rnd.getrandbits(128)).hex, "lat": 0.0, "lon": 0.0,
                          "is_reserved": False, "is_disabled": False, "vehicle_type_id": "2"})
        bronze.land("free_bike_status", {"last_updated": lu, "ttl": 60, "version": "2.3", "data": {"bikes": bikes}},
                    BASE + "free_bike_status.json", ingested_at=lu + 4, s=s)
        counts["free_bike_status"] += 1
        t += timedelta(minutes=STEP_MIN)

    bronze.land("weather", _weather_payload(rnd), SETTINGS.weather_url, ingested_at=end.timestamp(), s=s)
    return counts
