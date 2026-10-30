"""AI layer (generative part): a plain-English availability briefing.

Where it sits: on top of GOLD, after the quality gate. Never in the ingestion
or transformation path, because LLM output is non-deterministic and can't be
the system of record.

What it consumes: a small JSON "fact pack" pulled from gold marts with SQL
(top risk zones, weather effect, next-hour predictions, DQ status). The model
never sees raw rows and never computes numbers itself: every figure comes from
SQL, the model only writes the narrative.

What it produces: a markdown briefing for the XYZ product/ops team, stored in
gold.ai_briefing with the model name, prompt hash and the exact facts used, so
every sentence can be traced back to the data.

Runs with the Anthropic API when ANTHROPIC_API_KEY is set; otherwise falls
back to a deterministic template so the pipeline never depends on an LLM.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timezone

from .config import SETTINGS, Settings

log = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a mobility analyst writing for the product team of XYZ Travel, \
an online travel agency that wants to suggest last-mile options to travellers in Washington DC.
Rules:
- Use ONLY the numbers in the provided JSON. Do not invent, estimate or recompute figures.
- If data_quality lists any WARN or FAIL checks, mention the caveat first in one line.
- Percentages in the JSON are fractions (0.25 = 25%).
- Output markdown: a 1-sentence headline, then 4-6 bullets, then one concrete product recommendation.
- Under 220 words. Plain language, no hype."""


def _rows(con, sql: str) -> list[dict]:
    cur = con.execute(sql)
    cols = [c[0] for c in cur.description]
    out = []
    for r in cur.fetchall():
        out.append({c: (v.isoformat() if hasattr(v, "isoformat") else
                        round(v, 3) if isinstance(v, float) else v) for c, v in zip(cols, r)})
    return out


def build_fact_pack(con, s: Settings = SETTINGS) -> dict:
    window = _rows(con, """SELECT MIN(hour_local) AS first_hour_local, MAX(hour_local) AS last_hour_local,
                                  COUNT(*) AS hours_observed FROM gold.dim_hour""")[0]
    return {
        "data_window": window,
        "stations": _rows(con, "SELECT COUNT(*) AS n FROM gold.dim_station")[0]["n"],
        "highest_risk_zone_hours": _rows(con, f"""
            SELECT zone_label, region_name, day_type, hour_of_day, observed_hours,
                   pct_hours_low_supply, avg_bike_supply, avg_free_ebikes_usable
            FROM gold.mart_last_mile_risk
            WHERE observed_hours >= {int(s.min_observed_hours)} AND zone_label IS NOT NULL
            ORDER BY pct_hours_low_supply DESC, avg_bike_supply ASC LIMIT 6"""),
        "best_covered_zones_am_peak": _rows(con, """
            SELECT zone_label, ROUND(AVG(avg_bike_supply),1) AS avg_bike_supply
            FROM gold.mart_last_mile_risk
            WHERE hour_of_day BETWEEN 7 AND 9 AND day_type='Weekday' AND zone_label IS NOT NULL
            GROUP BY 1 ORDER BY 2 DESC LIMIT 3"""),
        "weather_effect": _rows(con, """
            SELECT weather_condition, ROUND(AVG(avg_station_pct_empty),3) AS avg_station_pct_empty,
                   ROUND(AVG(avg_churn_per_station_hour),2) AS usage_proxy_churn, SUM(observed_hours) AS hours
            FROM gold.mart_weather_effect WHERE weather_condition <> 'Unknown' GROUP BY 1 ORDER BY 1"""),
        "next_hour_high_risk_stations": _rows(con, """
            SELECT station_name, target_hour_local, forecast_condition, p_empty_next_hour, history_hours
            FROM gold.pred_station_empty_next_hour
            WHERE risk_band = 'High' ORDER BY p_empty_next_hour DESC LIMIT 5"""),
        "data_quality": _rows(con, """
            SELECT check_name, severity, observed FROM ops.dq_results
            WHERE run_id = (SELECT run_id FROM ops.dq_results ORDER BY run_at DESC LIMIT 1)
              AND severity <> 'PASS'"""),
    }


def _template_briefing(f: dict) -> str:
    w = f["data_window"]
    lines = [f"**Last-mile bike availability, DC** ({w['first_hour_local']} to {w['last_hour_local']}, "
             f"{f['stations']} stations)", ""]
    if f["data_quality"]:
        lines.append("> Data caveat: " + ", ".join(f"{d['check_name']} = {d['severity']}" for d in f["data_quality"]))
        lines.append("")
    for z in f["highest_risk_zone_hours"][:4]:
        lines.append(f"- **{z['zone_label']}** ({z['day_type']}, {z['hour_of_day']:02d}:00): fewer than 2 bikes nearby "
                     f"{z['pct_hours_low_supply']:.0%} of observed hours (avg supply {z['avg_bike_supply']}).")
    we = {r["weather_condition"]: r for r in f["weather_effect"]}
    if "Wet" in we and "Dry" in we:
        lines.append(f"- Wet hours: stations empty {we['Wet']['avg_station_pct_empty']:.0%} of the time vs "
                     f"{we['Dry']['avg_station_pct_empty']:.0%} when dry; usage proxy {we['Wet']['usage_proxy_churn']} "
                     f"vs {we['Dry']['usage_proxy_churn']} bike moves per station-hour.")
    for p in f["next_hour_high_risk_stations"][:3]:
        lines.append(f"- Next hour risk: {p['station_name']} likely empty (p={p['p_empty_next_hour']:.0%}, "
                     f"forecast {p['forecast_condition']}).")
    lines += ["", "**Recommendation:** only suggest bikeshare as a last-mile option for zone/hour combinations "
              "with low risk, and fall back to Metro/rideshare suggestions for the high-risk ones above."]
    return "\n".join(lines)


def _llm_briefing(facts: dict) -> tuple[str, str]:
    import anthropic  # optional dependency
    model = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-5-5")
    client = anthropic.Anthropic()
    msg = client.messages.create(
        model=model, max_tokens=600, temperature=0,   # temperature 0: as repeatable as possible
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": "Facts JSON:\n" + json.dumps(facts, indent=1)}],
    )
    return msg.content[0].text, model


def generate_briefing(con, s: Settings = SETTINGS) -> str:
    facts = build_fact_pack(con, s)
    text, model = None, "template_v1"
    if os.getenv("ANTHROPIC_API_KEY"):
        try:
            text, model = _llm_briefing(facts)
        except Exception as exc:     # LLM outage must not fail the pipeline
            log.warning("LLM briefing failed, using template: %s", exc)
    if text is None:
        text = _template_briefing(facts)

    con.execute("""CREATE TABLE IF NOT EXISTS gold.ai_briefing (
                     generated_at TIMESTAMP, model VARCHAR, prompt_sha256 VARCHAR,
                     facts_json VARCHAR, briefing_md VARCHAR)""")
    con.execute("INSERT INTO gold.ai_briefing VALUES (?,?,?,?,?)", [
        datetime.now(timezone.utc).replace(tzinfo=None), model,
        hashlib.sha256(SYSTEM_PROMPT.encode()).hexdigest()[:16],
        json.dumps(facts, default=str), text])
    s.serving_dir.mkdir(parents=True, exist_ok=True)
    (s.serving_dir / "briefing.md").write_text(text + f"\n\n_Generated by {model}_\n")
    return text
