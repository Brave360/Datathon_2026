#!/usr/bin/env python3
"""
Fill missing object_state (canton) for rows that have no geo coords.
Uses Nominatim forward search by postal code + countrycodes=ch.

Run:
    python scripts/fill_canton_from_zip.py
    python scripts/fill_canton_from_zip.py --file raw_data/structured_data_withoutimages-1776412361239.csv
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

REPO_ROOT  = Path(__file__).resolve().parents[1]
RAW_DATA   = REPO_ROOT / "raw_data"
CACHE_PATH = REPO_ROOT / "data" / "zip_canton_cache.json"

NULL_VALS       = {"", "NULL", "null", "None", "none", "N/A", "{}"}
USER_AGENT      = "datathon-2026-location-enrichment/1.0"
MIN_DELAY       = 1.1
NOMINATIM_URL   = "https://nominatim.openstreetmap.org"


def is_null(v: str | None) -> bool:
    return v is None or str(v).strip() in NULL_VALS


def normalize_swiss_canton(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        return ""
    upper = unicodedata.normalize("NFKD", normalized).encode("ascii", "ignore").decode("ascii").upper()
    upper = upper.removeprefix("CANTON OF ").removeprefix("CANTON DE ").removeprefix("KANTON ")
    if len(upper) == 2:
        return upper
    canton_names = {
        "AARGAU": "AG", "APPENZELL AUSSERRHODEN": "AR", "APPENZELL INNERRHODEN": "AI",
        "BASEL-LANDSCHAFT": "BL", "BASEL-LAND": "BL", "BASEL-STADT": "BS", "BASEL CITY": "BS",
        "BERN": "BE", "BERNE": "BE", "FRIBOURG": "FR", "FREIBURG": "FR",
        "GENEVA": "GE", "GENEVE": "GE", "GLARUS": "GL", "GRAUBUNDEN": "GR", "GRISONS": "GR",
        "JURA": "JU", "LUCERNE": "LU", "LUZERN": "LU", "NEUCHATEL": "NE",
        "NIDWALDEN": "NW", "OBWALDEN": "OW", "SCHAFFHAUSEN": "SH", "SCHWYZ": "SZ",
        "SOLOTHURN": "SO", "ST. GALLEN": "SG", "SAINT GALLEN": "SG", "ST GALLEN": "SG",
        "THURGAU": "TG", "TICINO": "TI", "URI": "UR", "VALAIS": "VS", "WALLIS": "VS",
        "VAUD": "VD", "ZUG": "ZG", "ZURICH": "ZH",
    }
    return canton_names.get(upper, normalized)


def forward_geocode_zip(postal_code: str, last_request_at: list[float]) -> str | None:
    """Query Nominatim for a Swiss postal code, return canton abbreviation or None."""
    query = urllib.parse.urlencode({
        "format": "json",
        "postalcode": postal_code,
        "countrycodes": "ch",
        "addressdetails": "1",
        "limit": "1",
    })
    req = urllib.request.Request(
        f"{NOMINATIM_URL}/search?{query}",
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
    )
    elapsed = time.monotonic() - last_request_at[0]
    if elapsed < MIN_DELAY:
        time.sleep(MIN_DELAY - elapsed)

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            results = json.loads(resp.read().decode("utf-8"))
    except Exception as exc:
        logger.warning("Forward geocode failed for %s: %s", postal_code, exc)
        last_request_at[0] = time.monotonic()
        return None

    last_request_at[0] = time.monotonic()
    if not results:
        return None

    address = results[0].get("address", {})
    state = (address.get("state") or address.get("county") or address.get("region") or "").strip()
    return normalize_swiss_canton(state) or None


def load_cache() -> dict[str, str]:
    if not CACHE_PATH.exists():
        return {}
    try:
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_cache(cache: dict[str, str]) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(cache, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def process_file(csv_path: Path) -> int:
    with csv_path.open(newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        fields = list(reader.fieldnames or [])
        rows = list(reader)

    if "object_state" not in fields:
        fields.append("object_state")
        for row in rows:
            row["object_state"] = ""

    cache = load_cache()
    last_request_at = [0.0]
    filled = lookups = cache_hits = 0

    for row in rows:
        state = row.get("object_state", "").strip().strip("'\"")
        if not is_null(state):
            continue

        # Try to get postal code from location_address JSON
        loc_str = row.get("location_address", "").strip()
        postal = ""
        try:
            d = json.loads(loc_str) if loc_str and loc_str not in NULL_VALS else {}
            postal = d.get("PostalCode", "").strip()
        except Exception:
            pass

        if not postal:
            continue

        if postal in cache:
            canton = cache[postal]
            cache_hits += 1
        else:
            canton = forward_geocode_zip(postal, last_request_at)
            cache[postal] = canton or ""
            lookups += 1
            if lookups % 50 == 0:
                save_cache(cache)
                logger.info("Progress: %d lookups, %d cache hits, %d filled", lookups, cache_hits, filled)

        if canton:
            row["object_state"] = canton
            filled += 1

    save_cache(cache)

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    logger.info("Done: %d lookups, %d cache hits, %d canton values filled → %s", lookups, cache_hits, filled, csv_path.name)
    return filled


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", help="Single CSV to process. Defaults to all CSVs in raw_data/.")
    args = parser.parse_args()

    csv_paths = [Path(args.file)] if args.file else sorted(RAW_DATA.glob("*.csv"))
    total = 0
    for p in csv_paths:
        total += process_file(p)
    logger.info("Total canton values filled: %d", total)


if __name__ == "__main__":
    main()
