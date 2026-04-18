#!/usr/bin/env python3
"""
Fill missing CSV location fields from geo_lat / geo_lng.

The script only enriches rows that are missing at least one location field:
object_street, object_zip, object_city, object_state, or location_address.

Default output is a new CSV next to the input:
    raw_data/sred_data_withmontageimages_latlong.location_enriched.csv

Examples:
    python scripts/enrich_locations_from_geo.py --file raw_data/sred_data_withmontageimages_latlong.csv
    python scripts/enrich_locations_from_geo.py --file raw_data/sred_data_withmontageimages_latlong.csv --output raw_data/sred_enriched.csv
    python scripts/enrich_locations_from_geo.py --file raw_data/sred_data_withmontageimages_latlong.csv --in-place

Notes:
    - Uses OpenStreetMap Nominatim by default.
    - Keeps a local cache so repeated runs do not re-query the same coordinates.
    - Public Nominatim requires a custom User-Agent and at most 1 request/second.
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
from typing import Any

LOGGER = logging.getLogger(__name__)

NULL_VALUES = {"", "NULL", "null", "None", "none", "N/A", "{}"}
LOCATION_COLUMNS = ("object_zip", "object_city", "object_state")
DEFAULT_USER_AGENT = "datathon-2026-location-enrichment/1.0"


def is_missing(value: Any) -> bool:
    if value is None:
        return True
    return str(value).strip() in NULL_VALUES


def first_present(*values: Any) -> str:
    for value in values:
        if not is_missing(value):
            return str(value).strip()
    return ""


def parse_float(value: Any) -> float | None:
    if is_missing(value):
        return None
    try:
        return float(str(value).strip().replace(",", "."))
    except ValueError:
        return None


def coordinate_key(lat: float, lon: float) -> str:
    # One decimal ≈ 11km — 310 unique queries, ~6 min, sufficient for city/zip/canton.
    return f"{lat:.1f},{lon:.1f}"


def needs_location_enrichment(row: dict[str, str]) -> bool:
    if any(is_missing(row.get(column)) for column in LOCATION_COLUMNS):
        return True

    location_address = row.get("location_address")
    return is_missing(location_address)


def get_lat_lon(row: dict[str, str]) -> tuple[float, float] | None:
    lat = parse_float(row.get("geo_lat") or row.get("lat"))
    lon = parse_float(row.get("geo_lng") or row.get("geo_long") or row.get("geo_lon") or row.get("lon"))
    if lat is None or lon is None:
        return None

    # Ignore obvious invalid placeholders.
    if lat == 0 or lon == 0:
        return None
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return None

    return lat, lon


def load_cache(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    try:
        with path.open(encoding="utf-8") as handle:
            loaded = json.load(handle)
    except (OSError, json.JSONDecodeError):
        LOGGER.warning("Could not read cache at %s; starting with an empty cache", path)
        return {}
    if not isinstance(loaded, dict):
        return {}
    return {
        str(key): value
        for key, value in loaded.items()
        if isinstance(value, dict)
    }


def save_cache(path: Path, cache: dict[str, dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(cache, handle, ensure_ascii=False, indent=2, sort_keys=True)
    tmp_path.replace(path)


def reverse_geocode_nominatim(
    lat: float,
    lon: float,
    *,
    endpoint: str,
    user_agent: str,
    timeout: float,
) -> dict[str, str]:
    query = urllib.parse.urlencode(
        {
            "format": "jsonv2",
            "lat": f"{lat:.8f}",
            "lon": f"{lon:.8f}",
            "addressdetails": "1",
            "accept-language": "en,de,fr,it",
        }
    )
    request = urllib.request.Request(
        f"{endpoint.rstrip('/')}/reverse?{query}",
        headers={
            "User-Agent": user_agent,
            "Accept": "application/json",
        },
    )

    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8"))

    address = payload.get("address", {})
    if not isinstance(address, dict):
        address = {}

    road = first_present(
        address.get("road"),
        address.get("pedestrian"),
        address.get("footway"),
        address.get("path"),
        address.get("residential"),
        address.get("cycleway"),
    )
    house_number = first_present(address.get("house_number"))
    street = " ".join(part for part in (road, house_number) if part).strip()

    city = first_present(
        address.get("city"),
        address.get("town"),
        address.get("village"),
        address.get("municipality"),
        address.get("hamlet"),
        address.get("suburb"),
    )

    # In Switzerland this is normally the canton name. Convert common English
    # canton names to abbreviations when possible so object_state stays close to
    # the existing CSV schema.
    state = normalize_swiss_canton(
        first_present(address.get("state"), address.get("county"), address.get("region"))
    )

    return {
        "object_street": street,
        "object_zip": first_present(address.get("postcode")),
        "object_city": city,
        "object_state": state,
        "country": first_present(address.get("country")),
        "country_code": first_present(address.get("country_code")).upper(),
        "display_name": first_present(payload.get("display_name")),
        "provider": "nominatim",
    }


def normalize_swiss_canton(value: str) -> str:
    normalized = value.strip()
    if not normalized:
        return ""

    upper = unicodedata.normalize("NFKD", normalized).encode("ascii", "ignore").decode("ascii").upper()
    upper = upper.removeprefix("CANTON OF ").removeprefix("CANTON DE ").removeprefix("KANTON ")
    if len(upper) == 2:
        return upper

    canton_names = {
        "AARGAU": "AG",
        "APPENZELL AUSSERRHODEN": "AR",
        "APPENZELL INNERRHODEN": "AI",
        "BASEL-LANDSCHAFT": "BL",
        "BASEL-LAND": "BL",
        "BASEL COUNTRY": "BL",
        "BASEL-STADT": "BS",
        "BASEL CITY": "BS",
        "BERN": "BE",
        "BERNE": "BE",
        "FRIBOURG": "FR",
        "FREIBURG": "FR",
        "GENEVA": "GE",
        "GENEVE": "GE",
        "GLARUS": "GL",
        "GRAUBUNDEN": "GR",
        "GRISONS": "GR",
        "JURA": "JU",
        "LUCERNE": "LU",
        "LUZERN": "LU",
        "NEUCHATEL": "NE",
        "NIDWALDEN": "NW",
        "OBWALDEN": "OW",
        "SCHAFFHAUSEN": "SH",
        "SCHWYZ": "SZ",
        "SOLOTHURN": "SO",
        "ST. GALLEN": "SG",
        "SAINT GALLEN": "SG",
        "ST GALLEN": "SG",
        "THURGAU": "TG",
        "TICINO": "TI",
        "URI": "UR",
        "VALAIS": "VS",
        "WALLIS": "VS",
        "VAUD": "VD",
        "ZUG": "ZG",
        "ZURICH": "ZH",
    }
    return canton_names.get(upper, normalized)


def merge_location(row: dict[str, str], location: dict[str, str]) -> int:
    filled = 0
    for column in LOCATION_COLUMNS:
        if is_missing(row.get(column)) and not is_missing(location.get(column)):
            row[column] = location[column]
            filled += 1

    if is_missing(row.get("location_address")):
        location_address = {
            "PostalCode": first_present(location.get("object_zip")),
            "City": first_present(location.get("object_city")),
            "Street": first_present(location.get("object_street")),
            "StreetNumber": "",
            "canton": first_present(location.get("object_state")),
            "Country": first_present(location.get("country_code"), "CH"),
        }
        has_real_location = any(
            location_address[key]
            for key in ("PostalCode", "City", "Street", "canton")
        )
        if has_real_location:
            row["location_address"] = json.dumps(location_address, ensure_ascii=False)
            filled += 1

    return filled


def ensure_schema(fieldnames: list[str]) -> list[str]:
    fields = list(fieldnames)
    for column in ("location_address", *LOCATION_COLUMNS):
        if column not in fields:
            fields.append(column)
    return fields


def enrich_file(
    input_path: Path,
    output_path: Path,
    *,
    cache_path: Path,
    endpoint: str,
    user_agent: str,
    min_delay_seconds: float,
    timeout: float,
) -> None:
    with input_path.open(newline="", encoding="utf-8-sig", errors="replace") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"{input_path} has no CSV header")
        rows = list(reader)
        fieldnames = ensure_schema(list(reader.fieldnames))

    cache = load_cache(cache_path)
    last_request_at = 0.0
    looked_up = 0
    cache_hits = 0
    skipped = 0
    failed = 0
    filled_values = 0

    for row in rows:
        if not needs_location_enrichment(row):
            skipped += 1
            continue

        lat_lon = get_lat_lon(row)
        if lat_lon is None:
            skipped += 1
            continue

        lat, lon = lat_lon
        key = coordinate_key(lat, lon)
        location = cache.get(key)

        if location is None:
            elapsed = time.monotonic() - last_request_at
            if elapsed < min_delay_seconds:
                time.sleep(min_delay_seconds - elapsed)

            try:
                location = reverse_geocode_nominatim(
                    lat,
                    lon,
                    endpoint=endpoint,
                    user_agent=user_agent,
                    timeout=timeout,
                )
            except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
                LOGGER.warning("Reverse geocode failed for %s: %s", key, exc)
                failed += 1
                continue

            cache[key] = location
            looked_up += 1
            last_request_at = time.monotonic()
            if looked_up % 50 == 0:
                save_cache(cache_path, cache)
                LOGGER.info("Progress: %d lookups, %d cache hits, %d filled (cache saved)", looked_up, cache_hits, filled_values)
        else:
            cache_hits += 1

        filled_values += merge_location(row, location)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    save_cache(cache_path, cache)

    LOGGER.info("Input rows: %d", len(rows))
    LOGGER.info("Skipped rows: %d", skipped)
    LOGGER.info("Cache hits: %d", cache_hits)
    LOGGER.info("Network lookups: %d", looked_up)
    LOGGER.info("Failed lookups: %d", failed)
    LOGGER.info("Filled values: %d", filled_values)
    LOGGER.info("Wrote: %s", output_path)
    LOGGER.info("Cache: %s", cache_path)


def default_output_path(input_path: Path) -> Path:
    return input_path.with_name(f"{input_path.stem}.location_enriched{input_path.suffix}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fill missing object_city/object_state/object_zip/object_street from geo_lat/geo_lng."
    )
    parser.add_argument("--file", required=True, help="CSV file to enrich.")
    parser.add_argument("--output", help="Output CSV path. Defaults to *.location_enriched.csv.")
    parser.add_argument("--in-place", action="store_true", help="Overwrite the input CSV.")
    parser.add_argument(
        "--cache",
        default="data/reverse_geocode_cache.json",
        help="JSON cache file for coordinate lookups.",
    )
    parser.add_argument(
        "--endpoint",
        default="https://nominatim.openstreetmap.org",
        help="Reverse geocoding endpoint compatible with Nominatim.",
    )
    parser.add_argument(
        "--user-agent",
        default=DEFAULT_USER_AGENT,
        help="Custom User-Agent required by public Nominatim.",
    )
    parser.add_argument(
        "--min-delay-seconds",
        type=float,
        default=1.1,
        help="Minimum delay between uncached requests.",
    )
    parser.add_argument("--timeout", type=float, default=30.0, help="HTTP timeout in seconds.")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    input_path = Path(args.file)
    if args.in_place and args.output:
        raise ValueError("Use either --in-place or --output, not both.")
    output_path = input_path if args.in_place else Path(args.output) if args.output else default_output_path(input_path)

    enrich_file(
        input_path,
        output_path,
        cache_path=Path(args.cache),
        endpoint=args.endpoint,
        user_agent=args.user_agent,
        min_delay_seconds=args.min_delay_seconds,
        timeout=args.timeout,
    )


if __name__ == "__main__":
    main()
