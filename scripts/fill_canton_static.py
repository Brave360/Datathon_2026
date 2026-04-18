#!/usr/bin/env python3
"""
Fill missing object_state (canton) for all CSVs using a static Swiss PLZ→canton
lookup table. Falls back to Nominatim cache (data/zip_canton_cache.json) for
PLZ codes that fall in ambiguous ranges.

Run:
    py scripts/fill_canton_static.py
"""
from __future__ import annotations

import csv
import json
import unicodedata
from pathlib import Path

REPO_ROOT  = Path(__file__).resolve().parents[1]
RAW_DATA   = REPO_ROOT / "raw_data"
CACHE_PATH = REPO_ROOT / "data" / "zip_canton_cache.json"

NULL_VALS = {"", "NULL", "null", "None", "none", "N/A", "{}"}


def is_null(v: str | None) -> bool:
    return v is None or str(v).strip() in NULL_VALS


# ── Static range table ────────────────────────────────────────────────────────
# Sorted by (start, end). More specific ranges must come first when they overlap.
# Format: (start_inclusive, end_inclusive, canton_code)
_RANGES: list[tuple[int, int, str]] = [
    # GE — Geneva (must be before VD 1000-1199)
    (1200, 1299, "GE"),
    # VD — Vaud
    (1000, 1199, "VD"),
    (1300, 1543, "VD"),
    (1800, 1891, "VD"),   # Riviera / Montreux / Pays d'Enhaut
    # FR — Fribourg (1544-1799)
    (1544, 1799, "FR"),
    # VS — Valais
    (1892, 1999, "VS"),
    (3900, 3999, "VS"),
    # NE — Neuchâtel
    (2000, 2414, "NE"),
    (2415, 2416, "NE"),   # Le Locle area
    # BE — Bern
    (2500, 2609, "BE"),   # Biel/Seeland/Aarberg area
    (3000, 3899, "BE"),
    # JU — Jura  (some 26xx are JU, but 27xx+ is mainly JU)
    (2610, 2699, "JU"),
    (2700, 2999, "JU"),
    # BS — Basel-Stadt
    (4000, 4058, "BS"),
    # BL — Basel-Landschaft
    (4100, 4499, "BL"),
    # SO — Solothurn
    (4500, 4799, "SO"),
    # AG — Aargau (4800-4999 is actually AG, not SO)
    (4800, 4999, "AG"),
    (5000, 5999, "AG"),
    # OW — Obwalden (before LU range)
    (6060, 6086, "OW"),
    # LU — Luzern
    (6000, 6059, "LU"),
    (6087, 6299, "LU"),
    # ZG — Zug
    (6300, 6369, "ZG"),
    # NW — Nidwalden
    (6370, 6389, "NW"),
    (6390, 6399, "LU"),
    # SZ — Schwyz (Innerschwyz: 6400-6416)
    (6400, 6416, "SZ"),
    # UR — Uri  (6460-6499)
    (6460, 6499, "UR"),
    # SZ — Schwyz (outer: 6417-6459)
    (6417, 6459, "SZ"),
    # TI — Ticino
    (6500, 6999, "TI"),
    # GR — Graubünden
    (7000, 7999, "GR"),
    # SH — Schaffhausen
    (8200, 8260, "SH"),
    # GL — Glarus  (must be before ZH/TG 87xx)
    (8750, 8774, "GL"),
    # SZ — Schwyz (Äusserschwyz: March/Höfe)
    (8800, 8856, "SZ"),
    # TG — Thurgau
    (8261, 8280, "TG"),
    (8355, 8370, "TG"),
    (8500, 8599, "TG"),
    # ZH — Zürich (city + suburbs)
    (8000, 8199, "ZH"),
    (8281, 8354, "ZH"),
    (8371, 8499, "ZH"),
    (8600, 8749, "ZH"),
    (8775, 8799, "ZH"),
    (8857, 8999, "ZH"),
    # AI — Appenzell Innerrhoden (narrow range, before SG/AR)
    (9050, 9058, "AI"),
    # AR — Appenzell Ausserrhoden
    (9040, 9049, "AR"),
    (9059, 9108, "AR"),
    # SG — St. Gallen
    (9000, 9039, "SG"),
    (9100, 9499, "SG"),
    # TG — Thurgau (Wil / Konstanz area; overlaps with SG)
    (9500, 9599, "TG"),
    # SG — St. Gallen (Toggenburg)
    (9600, 9699, "SG"),
]

# Sort: more specific (narrower) ranges take precedence
_RANGES.sort(key=lambda t: (t[0], -(t[1] - t[0])))


def plz_to_canton_range(plz: str) -> str | None:
    """Return canton abbreviation from static range table, or None."""
    try:
        code = int(plz.strip())
    except ValueError:
        return None
    for start, end, canton in _RANGES:
        if start <= code <= end:
            return canton
    return None


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


def load_cache() -> dict[str, str]:
    if not CACHE_PATH.exists():
        return {}
    try:
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_cache(cache: dict[str, str]) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8"
    )


def lookup_canton(plz: str, cache: dict[str, str]) -> str | None:
    """Check Nominatim cache first, then static range lookup."""
    if plz in cache:
        return cache[plz] or None
    canton = plz_to_canton_range(plz)
    if canton:
        cache[plz] = canton  # populate cache for future runs
    return canton


def process_file(csv_path: Path, cache: dict[str, str]) -> int:
    with csv_path.open(newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        fields = list(reader.fieldnames or [])
        rows = list(reader)

    if "object_state" not in fields:
        fields.append("object_state")
        for row in rows:
            row["object_state"] = ""

    filled = 0
    for row in rows:
        state = row.get("object_state", "").strip().strip("'\"")
        if not is_null(state):
            continue

        loc_str = row.get("location_address", "").strip()
        postal = ""
        try:
            d = json.loads(loc_str) if loc_str and loc_str not in NULL_VALS else {}
            postal = str(d.get("PostalCode", "")).strip()
        except Exception:
            pass
        if not postal:
            postal = row.get("object_zip", "").strip()

        if not postal or postal in NULL_VALS:
            continue

        canton = lookup_canton(postal, cache)
        if canton:
            row["object_state"] = canton
            filled += 1

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print(f"  {csv_path.name}: {filled} values filled")
    return filled


def main() -> None:
    cache = load_cache()
    print(f"Loaded {len(cache)} cached PLZ entries")

    total = 0
    for csv_path in sorted(RAW_DATA.glob("*.csv")):
        total += process_file(csv_path, cache)

    save_cache(cache)
    print(f"\nTotal canton values filled: {total}")
    print(f"Cache now has {len(cache)} PLZ entries")


if __name__ == "__main__":
    main()
