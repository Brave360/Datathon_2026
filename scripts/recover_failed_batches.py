#!/usr/bin/env python3
"""
Recover data from failed_batches.jsonl by parsing partial/truncated LLM output
field-by-field using regex instead of full JSON parsing.

Even truncated responses contain complete objects at the start — this script
extracts as many complete listings as possible from each failed batch and
applies them back to the CSVs.

Run:
    python scripts/recover_failed_batches.py
"""
from __future__ import annotations

import csv
import json
import re
from pathlib import Path

REPO_ROOT  = Path(__file__).resolve().parents[1]
RAW_DATA   = REPO_ROOT / "raw_data"
FAILED_LOG = REPO_ROOT / "failed_batches.jsonl"

FIELD_MAP: dict[str, str] = {
    "available_from":      "available_from",
    "floor":               "floor",
    "number_of_bedrooms":  "number_of_bedrooms",
    "number_of_bathrooms": "number_of_bathrooms",
    "last_renovation":     "last_renovation",
    "year_built":          "year_built",
    "prop_elevator":       "prop_elevator",
    "prop_balcony":        "prop_balcony",
    "prop_parking":        "prop_parking",
    "prop_garage":         "prop_garage",
    "prop_fireplace":      "prop_fireplace",
    "prop_child_friendly": "prop_child_friendly",
    "animal_allowed":      "animal_allowed",
    "washing_machine":     "washing_machine",
}

SKIP_SOURCES = {"ROBINREAL"}
NULL_VALS    = {"", "NULL", "null", "None", "none", "N/A"}

# Per-field regex patterns — each captures the raw value token
FIELD_PATTERNS: dict[str, str] = {
    "available_from":      r'"available_from"\s*:\s*(?:"([^"]*)"|(null))',
    "floor":               r'"floor"\s*:\s*(-?\d+|(null))',
    "number_of_bedrooms":  r'"number_of_bedrooms"\s*:\s*(\d+|(null))',
    "number_of_bathrooms": r'"number_of_bathrooms"\s*:\s*(\d+|(null))',
    "last_renovation":     r'"last_renovation"\s*:\s*(\d+|(null))',
    "year_built":          r'"year_built"\s*:\s*(\d+|(null))',
    "prop_elevator":       r'"prop_elevator"\s*:\s*(true|false|(null))',
    "prop_balcony":        r'"prop_balcony"\s*:\s*(true|false|(null))',
    "prop_parking":        r'"prop_parking"\s*:\s*(true|false|(null))',
    "prop_garage":         r'"prop_garage"\s*:\s*(true|false|(null))',
    "prop_fireplace":      r'"prop_fireplace"\s*:\s*(true|false|(null))',
    "prop_child_friendly": r'"prop_child_friendly"\s*:\s*(true|false|(null))',
    "animal_allowed":      r'"animal_allowed"\s*:\s*(true|false|(null))',
    "washing_machine":     r'"washing_machine"\s*:\s*(true|false|(null))',
}


def is_null(v: str | None) -> bool:
    return v is None or str(v).strip() in NULL_VALS


def parse_value(raw: str) -> str | None:
    """Convert a raw token (null/true/false/number/date-string) to CSV string."""
    if raw == "null":
        return None
    if raw == "true":
        return "true"
    if raw == "false":
        return "false"
    return raw  # integer year, date string, floor number


def extract_from_object_text(obj_text: str) -> dict:
    """Extract all fields from a single {...} block using per-field regex."""
    result = {}
    for field, pattern in FIELD_PATTERNS.items():
        m = re.search(pattern, obj_text)
        if not m:
            continue
        # First non-None group is the captured value
        raw = next((g for g in m.groups() if g is not None), None)
        if raw is None:
            continue
        val = parse_value(raw)
        if val is not None:
            result[field] = val
    return result


def extract_objects(raw_text: str) -> list[dict]:
    """
    Extract as many complete field sets as possible from partial JSON text.
    Works on truncated output by matching individual {...} blocks.
    """
    objects = []
    # Match flat objects: { ... } with no nested braces
    for m in re.finditer(r'\{([^{}]*)\}', raw_text, re.DOTALL):
        obj_text = m.group(0)
        # Skip if it doesn't contain at least one of our field names
        if '"available_from"' not in obj_text and '"floor"' not in obj_text:
            continue
        # Try clean JSON parse first
        try:
            parsed = json.loads(obj_text)
            if isinstance(parsed, dict):
                objects.append({k: v for k, v in parsed.items() if k in FIELD_MAP})
                continue
        except json.JSONDecodeError:
            pass
        # Fall back to per-field regex
        extracted = extract_from_object_text(obj_text)
        if extracted:
            objects.append(extracted)
    return objects


def decode_id(custom_id: str) -> tuple[int, int]:
    m = re.match(r"c(\d+)b(\d+)", custom_id)
    return int(m.group(1)), int(m.group(2))


def load_csv(csv_path: Path) -> tuple[list[str], list[dict]]:
    with csv_path.open(newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.DictReader(fh)
        fields = list(reader.fieldnames or [])
        rows   = list(reader)
    return fields, rows


def write_csv(csv_path: Path, rows: list[dict], fields: list[str]) -> None:
    tmp = csv_path.with_suffix(".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(csv_path)


def source_of(csv_path: Path) -> str:
    with csv_path.open(newline="", encoding="utf-8", errors="replace") as fh:
        first = next(csv.DictReader(fh), None)
    return (first or {}).get("scrape_source", "").strip("'\"")


def missing_fields(row: dict) -> set[str]:
    return {k for k, col in FIELD_MAP.items() if is_null(row.get(col))}


def main() -> None:
    if not FAILED_LOG.exists():
        print("No failed_batches.jsonl found — nothing to recover.")
        return

    csv_paths = [p for p in sorted(RAW_DATA.glob("*.csv"))
                 if source_of(p) not in SKIP_SOURCES]

    # Load all CSVs
    all_fields: list[list[str]] = []
    all_rows:   list[list[dict]] = []
    for p in csv_paths:
        f, r = load_csv(p)
        all_fields.append(f)
        all_rows.append(r)
        print(f"Loaded {p.name}: {len(r)} rows")

    # Build needs_llm per CSV (same order as submission)
    all_needs_llm = [
        [(i, row) for i, row in enumerate(rows) if missing_fields(row)]
        for rows in all_rows
    ]

    total_batches = 0
    total_objects = 0
    total_filled  = 0

    with FAILED_LOG.open(encoding="utf-8") as fh:
        for line in fh:
            entry = json.loads(line)
            custom_id = entry["custom_id"]
            raw_text  = entry["raw"]

            csv_idx, batch_start = decode_id(custom_id)
            rows      = all_rows[csv_idx]
            needs_llm = all_needs_llm[csv_idx]
            chunk     = needs_llm[batch_start: batch_start + 25]  # BATCH_SIZE=25

            objects = extract_objects(raw_text)
            total_batches += 1
            total_objects += len(objects)

            # Apply positionally — only use objects we have (skip truncated tail)
            for (row_idx, _), extracted in zip(chunk, objects):
                for llm_key, value in extracted.items():
                    col = FIELD_MAP.get(llm_key)
                    if col and is_null(rows[row_idx].get(col)):
                        rows[row_idx][col] = value
                        total_filled += 1

    print(f"\nRecovered {total_objects} listings from {total_batches} failed batches")
    print(f"Filled {total_filled} values")

    for csv_path, fields, rows in zip(csv_paths, all_fields, all_rows):
        write_csv(csv_path, rows, fields)
        print(f"Saved {csv_path.name}")


if __name__ == "__main__":
    main()
