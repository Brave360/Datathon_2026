#!/usr/bin/env python3
"""Replace price values of 0 or 1 CHF with empty string (null) across all CSVs."""
from __future__ import annotations

import csv
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA  = REPO_ROOT / "raw_data"

PLACEHOLDER_PRICES = {0.0, 1.0}


def fix_csv(csv_path: Path) -> int:
    with csv_path.open(newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.DictReader(fh)
        fields = list(reader.fieldnames or [])
        rows   = list(reader)

    if "price" not in fields:
        return 0

    fixed = 0
    for row in rows:
        raw = row.get("price", "").strip().strip("'\"")
        if not raw:
            continue
        try:
            if float(raw) in PLACEHOLDER_PRICES:
                row["price"] = ""
                fixed += 1
        except ValueError:
            pass

    tmp = csv_path.with_suffix(".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(csv_path)

    return fixed


def main() -> None:
    csv_paths = sorted(RAW_DATA.glob("*.csv"))
    total = 0
    for p in csv_paths:
        n = fix_csv(p)
        print(f"{p.name}: {n} prices nulled")
        total += n
    print(f"Total: {total} prices replaced with null")


if __name__ == "__main__":
    main()
