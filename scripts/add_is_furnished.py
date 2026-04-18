#!/usr/bin/env python3
"""
One-time migration: add is_furnished (0/1/NULL) column to listings.

Logic (priority order):
  1. object_category = 'Möblierte Wohnung' → 1  (authoritative DB signal)
  2. Negative patterns in title+description → 0  (explicit "unfurnished")
  3. Positive patterns in title+description → 1
  4. Everything else → NULL  (unknown, not assumed unfurnished)
"""
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "listings.db"

NEGATIVE_PATTERNS = [
    "%nicht möbliert%",
    "%nicht moebliert%",
    "%unmöbliert%",
    "%unmoebliert%",
    "%ohne möbel%",
    "%non meublé%",
    "%non meuble%",
    "%non-meublé%",
    "%unfurnished%",
    "%without furniture%",
    "%not furnished%",
]

POSITIVE_PATTERNS = [
    "%möbliert%",
    "%moebliert%",
    "%möbl.%",
    "%meublé%",
    "%meuble%",  # catches "meublée" etc.
    "%furnished%",
]


def _like_clause(column: str, patterns: list[str]) -> str:
    parts = [f"LOWER({column}) LIKE ?" for _ in patterns]
    return "(" + " OR ".join(parts) + ")"


def main() -> None:
    print(f"Connecting to {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)

    # Add column if it doesn't exist
    existing = {row[1] for row in conn.execute("PRAGMA table_info(listings)")}
    if "is_furnished" not in existing:
        print("Adding is_furnished column...")
        conn.execute("ALTER TABLE listings ADD COLUMN is_furnished INTEGER DEFAULT NULL")
    else:
        print("Column is_furnished already exists — re-running classification.")
        conn.execute("UPDATE listings SET is_furnished = NULL")

    text_col = "LOWER(title || ' ' || COALESCE(description, ''))"
    neg_clause = _like_clause(text_col, NEGATIVE_PATTERNS)
    pos_clause = _like_clause(text_col, POSITIVE_PATTERNS)

    # Step 1: Möblierte Wohnung — authoritative category signal
    conn.execute("UPDATE listings SET is_furnished = 1 WHERE object_category = 'Möblierte Wohnung'")
    cat_count = conn.execute(
        "SELECT COUNT(*) FROM listings WHERE object_category = 'Möblierte Wohnung'"
    ).fetchone()[0]
    print(f"  Möblierte Wohnung (category): {cat_count}")

    # Step 2: negative text patterns override even the category signal
    conn.execute(f"UPDATE listings SET is_furnished = 0 WHERE {neg_clause}", NEGATIVE_PATTERNS)
    neg_count = conn.execute(
        f"SELECT COUNT(*) FROM listings WHERE {neg_clause}", NEGATIVE_PATTERNS
    ).fetchone()[0]
    print(f"  Negative patterns (explicitly unfurnished): {neg_count}")

    # Step 3: positive text patterns — only where not already marked negative
    conn.execute(
        f"UPDATE listings SET is_furnished = 1 WHERE {pos_clause} AND NOT ({neg_clause})",
        POSITIVE_PATTERNS + NEGATIVE_PATTERNS,
    )
    pos_count = conn.execute(
        f"SELECT COUNT(*) FROM listings WHERE {pos_clause} AND NOT ({neg_clause})",
        POSITIVE_PATTERNS + NEGATIVE_PATTERNS,
    ).fetchone()[0]
    print(f"  Positive text patterns (excluding negatives): {pos_count}")

    furnished_count = conn.execute(
        "SELECT COUNT(*) FROM listings WHERE is_furnished = 1"
    ).fetchone()[0]
    unfurnished_count = conn.execute(
        "SELECT COUNT(*) FROM listings WHERE is_furnished = 0"
    ).fetchone()[0]
    unknown_count = conn.execute(
        "SELECT COUNT(*) FROM listings WHERE is_furnished IS NULL"
    ).fetchone()[0]
    total = conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0]

    conn.commit()
    conn.close()

    print(f"  Furnished   (is_furnished=1):    {furnished_count} / {total}")
    print(f"  Unfurnished (is_furnished=0):    {unfurnished_count} / {total}")
    print(f"  Unknown     (is_furnished=NULL): {unknown_count} / {total}")
    print("Done.")


if __name__ == "__main__":
    main()
