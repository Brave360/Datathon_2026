from __future__ import annotations

import logging
from pathlib import Path

from app.db import get_connection

logger = logging.getLogger(__name__)

_NEGATIVE_PATTERNS = [
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

_POSITIVE_PATTERNS = [
    "%möbliert%",
    "%moebliert%",
    "%möbl.%",
    "%meublé%",
    "%meuble%",
    "%furnished%",
]


def _like_clause(column: str, patterns: list[str]) -> str:
    parts = [f"LOWER({column}) LIKE ?" for _ in patterns]
    return "(" + " OR ".join(parts) + ")"


def run_migration(db_path: Path) -> None:
    with get_connection(db_path) as conn:
        existing = {row[1] for row in conn.execute("PRAGMA table_info(listings)")}

        if "is_furnished" not in existing:
            logger.info("Adding is_furnished column to listings")
            conn.execute("ALTER TABLE listings ADD COLUMN is_furnished INTEGER DEFAULT NULL")
        else:
            # Skip if already populated (check for at least one non-NULL value)
            populated = conn.execute(
                "SELECT COUNT(*) FROM listings WHERE is_furnished IS NOT NULL"
            ).fetchone()[0]
            if populated > 0:
                logger.info("is_furnished already populated (%s rows), skipping migration", populated)
                return
            logger.info("is_furnished column exists but is empty, running classification")

        text_col = "LOWER(title || ' ' || COALESCE(description, ''))"
        neg_clause = _like_clause(text_col, _NEGATIVE_PATTERNS)
        pos_clause = _like_clause(text_col, _POSITIVE_PATTERNS)

        # Priority 1: Möblierte Wohnung category
        conn.execute("UPDATE listings SET is_furnished = 1 WHERE object_category = 'Möblierte Wohnung'")

        # Priority 2: negative text patterns (override category)
        conn.execute(f"UPDATE listings SET is_furnished = 0 WHERE {neg_clause}", _NEGATIVE_PATTERNS)

        # Priority 3: positive text patterns (only where not negative)
        conn.execute(
            f"UPDATE listings SET is_furnished = 1 WHERE is_furnished IS NULL AND {pos_clause} AND NOT ({neg_clause})",
            _POSITIVE_PATTERNS + _NEGATIVE_PATTERNS,
        )

        furnished = conn.execute("SELECT COUNT(*) FROM listings WHERE is_furnished = 1").fetchone()[0]
        unfurnished = conn.execute("SELECT COUNT(*) FROM listings WHERE is_furnished = 0").fetchone()[0]
        unknown = conn.execute("SELECT COUNT(*) FROM listings WHERE is_furnished IS NULL").fetchone()[0]
        logger.info(
            "is_furnished migration done: furnished=%s unfurnished=%s unknown=%s",
            furnished, unfurnished, unknown,
        )
