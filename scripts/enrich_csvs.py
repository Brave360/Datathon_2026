#!/usr/bin/env python3
"""
Data enrichment pipeline — Anthropic Message Batches API (50% cost discount).

Stage 1 (free):  Extract structured fields from COMPARIS orig_data JSON.
Stage 2 (batch): Submit all eligible rows in one Batch API call; poll until done.

Resume-safe:
  - Stage 1 skips already-filled fields.
  - Stage 2 saves the batch ID to .enrichment_batch_id; re-running polls the
    existing batch rather than submitting a new one.

Run:
    python scripts/enrich_csvs.py                # full run
    python scripts/enrich_csvs.py --dry-run      # estimate cost, no API calls
    python scripts/enrich_csvs.py --stage1-only  # free extraction only
    python scripts/enrich_csvs.py --poll         # poll / apply an existing batch
    python scripts/enrich_csvs.py --file PATH    # single CSV
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

FIELD_MAP: dict[str, str] = {
    # scalar fields
    "available_from":      "available_from",
    "floor":               "floor",
    "number_of_bedrooms":  "number_of_bedrooms",
    "number_of_bathrooms": "number_of_bathrooms",
    "last_renovation":     "last_renovation",
    "year_built":          "year_built",
    # boolean features
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

NEW_COLUMNS = ["number_of_bedrooms", "number_of_bathrooms", "last_renovation", "year_built"]

NULL_VALS = {"", "NULL", "null", "None", "none", "N/A"}

BATCH_SIZE   = 25
MAX_TOKENS   = 8192
MODEL        = "claude-haiku-4-5-20251001"
POLL_INTERVAL = 30  # seconds between status checks

REPO_ROOT    = Path(__file__).resolve().parents[1]
BATCH_ID_FILE = REPO_ROOT / ".enrichment_batch_id"
FAILED_LOG    = REPO_ROOT / "failed_batches.jsonl"

SYSTEM_PROMPT = """You are a Swiss real estate data extractor. Given a JSON array of listings, extract the following fields from the title and description. Listings may be in German, French, Italian, or English.

Return ONLY a valid JSON array (no markdown, no explanation) with one object per listing in the same order. Each object must have exactly these keys:

Scalar fields:
  available_from      – date string "YYYY-MM-DD", or null
  floor               – integer floor number (0=ground floor), or null
  number_of_bedrooms  – integer count, or null
  number_of_bathrooms – integer count, or null
  last_renovation     – integer year (e.g. 2018), or null
  year_built          – integer year (e.g. 1995), or null

Boolean features (true/false/null — null means not mentioned):
  prop_elevator       – lift / Lift / ascenseur / ascensore present
  prop_balcony        – balcony / Balkon / balcon / terrasse / loggia
  prop_parking        – outdoor parking / Parkplatz / place de parc
  prop_garage         – indoor garage / Garage / box auto
  prop_fireplace      – fireplace / Cheminée / camino / Kamin
  prop_child_friendly – child-friendly / kinderfreundlich / für Familien
  animal_allowed      – pets allowed / Haustiere erlaubt / animaux acceptés
  washing_machine     – washing machine included / Waschmaschine / lave-linge

Rules:
- null for any field not mentioned or not reliably inferable.
- "sofort" / "immédiatement" / "subito" → null for available_from.
- For bedrooms and bathrooms: only extract if explicitly stated in the text."""


# ── Helpers ───────────────────────────────────────────────────────────────────

def is_null(v: str | None) -> bool:
    return v is None or str(v).strip() in NULL_VALS


def strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", " ", text or "").strip()


def missing_fields(row: dict) -> set[str]:
    return {k for k, col in FIELD_MAP.items() if is_null(row.get(col))}


def build_input(row: dict) -> dict:
    desc = strip_html(row.get("object_description") or "")
    if not desc and row.get("scrape_source", "").strip("'\"") == "SRED":
        try:
            rr = json.loads(row.get("orig_data", "{}")).get("raw_row", {})
            desc = strip_html(rr.get("ad_description", ""))
        except Exception:
            pass
    return {
        "title": (row.get("title") or "")[:120],
        "desc":  desc[:500],
    }


def parse_llm_text(text: str, expected: int) -> list[dict] | None:
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list) and len(parsed) == expected:
            return parsed
    except json.JSONDecodeError:
        pass
    match = re.search(r"\[[\s\S]*\]", text)
    if match:
        try:
            parsed = json.loads(match.group(0))
            if isinstance(parsed, list) and len(parsed) == expected:
                return parsed
        except json.JSONDecodeError:
            pass
    return None


def write_csv(csv_path: Path, rows: list[dict], fields: list[str]) -> None:
    tmp = csv_path.with_suffix(".tmp")
    with tmp.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    tmp.replace(csv_path)


# ── Stage 1: free extraction from COMPARIS orig_data ─────────────────────────

def _maindata(maindata: list, key: str) -> Any:
    for m in maindata:
        if m.get("Key") == key:
            return m.get("Value")
    return None


def extract_from_comparis(orig_data_str: str) -> dict[str, str]:
    result: dict[str, str] = {}
    try:
        d = json.loads(orig_data_str)
    except Exception:
        return result

    maindata = d.get("MainData", [])

    floor_val = d.get("Floor")
    if floor_val is not None and str(floor_val) not in ("", "0", "null"):
        try:
            result["floor"] = str(int(floor_val))
        except (ValueError, TypeError):
            pass

    av = _maindata(maindata, "AvailableDate")
    if av and str(av).strip() not in ("", "sofort", "nach Vereinbarung"):
        raw = str(av).strip()
        m = re.match(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", raw)
        if m:
            result["available_from"] = f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
        elif re.match(r"\d{4}-\d{2}-\d{2}", raw):
            result["available_from"] = raw

    cy = d.get("ConstructionYear")
    if cy and str(cy) not in ("", "0", "null"):
        try:
            result["year_built"] = str(int(cy))
        except (ValueError, TypeError):
            pass

    ry = d.get("RenovationYear")
    if ry and str(ry) not in ("", "0", "null"):
        try:
            result["last_renovation"] = str(int(ry))
        except (ValueError, TypeError):
            pass

    nr = d.get("NumRooms")
    if nr is not None:
        try:
            nr_f = float(str(nr).replace(",", "."))
            if nr_f > 0:
                result["number_of_rooms"] = str(nr_f)
        except (ValueError, TypeError):
            pass

    return result


def run_stage1(rows: list[dict], fields: list[str]) -> int:
    filled = 0
    for row in rows:
        if row.get("scrape_source", "").strip("'\"") == "COMPARIS":
            for col, value in extract_from_comparis(row.get("orig_data", "")).items():
                if col in fields and is_null(row.get(col)):
                    row[col] = value
                    filled += 1
    return filled


# ── Stage 2: Batch API ────────────────────────────────────────────────────────

def make_batch_request(custom_id: str, batch_inputs: list[dict]) -> dict:
    return {
        "custom_id": custom_id,
        "params": {
            "model": MODEL,
            "max_tokens": MAX_TOKENS,
            "system": SYSTEM_PROMPT,
            "messages": [{
                "role": "user",
                "content": f"Extract for {len(batch_inputs)} listings:\n{json.dumps(batch_inputs, ensure_ascii=False)}",
            }],
        },
    }


def encode_id(csv_idx: int, batch_start: int) -> str:
    return f"c{csv_idx:02d}b{batch_start:06d}"


def decode_id(custom_id: str) -> tuple[int, int]:
    m = re.match(r"c(\d+)b(\d+)", custom_id)
    return int(m.group(1)), int(m.group(2))


def build_all_requests(
    all_needs_llm: list[list[tuple[int, dict]]],
) -> list[dict]:
    requests = []
    for csv_idx, needs_llm in enumerate(all_needs_llm):
        for batch_start in range(0, len(needs_llm), BATCH_SIZE):
            chunk = needs_llm[batch_start: batch_start + BATCH_SIZE]
            inputs = [build_input(row) for _, row in chunk]
            requests.append(make_batch_request(encode_id(csv_idx, batch_start), inputs))
    return requests


def submit_batch(requests: list[dict], client: Any) -> str:
    batch = client.messages.batches.create(requests=requests)
    batch_id = batch.id
    BATCH_ID_FILE.write_text(batch_id, encoding="utf-8")
    logger.info("Batch submitted: %s (%d requests) — ID saved to %s",
                batch_id, len(requests), BATCH_ID_FILE.name)
    return batch_id


def poll_until_done(batch_id: str, client: Any) -> None:
    while True:
        batch = client.messages.batches.retrieve(batch_id)
        status = batch.processing_status
        counts = batch.request_counts
        logger.info("  Batch %s — status=%s processing=%s succeeded=%s errored=%s",
                    batch_id, status,
                    getattr(counts, "processing", "?"),
                    getattr(counts, "succeeded", "?"),
                    getattr(counts, "errored", "?"))
        if status == "ended":
            break
        time.sleep(POLL_INTERVAL)


def apply_batch_results(
    batch_id: str,
    client: Any,
    all_rows: list[list[dict]],
    all_needs_llm: list[list[tuple[int, dict]]],
) -> int:
    total_filled = 0
    for result in client.messages.batches.results(batch_id):
        csv_idx, batch_start = decode_id(result.custom_id)
        rows       = all_rows[csv_idx]
        needs_llm  = all_needs_llm[csv_idx]
        chunk      = needs_llm[batch_start: batch_start + BATCH_SIZE]

        if result.result.type != "succeeded":
            logger.warning("Request %s failed: %s", result.custom_id, result.result.type)
            with FAILED_LOG.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps({
                    "custom_id": result.custom_id,
                    "error": str(result.result),
                    "inputs": [build_input(row) for _, row in chunk],
                }, ensure_ascii=False) + "\n")
            continue

        text    = result.result.message.content[0].text.strip()
        results = parse_llm_text(text, len(chunk))

        if results is None:
            logger.warning("Malformed JSON for %s; saving to %s", result.custom_id, FAILED_LOG.name)
            with FAILED_LOG.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps({
                    "custom_id": result.custom_id,
                    "inputs": [build_input(row) for _, row in chunk],
                    "raw": text,
                }, ensure_ascii=False) + "\n")
            continue

        for (row_idx, _), extracted in zip(chunk, results):
            for llm_key, value in extracted.items():
                if value is None:
                    continue
                col = FIELD_MAP.get(llm_key)
                if col and is_null(rows[row_idx].get(col)):
                    rows[row_idx][col] = str(value).lower() if isinstance(value, bool) else str(value)
                    total_filled += 1

    return total_filled


# ── CSV loading / saving ──────────────────────────────────────────────────────

def load_csv(csv_path: Path) -> tuple[list[str], list[dict]]:
    with csv_path.open(newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.DictReader(fh)
        fields = list(reader.fieldnames or [])
        rows   = list(reader)
    for col in NEW_COLUMNS:
        if col not in fields:
            fields.append(col)
            for row in rows:
                row[col] = ""
    return fields, rows


def source_of(csv_path: Path) -> str:
    with csv_path.open(newline="", encoding="utf-8", errors="replace") as fh:
        first = next(csv.DictReader(fh), None)
    return (first or {}).get("scrape_source", "").strip("'\"")


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run",     action="store_true")
    parser.add_argument("--stage1-only", action="store_true")
    parser.add_argument("--poll",        action="store_true",
                        help="Skip submission; poll the batch ID in .enrichment_batch_id.")
    parser.add_argument("--file", help="Process a single CSV instead of all.")
    args = parser.parse_args()

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key and not args.dry_run and not args.stage1_only:
        logger.error("ANTHROPIC_API_KEY not set.")
        sys.exit(1)

    client = None
    if not args.dry_run and not args.stage1_only:
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)
        except ImportError:
            logger.error("anthropic package not installed.")
            sys.exit(1)

    raw_data_dir = REPO_ROOT / "raw_data"
    csv_paths = [Path(args.file)] if args.file else sorted(raw_data_dir.glob("*.csv"))
    csv_paths = [p for p in csv_paths if source_of(p) not in SKIP_SOURCES]
    logger.info("CSVs to process: %s", [p.name for p in csv_paths])

    # ── Load all CSVs + Stage 1 ───────────────────────────────────────────
    all_fields: list[list[str]] = []
    all_rows:   list[list[dict]] = []

    for csv_path in csv_paths:
        logger.info("Loading %s", csv_path.name)
        fields, rows = load_csv(csv_path)
        filled = run_stage1(rows, fields)
        logger.info("  Stage 1: %d values filled", filled)
        all_fields.append(fields)
        all_rows.append(rows)

    if args.stage1_only:
        for csv_path, fields, rows in zip(csv_paths, all_fields, all_rows):
            write_csv(csv_path, rows, fields)
            logger.info("  Saved %d rows → %s", len(rows), csv_path.name)
        return

    # ── Build needs_llm per CSV ───────────────────────────────────────────
    all_needs_llm: list[list[tuple[int, dict]]] = []
    for rows in all_rows:
        needs_llm = [(i, row) for i, row in enumerate(rows) if missing_fields(row)]
        all_needs_llm.append(needs_llm)
        logger.info("  %d rows need LLM enrichment", len(needs_llm))

    total_rows_llm = sum(len(n) for n in all_needs_llm)
    total_requests = sum(
        (len(n) + BATCH_SIZE - 1) // BATCH_SIZE for n in all_needs_llm
    )
    logger.info("Total: %d rows → %d batch requests", total_rows_llm, total_requests)

    if args.dry_run:
        in_tok  = total_rows_llm * 250 + total_requests * 350
        out_tok = total_rows_llm * 60
        # Batch API = 50% off
        cost = (in_tok / 1e6 * 0.25 + out_tok / 1e6 * 1.25) * 0.5
        logger.info("DRY-RUN: ~%dK input tokens, ~%dK output tokens, ~$%.2f (batch pricing)",
                    in_tok // 1000, out_tok // 1000, cost)
        return

    # ── Save Stage 1 results before submitting (so a crash mid-batch is safe) ─
    for csv_path, fields, rows in zip(csv_paths, all_fields, all_rows):
        write_csv(csv_path, rows, fields)
    logger.info("Stage 1 results saved to all CSVs.")

    # ── Submit or reuse existing batch ────────────────────────────────────
    if args.poll and BATCH_ID_FILE.exists():
        batch_id = BATCH_ID_FILE.read_text(encoding="utf-8").strip()
        logger.info("Reusing existing batch: %s", batch_id)
    elif BATCH_ID_FILE.exists() and not args.poll:
        batch_id = BATCH_ID_FILE.read_text(encoding="utf-8").strip()
        logger.info("Found existing batch ID %s — polling instead of re-submitting. "
                    "Delete %s to force a new submission.", batch_id, BATCH_ID_FILE.name)
    else:
        requests = build_all_requests(all_needs_llm)
        batch_id = submit_batch(requests, client)

    # ── Poll ──────────────────────────────────────────────────────────────
    poll_until_done(batch_id, client)

    # ── Apply results ─────────────────────────────────────────────────────
    logger.info("Applying results...")
    total_filled = apply_batch_results(batch_id, client, all_rows, all_needs_llm)
    logger.info("Stage 2: %d values filled", total_filled)

    for csv_path, fields, rows in zip(csv_paths, all_fields, all_rows):
        write_csv(csv_path, rows, fields)
        logger.info("Saved %d rows → %s", len(rows), csv_path.name)

    BATCH_ID_FILE.unlink(missing_ok=True)
    logger.info("Done. Batch ID file removed.")


if __name__ == "__main__":
    main()
