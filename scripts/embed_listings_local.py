#!/usr/bin/env python3
"""Embed all listing titles+descriptions and store in ChromaDB.

Run once from the project root after the database has been bootstrapped:
    pip install -e ".[local-embeddings]"
    python scripts/embed_listings_local.py
"""
from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

DB_PATH = Path(os.getenv("LISTINGS_DB_PATH", "data/listings.db"))
CHROMA_PATH = Path(os.getenv("CHROMA_DB_PATH", "data/chroma"))
MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
COLLECTION_NAME = "listings"
BATCH_SIZE = 256


def main() -> None:
    if not DB_PATH.exists():
        print(f"Database not found at {DB_PATH}. Start the API once to bootstrap it.")
        sys.exit(1)

    print(f"Loading model {MODEL_NAME} (downloaded on first run) ...")
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer(MODEL_NAME)

    print(f"Opening ChromaDB at {CHROMA_PATH} ...")
    import chromadb
    CHROMA_PATH.mkdir(parents=True, exist_ok=True)
    client = chromadb.PersistentClient(path=str(CHROMA_PATH))

    try:
        client.delete_collection(COLLECTION_NAME)
        print("Dropped existing collection.")
    except Exception:
        pass

    collection = client.create_collection(
        COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"},
    )

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT listing_id, title, description FROM listings").fetchall()
    total = len(rows)
    print(f"Embedding {total} listings ...")

    for i in range(0, total, BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        texts = [f"{row['title']} {row['description'] or ''}".strip() for row in batch]
        ids = [str(row["listing_id"]) for row in batch]
        embeddings = model.encode(texts, normalize_embeddings=True).tolist()
        collection.add(ids=ids, embeddings=embeddings)
        print(f"  {min(i + BATCH_SIZE, total)}/{total}")

    conn.close()
    print(f"Done. {total} listings embedded and stored in {CHROMA_PATH}")


if __name__ == "__main__":
    main()
