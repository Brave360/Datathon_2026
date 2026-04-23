from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger(__name__)

_MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
_COLLECTION_NAME = "listings"
_CHROMA_PATH = Path(os.getenv("CHROMA_DB_PATH", "data/chroma"))

_model = None
_collection = None


def _load_model():
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer
        LOGGER.info("Loading local embedding model %s", _MODEL_NAME)
        _model = SentenceTransformer(_MODEL_NAME)
    return _model


def _load_collection():
    global _collection
    if _collection is not None:
        return _collection
    try:
        import chromadb
        if not _CHROMA_PATH.exists():
            LOGGER.info("ChromaDB path %s not found — run scripts/embed_listings_local.py first", _CHROMA_PATH)
            return None
        client = chromadb.PersistentClient(path=str(_CHROMA_PATH))
        _collection = client.get_collection(_COLLECTION_NAME)
        LOGGER.info("Loaded ChromaDB collection '%s' (%d entries)", _COLLECTION_NAME, _collection.count())
        return _collection
    except Exception as exc:
        LOGGER.warning("ChromaDB collection unavailable: %s", exc)
        return None


def embed(text: str) -> list[float]:
    return _load_model().encode(text, normalize_embeddings=True).tolist()


def semantic_score_local(query: str, candidates: list[dict[str, Any]]) -> dict[str, float]:
    collection = _load_collection()
    if collection is None:
        return {}
    try:
        query_embedding = embed(query)
        K = min(collection.count(), 250)
        if K == 0:
            return {}
        results = collection.query(query_embeddings=[query_embedding], n_results=K)
        # cosine distance → similarity (1 - distance)
        scores: dict[str, float] = {
            doc_id: 1.0 - distance
            for doc_id, distance in zip(results["ids"][0], results["distances"][0])
        }
        min_score = min(scores.values()) * 0.75 if scores else 0.0
        return {c["listing_id"]: scores.get(str(c["listing_id"]), min_score) for c in candidates}
    except Exception as exc:
        LOGGER.warning("Local semantic scoring failed: %s", exc)
        return {}
