"""
Semantic search over listings using OpenSearch vector search.

Usage:
  uv run python scripts/semantic_search.py "bright apartment in Zurich with balcony"
  uv run python scripts/semantic_search.py "ruhige Wohnung in Bern" --limit 10
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from pathlib import Path

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

# ── Config ────────────────────────────────────────────────────────────────────

DB_PATH = Path("data/listings.db")
INDEX_NAME = "description"
EMBEDDING_MODEL_ID = "amazon.titan-embed-text-v2:0"
EMBEDDING_DIMS = 256

OPENSEARCH_ENDPOINT = "https://rwjzlgc3jmnsm1knq1w2.us-west-2.aoss.amazonaws.com"
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-west-2")

# ── AWS clients ───────────────────────────────────────────────────────────────

session = boto3.Session()
credentials = session.get_credentials().get_frozen_credentials()

awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    AWS_REGION,
    "aoss",
    session_token=credentials.token,
)

bedrock = boto3.client("bedrock-runtime", region_name=AWS_REGION)

host = OPENSEARCH_ENDPOINT.removeprefix("https://").removeprefix("http://")
os_client = OpenSearch(
    hosts=[{"host": host, "port": 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    timeout=30,
)

# ── Embedding ─────────────────────────────────────────────────────────────────

def embed(text: str) -> list[float]:
    body = json.dumps({"inputText": text[:8000], "dimensions": EMBEDDING_DIMS, "normalize": True})
    response = bedrock.invoke_model(modelId=EMBEDDING_MODEL_ID, body=body)
    return json.loads(response["body"].read())["embedding"]

# ── Search ────────────────────────────────────────────────────────────────────

def vector_search(query_vector: list[float], limit: int) -> list[str]:
    response = os_client.search(
        index=INDEX_NAME,
        body={
            "size": limit,
            "query": {
                "knn": {
                    "title_desc": {
                        "vector": query_vector,
                        "k": limit,
                    }
                }
            },
            "_source": ["listing_id"],
        },
    )
    return [(hit["_source"]["listing_id"], hit["_score"]) for hit in response["hits"]["hits"]]

# ── SQLite fetch ──────────────────────────────────────────────────────────────

def fetch_listings(listing_ids: list[str]) -> list[dict]:
    if not listing_ids:
        return []
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    placeholders = ", ".join("?" for _ in listing_ids)
    rows = con.execute(
        f"SELECT listing_id, title, city, price, rooms, description FROM listings WHERE listing_id IN ({placeholders})",
        listing_ids,
    ).fetchall()
    con.close()
    row_map = {row["listing_id"]: dict(row) for row in rows}
    return [row_map[lid] for lid in listing_ids if lid in row_map]

# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Semantic listing search.")
    parser.add_argument("query", help="Natural-language search query.")
    parser.add_argument("--limit", type=int, default=1, help="Number of results to return.")
    args = parser.parse_args()

    print(f"Embedding query: '{args.query}'...")
    query_vector = embed(args.query)

    print(f"Searching top {args.limit} listings...")
    hits = vector_search(query_vector, args.limit)
    listing_ids = [lid for lid, _ in hits]
    scores = {lid: score for lid, score in hits}

    listings = fetch_listings(listing_ids)

    print(f"\nTop {len(listings)} results:\n")
    for i, listing in enumerate(listings, 1):
        score = scores.get(listing["listing_id"], 0.0)
        print(f"#{i} [{listing['listing_id']}] {listing['title']} (score: {score:.4f})")
        print(f"    {listing['city']} | {listing['price']} CHF | {listing['rooms']} rooms")
        print(f"    {listing['description'] or ''}")
        print()


if __name__ == "__main__":
    main()
