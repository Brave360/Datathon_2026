"""
Search listings by image similarity using a natural language text query.

Embeds the query text using amazon.titan-embed-image-v1 (text-only mode),
which produces embeddings in the same space as image embeddings, enabling
cross-modal search.

Usage:
  uv run python scripts/image_search.py "bright living room with lake view"
  uv run python scripts/image_search.py "modern kitchen" --limit 10
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

DB_PATH = Path("data/listings.db")

# ── Config ────────────────────────────────────────────────────────────────────

INDEX_NAME = "images"
EMBEDDING_MODEL_ID = "amazon.titan-embed-image-v1"
EMBEDDING_DIMS = 256

OPENSEARCH_ENDPOINT = "https://rwjzlgc3jmnsm1knq1w2.us-west-2.aoss.amazonaws.com"
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-west-2")

# ── AWS clients ───────────────────────────────────────────────────────────────

_session = boto3.Session()
_credentials = _session.get_credentials().get_frozen_credentials()
_awsauth = AWS4Auth(
    _credentials.access_key,
    _credentials.secret_key,
    AWS_REGION,
    "aoss",
    session_token=_credentials.token,
)

_bedrock = boto3.client("bedrock-runtime", region_name=AWS_REGION)

_os_client = OpenSearch(
    hosts=[{"host": OPENSEARCH_ENDPOINT.removeprefix("https://"), "port": 443}],
    http_auth=_awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    timeout=30,
)

# ── Embedding ─────────────────────────────────────────────────────────────────

def embed_text(text: str) -> list[float]:
    body = json.dumps({
        "inputText": text[:512],
        "embeddingConfig": {"outputEmbeddingLength": EMBEDDING_DIMS},
    })
    response = _bedrock.invoke_model(modelId=EMBEDDING_MODEL_ID, body=body)
    return json.loads(response["body"].read())["embedding"]

# ── Search ────────────────────────────────────────────────────────────────────

def search_by_image(query_vector: list[float], limit: int) -> list[tuple[str, float]]:
    response = _os_client.search(
        index=INDEX_NAME,
        body={
            "size": limit,
            "query": {
                "knn": {
                    "image_embedding": {
                        "vector": query_vector,
                        "k": limit,
                    }
                }
            },
            "_source": ["listing_id"],
        },
    )
    return [(hit["_source"]["listing_id"], hit["_score"]) for hit in response["hits"]["hits"]]

# ── Main ──────────────────────────────────────────────────────────────────────

def fetch_image_urls(listing_ids: list[str]) -> dict[str, list[str]]:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    placeholders = ", ".join("?" for _ in listing_ids)
    rows = con.execute(
        f"SELECT listing_id, images_json FROM listings WHERE listing_id IN ({placeholders})",
        listing_ids,
    ).fetchall()
    con.close()
    result = {}
    for row in rows:
        urls = []
        if row["images_json"]:
            try:
                parsed = json.loads(row["images_json"])
                for item in parsed.get("images", []):
                    url = item.get("url") if isinstance(item, dict) else item
                    if url and (url.startswith("http") or url.startswith("s3://")):
                        urls.append(url)
            except json.JSONDecodeError:
                pass
        result[row["listing_id"]] = urls
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Search listings by image using a text query.")
    parser.add_argument("query", help="Natural-language query describing the image.")
    parser.add_argument("--limit", type=int, default=5, help="Number of results to return.")
    args = parser.parse_args()

    print(f"Embedding query: '{args.query}'...")
    query_vector = embed_text(args.query)

    print(f"Searching top {args.limit} listings by image similarity...\n")
    results = search_by_image(query_vector, args.limit)

    listing_ids = [lid for lid, _ in results]
    image_urls = fetch_image_urls(listing_ids)

    for i, (listing_id, score) in enumerate(results, 1):
        print(f"#{i} {listing_id} (score: {score:.4f})")
        for url in image_urls.get(listing_id, [])[:3]:
            print(f"    {url}")
        print()


if __name__ == "__main__":
    main()
