"""
Embed listing hero images into Amazon OpenSearch Serverless using Titan Multimodal Embeddings.

For each listing, downloads the first image from its S3 URL, base64-encodes it,
and calls amazon.titan-embed-image-v1 to produce a 1024-dim embedding.

Usage:
  uv run python scripts/embed_images.py
  uv run python scripts/embed_images.py --limit 100
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import boto3
import httpx
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

# ── Config ────────────────────────────────────────────────────────────────────

DB_PATH = Path("data/listings.db")
INDEX_NAME = "images"
EMBEDDING_MODEL_ID = "amazon.titan-embed-image-v1"
EMBEDDING_DIMS = 256
BATCH_SIZE = 50
MAX_WORKERS = 10   # lower than text — image downloads add latency

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

_thread_local = threading.local()

def _bedrock():
    if not hasattr(_thread_local, "client"):
        _thread_local.client = boto3.client("bedrock-runtime", region_name=AWS_REGION)
    return _thread_local.client

host = OPENSEARCH_ENDPOINT.removeprefix("https://")
_os_client = OpenSearch(
    hosts=[{"host": host, "port": 443}],
    http_auth=_awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    timeout=30,
)

# ── Index setup ───────────────────────────────────────────────────────────────

INDEX_MAPPING = {
    "settings": {"index.knn": True},
    "mappings": {
        "properties": {
            "listing_id": {"type": "keyword"},
            "image_embedding": {
                "type": "knn_vector",
                "dimension": EMBEDDING_DIMS,
                "method": {
                    "name": "hnsw",
                    "engine": "nmslib",
                    "space_type": "cosinesimil",
                    "parameters": {},
                },
            },
        }
    },
}


def ensure_index() -> None:
    try:
        _os_client.indices.delete(index=INDEX_NAME)
        print(f"Deleted existing index '{INDEX_NAME}'.")
    except Exception:
        pass
    _os_client.indices.create(index=INDEX_NAME, body=INDEX_MAPPING)
    print(f"Created index '{INDEX_NAME}' with knn_vector mapping.")


# ── Image helpers ─────────────────────────────────────────────────────────────

_s3_client = boto3.client("s3", region_name=AWS_REGION)


def extract_image_urls(images_json: str | None) -> list[str]:
    if not images_json:
        return []
    try:
        parsed = json.loads(images_json)
    except json.JSONDecodeError:
        return []
    urls = []
    for item in parsed.get("images", []):
        url = item.get("url") if isinstance(item, dict) else item if isinstance(item, str) else None
        if url and (url.startswith("http") or url.startswith("s3://")):
            urls.append(url)
    return urls


def download_image_b64(url: str) -> str | None:
    try:
        if url.startswith("s3://"):
            # Parse s3://bucket/key
            without_prefix = url[5:]
            bucket, _, key = without_prefix.partition("/")
            obj = _s3_client.get_object(Bucket=bucket, Key=key)
            data = obj["Body"].read()
        else:
            response = httpx.get(url, timeout=10, follow_redirects=True)
            response.raise_for_status()
            data = response.content
        return base64.b64encode(data).decode("utf-8")
    except Exception as e:
        print(f"  Failed to download {url}: {e}")
        return None


# ── Embedding ─────────────────────────────────────────────────────────────────

def embed_image(image_b64: str) -> list[float]:
    body = json.dumps({
        "inputImage": image_b64,
        "embeddingConfig": {"outputEmbeddingLength": EMBEDDING_DIMS},
    })
    response = _bedrock().invoke_model(modelId=EMBEDDING_MODEL_ID, body=body)
    return json.loads(response["body"].read())["embedding"]


def process_listing(listing: dict) -> dict | None:
    for url in extract_image_urls(listing["images_json"]):
        image_b64 = download_image_b64(url)
        if image_b64:
            vector = embed_image(image_b64)
            return {"listing_id": listing["listing_id"], "image_embedding": vector}
    return None


# ── OpenSearch ────────────────────────────────────────────────────────────────

def bulk_index(docs: list[dict]) -> None:
    body = []
    for doc in docs:
        body.append({"index": {"_index": INDEX_NAME}})
        body.append({"listing_id": doc["listing_id"], "image_embedding": doc["image_embedding"]})
    response = _os_client.bulk(body=body)
    if response.get("errors"):
        for item in response["items"]:
            if "error" in item.get("index", {}):
                print(f"  Bulk error: {item['index']['error']}")


# ── Main ──────────────────────────────────────────────────────────────────────

def load_listings(limit: int | None) -> list[dict]:
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    sql = "SELECT listing_id, images_json FROM listings WHERE images_json IS NOT NULL AND images_json != ''"
    if limit:
        sql += f" LIMIT {limit}"
    rows = con.execute(sql).fetchall()
    con.close()
    return [dict(r) for r in rows]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None, help="Max listings to process.")
    args = parser.parse_args()

    ensure_index()
    listings = load_listings(args.limit)
    print(f"Loaded {len(listings)} listings with images.")
    print(f"Embedding with {MAX_WORKERS} parallel workers...")

    batch: list[dict] = []
    indexed = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_listing, listing): listing for listing in listings}
        for future in as_completed(futures):
            result = future.result()
            if result is None:
                continue
            batch.append(result)
            if len(batch) >= BATCH_SIZE:
                bulk_index(batch)
                indexed += len(batch)
                print(f"  Indexed {indexed}/{len(listings)}...")
                batch = []

    if batch:
        bulk_index(batch)
        indexed += len(batch)

    print(f"Done. {indexed} image embeddings indexed into '{INDEX_NAME}'.")


if __name__ == "__main__":
    main()
