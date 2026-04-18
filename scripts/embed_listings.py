# """
# Embed all listings from data/listings.db and index them into Amazon OpenSearch Serverless.

# Prerequisites:
#   1. Create an OpenSearch Serverless collection (vector search type) in the AWS console.
#   2. Note the collection endpoint (e.g. https://xxxx.eu-central-2.aoss.amazonaws.com).
#   3. Set up a data access policy granting your IAM identity read/write on the collection.
#   4. Set environment variables:
#        OPENSEARCH_ENDPOINT   - the collection endpoint URL (no trailing slash)
#        AWS_REGION            - e.g. eu-central-2
#        AWS_ACCESS_KEY_ID     - your AWS key
#        AWS_SECRET_ACCESS_KEY - your AWS secret

# Usage:
#   uv run python scripts/embed_listings.py
# """

# from __future__ import annotations

# import json
# import os
# import sqlite3
# import threading
# from concurrent.futures import ThreadPoolExecutor, as_completed
# from pathlib import Path


# import boto3
# from opensearchpy import OpenSearch, RequestsHttpConnection, exceptions as os_exceptions
# from requests_aws4auth import AWS4Auth

# # ── Config ────────────────────────────────────────────────────────────────────

# DB_PATH = Path("data/listings.db")
# INDEX_NAME = "description"
# EMBEDDING_MODEL_ID = "amazon.titan-embed-text-v2:0"
# EMBEDDING_DIMS = 256
# BATCH_SIZE = 50          # listings per bulk request to OpenSearch
# MAX_WORKERS = 10         # parallel Bedrock embedding calls

# OPENSEARCH_ENDPOINT = "https://rwjzlgc3jmnsm1knq1w2.us-west-2.aoss.amazonaws.com"
# AWS_REGION = os.getenv("AWS__DEFAULT_REGION", "us-west-2")

# # ── AWS clients ───────────────────────────────────────────────────────────────

# session = boto3.Session()
# credentials = session.get_credentials().get_frozen_credentials()

# awsauth = AWS4Auth(
#     credentials.access_key,
#     credentials.secret_key,
#     AWS_REGION,
#     "aoss",
#     session_token=credentials.token,
# )

# # Each thread needs its own Bedrock client (boto3 clients are not thread-safe)
# _thread_local = threading.local()

# def _bedrock() -> object:
#     if not hasattr(_thread_local, "client"):
#         _thread_local.client = boto3.client("bedrock-runtime", region_name=AWS_REGION)
#     return _thread_local.client

# host = OPENSEARCH_ENDPOINT.removeprefix("https://").removeprefix("http://")
# os_client = OpenSearch(
#     hosts=[{"host": host, "port": 443}],
#     http_auth=awsauth,
#     use_ssl=True,
#     verify_certs=True,
#     connection_class=RequestsHttpConnection,
#     timeout=30,
# )

# # ── Index setup ───────────────────────────────────────────────────────────────

# INDEX_MAPPING = {
#     "settings": {"index.knn": True},
#     "mappings": {
#         "properties": {
#             "listing_id": {"type": "keyword"},
#             "title_desc": {
#                 "type": "knn_vector",
#                 "dimension": EMBEDDING_DIMS,
#                 "method": {
#                     "name": "hnsw",
#                     "engine": "nmslib",
#                     "space_type": "cosinesimil",
#                     "parameters": {},
#                 },
#             },
#         }
#     },
# }


# def ensure_index() -> None:
#     try:
#         os_client.indices.delete(index=INDEX_NAME)
#         print(f"Deleted existing index '{INDEX_NAME}'.")
#     except Exception:
#         pass
#     os_client.indices.create(index=INDEX_NAME, body=INDEX_MAPPING)
#     print(f"Created index '{INDEX_NAME}' with knn_vector mapping.")


# # ── Embedding ─────────────────────────────────────────────────────────────────

# def embed(text: str) -> list[float]:
#     body = json.dumps({"inputText": text[:8000], "dimensions": EMBEDDING_DIMS, "normalize": True})
#     response = _bedrock().invoke_model(modelId=EMBEDDING_MODEL_ID, body=body)
#     return json.loads(response["body"].read())["embedding"]


# def embed_listing(listing: dict) -> dict | None:
#     text = " ".join(filter(None, [listing["title"], listing["description"]]))
#     if not text.strip():
#         return None
#     return {"listing_id": listing["listing_id"], "title_desc": embed(text)}


# # ── Main ──────────────────────────────────────────────────────────────────────

# def load_listings() -> list[dict]:
#     con = sqlite3.connect(DB_PATH)
#     con.row_factory = sqlite3.Row
#     rows = con.execute("SELECT listing_id, title, description FROM listings").fetchall()
#     con.close()
#     return [dict(r) for r in rows]


# def bulk_index(docs: list[dict]) -> None:
#     body = []
#     for doc in docs:
#         body.append({"index": {"_index": INDEX_NAME}})
#         body.append({"listing_id": doc["listing_id"], "title_desc": doc["title_desc"]})
#     response = os_client.bulk(body=body)
#     if response.get("errors"):
#         for item in response["items"]:
#             if "error" in item.get("index", {}):
#                 print(f"  Bulk error: {item['index']['error']}")


# def main() -> None:
#     ensure_index()
#     listings = load_listings()
#     print(f"Loaded {len(listings)} listings from {DB_PATH}.")
#     print(f"Embedding with {MAX_WORKERS} parallel workers...")

#     batch: list[dict] = []
#     indexed = 0

#     with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#         futures = {executor.submit(embed_listing, listing): listing for listing in listings}
#         for future in as_completed(futures):
#             result = future.result()
#             if result is None:
#                 continue
#             batch.append(result)
#             if len(batch) >= BATCH_SIZE:
#                 bulk_index(batch)
#                 indexed += len(batch)
#                 print(f"  Indexed {indexed}/{len(listings)}...")
#                 batch = []

#     if batch:
#         bulk_index(batch)
#         indexed += len(batch)

#     print(f"Done. {indexed} listings indexed into '{INDEX_NAME}'.")


# if __name__ == "__main__":
#     main()
