from __future__ import annotations

import json
import os
from typing import Any

import boto3
from geopy.distance import geodesic
from geopy.geocoders import Nominatim
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

# ── Semantic Score (Description) ───────────────────────────────────────────────

INDEX_NAME = "description"
EMBEDDING_MODEL_ID = "amazon.titan-embed-text-v2:0"
EMBEDDING_DIMS = 256
OPENSEARCH_ENDPOINT = "https://rwjzlgc3jmnsm1knq1w2.us-west-2.aoss.amazonaws.com"
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-west-2")

_bedrock = None
_os_client = None

def _init_aws_clients() -> bool:
    global _bedrock, _os_client
    if _bedrock is not None:
        return True
    try:
        session = boto3.Session()
        creds = session.get_credentials()
        if creds is None:
            return False
        frozen = creds.get_frozen_credentials()
        awsauth = AWS4Auth(
            frozen.access_key,
            frozen.secret_key,
            AWS_REGION,
            "aoss",
            session_token=frozen.token,
        )
        _bedrock = boto3.client("bedrock-runtime", region_name=AWS_REGION)
        _os_client = OpenSearch(
            hosts=[{"host": OPENSEARCH_ENDPOINT.removeprefix("https://"), "port": 443}],
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            timeout=30,
        )
        return True
    except Exception:
        return False

def _embed(text: str) -> list[float]:
    body = json.dumps({"inputText": text[:8000], "dimensions": EMBEDDING_DIMS, "normalize": True})
    response = _bedrock.invoke_model(modelId=EMBEDDING_MODEL_ID, body=body)
    return json.loads(response["body"].read())["embedding"]

def semantic_score_desc(query: str, candidates: list[dict]) -> dict[str, float]:
    """Returns a mapping of listing_id -> semantic similarity score for each candidate."""
    if not _init_aws_clients():
        raise RuntimeError("AWS credentials not available")
    K = 250
    query_vector = _embed(query)
    response = _os_client.search(
        index=INDEX_NAME,
        body={
            "size": K,
            "query": {
                "knn": {
                    "title_desc": {
                        "vector": query_vector,
                        "k": K,
                    }
                }
            },
            "_source": ["listing_id"],
        },
    )
    
    scores = {hit["_source"]["listing_id"]: hit["_score"] for hit in response["hits"]["hits"]}
    min_score = min(scores.values()) * 0.75
    return {c["listing_id"]: scores.get(c["listing_id"], min_score) for c in candidates}

# ── Distance Score ───────────────────────────────────────────────

def sort_by_proximity(target_location: str, candidates: list[dict]) -> list[dict]:
    """
    Returns candidates sorted by geodesic distance to target_location (closest first).
    Each candidate must have "location" (str), "lat" (float), "lon" (float).
    Each returned entry adds "distance_km": float.
    """
    geolocator = Nominatim(user_agent="robin-search")

    target_results = geolocator.geocode(target_location, exactly_one=False, country_codes="ch", limit=50)
 
    if not target_results:
        print(f"Could not geocode target: {target_location}")
        return candidates

    results = []
    for candidate in candidates:

        if candidate.get("latitude") is None or candidate.get("longitude") is None:
            results.append({**candidate, "close_to_distance_km": float("inf")})
            continue

        candidate_coords = (candidate["latitude"], candidate["longitude"])

        # Pick the geocode result closest to this specific candidate
        best_target = min(
            target_results,
            key=lambda t: geodesic(candidate_coords, (t.latitude, t.longitude)).km
        )

        dist = geodesic(candidate_coords, (best_target.latitude, best_target.longitude)).km
        results.append({**candidate, "close_to_distance_km": round(dist, 2)})

    return sorted(results, key=lambda x: x["close_to_distance_km"])

# ── Ranking ───────────────────────────────────────────────

def filter_soft_facts(
    candidates: list[dict[str, Any]],
    soft_facts: dict[str, Any],
) -> list[dict[str, Any]]:
    if len(candidates) == 0:
        return candidates
        
    ids_to_candidate = {c['listing_id']: c for c in candidates}
    raw_query = soft_facts['raw_query']
    embedding_scores = semantic_score_desc(raw_query, candidates)
    best_listing_ids = sorted(embedding_scores, key=embedding_scores.get, reverse=True)[:5]
    print("Len candidates", len(candidates))
    for i, listing_id in enumerate(best_listing_ids, 1):
        candidate = ids_to_candidate[listing_id]
        score = embedding_scores.get(listing_id, 0.0)
        print(f"#{i} [{listing_id}] {candidate['title']} (score: {score:.4f})")
        print(f"    {candidate['city']} | {candidate['price']} CHF | {candidate['rooms']} rooms")
        print(f"    {candidate['description'] or ''}")
        print()
        
        
    # if soft_facts.get("Close to"):
    #     target = soft_facts["Close to"]
    #     candidates = sort_by_proximity(target, candidates)

    # Intentionally stubbed. All hard-filtered candidates pass through.
    return candidates

