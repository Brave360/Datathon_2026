from __future__ import annotations

from typing import Any
from geopy.distance import geodesic
from geopy.geocoders import Nominatim


def filter_soft_facts(
    candidates: list[dict[str, Any]],
    soft_facts: dict[str, Any],
) -> list[dict[str, Any]]:
    
    # print(candidates[0])
    # print(candidates[0]['latitude'], candidates[0]['longitude'])
    # print(candidates[1]['latitude'], candidates[1]['longitude'])
    # print(candidates[2]['latitude'], candidates[2]['longitude'])
    # print(candidates[3]['latitude'], candidates[3]['longitude'])
   
    if soft_facts.get("Close to"):
        target = soft_facts["Close to"]
        candidates = sort_by_proximity(target, candidates)

    # Intentionally stubbed. All hard-filtered candidates pass through.
    return candidates


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
