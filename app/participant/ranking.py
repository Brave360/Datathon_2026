from __future__ import annotations

import json
import logging
import math
import re
from collections import Counter
from typing import Any

from geopy.distance import geodesic
from geopy.geocoders import Nominatim

from app.models.schemas import ListingData, RankedListingResult

LOGGER = logging.getLogger(__name__)

# Base weights used when every component has signal; rebalanced dynamically otherwise
_BASE_WEIGHTS = {
    "text":    0.30,
    "poi":     0.35,
    "feature": 0.20,
    "numeric": 0.15,
}

_MAX_CANDIDATES = 200

_BOOLEAN_FEATURE_FIELDS = [
    "feature_balcony", "feature_elevator", "feature_parking", "feature_garage",
    "feature_fireplace", "feature_child_friendly", "feature_pets_allowed",
    "feature_temporary", "feature_new_build", "feature_wheelchair_accessible",
    "feature_private_laundry", "feature_minergie_certified", "is_furnished",
]

_STOP_WORDS = {
    "a", "an", "the", "and", "or", "of", "in", "to", "for", "with", "on",
    "at", "by", "from", "is", "it", "its", "be", "are", "was", "were",
    "i", "we", "you", "my", "me", "ich", "wir", "die", "der", "das", "den",
    "ein", "eine", "und", "oder", "mit", "von", "zu", "in", "im", "am",
    "les", "le", "la", "de", "du", "et", "un", "une", "pour", "avec",
}

# Attempt to load AWS semantic search; falls back to keyword matching if unavailable
try:
    from app.participant.soft_filtering import semantic_score_desc as _aws_semantic_score
    _HAS_SEMANTIC = True
    LOGGER.info("AWS semantic search loaded successfully")
except Exception as _e:
    _aws_semantic_score = None  # type: ignore[assignment]
    _HAS_SEMANTIC = False
    LOGGER.info("AWS semantic search unavailable (%s), using keyword fallback", _e)


def rank_listings(
    candidates: list[dict[str, Any]],
    soft_facts: dict[str, Any],
    query_text: str = "",
    component_weights: dict[str, float] | None = None,
) -> list[RankedListingResult]:
    if not candidates:
        return []

    pool = candidates[:_MAX_CANDIDATES] if len(candidates) > _MAX_CANDIDATES else candidates

    # Build text scores once for the whole pool
    text_scores = _build_text_scores(pool, query_text, soft_facts)
    poi_locations = _geocode_pois(soft_facts.get("points_of_interest") or [])

    scored: list[tuple[float, dict[str, float], dict[str, Any]]] = []
    for candidate in pool:
        s, breakdown = _score(candidate, soft_facts, text_scores, poi_locations, component_weights or {})
        scored.append((s, breakdown, candidate))

    scored.sort(key=lambda x: x[0], reverse=True)

    return [
        RankedListingResult(
            listing_id=str(c["listing_id"]),
            score=round(s, 4),
            reason=_format_breakdown(breakdown),
            listing=_to_listing_data(c),
        )
        for s, breakdown, c in scored
    ]


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def _build_text_scores(
    pool: list[dict[str, Any]],
    query_text: str,
    soft_facts: dict[str, Any],
) -> dict[Any, float]:
    """Returns listing_id -> text relevance score [0, 1] for all candidates."""
    if _HAS_SEMANTIC and query_text:
        try:
            raw = _aws_semantic_score(query_text, pool)
            # Normalize to [0, 1]: divide by the max observed score
            if raw:
                max_s = max(raw.values()) or 1.0
                return {lid: s / max_s for lid, s in raw.items()}
        except Exception as exc:
            LOGGER.warning("Semantic scoring failed, falling back to keywords: %s", exc)

    # Keyword fallback
    query_terms = _tokenize(query_text) if query_text else _terms_from_soft(soft_facts)
    return {c["listing_id"]: _keyword_score(c, query_terms) for c in pool}


def _score(
    candidate: dict[str, Any],
    soft_facts: dict[str, Any],
    text_scores: dict[Any, float],
    poi_locations: list[tuple[float, float, float, str]],
    component_weights: dict[str, float],
) -> tuple[float, dict[str, float]]:
    has_features = bool({f for f in _BOOLEAN_FEATURE_FIELDS if soft_facts.get(f) is not None})
    has_numeric = any(
        soft_facts.get(k) is not None
        for k in (
            "min_price",
            "max_price",
            "min_rooms",
            "max_rooms",
            "min_area",
            "max_area",
            "min_bedrooms",
            "max_bedrooms",
            "min_bathrooms",
            "max_bathrooms",
            "min_year_built",
            "max_year_built",
        )
    )
    has_poi = bool(poi_locations)
    # text always has signal (semantic scores vary across candidates)

    active = {
        "text":    True,
        "poi":     has_poi,
        "feature": has_features,
        "numeric": has_numeric,
    }
    raw_weights = {
        key: (_normalized_component_weight(component_weights, key) if active[key] else 0.0)
        for key in active
    }
    if all(weight == 0.0 for weight in raw_weights.values()):
        raw_weights = {
            key: (_BASE_WEIGHTS[key] if active[key] else 0.0)
            for key in active
        }
    weight_sum = sum(raw_weights.values())
    weights = {k: (raw_weights[k] / weight_sum if active[k] and weight_sum else 0.0) for k in active}

    text_s    = text_scores.get(candidate["listing_id"], 0.5)
    poi_s     = _poi_score(candidate, poi_locations) if has_poi else None
    feature_s = _feature_score(candidate, soft_facts) if has_features else None
    numeric_s = _numeric_score(candidate, soft_facts) if has_numeric else None

    total = weights["text"] * text_s
    if poi_s     is not None: total += weights["poi"]     * poi_s
    if feature_s is not None: total += weights["feature"] * feature_s
    if numeric_s is not None: total += weights["numeric"] * numeric_s

    breakdown = {
        "text":    text_s,
        **({"poi":     poi_s}     if poi_s     is not None else {}),
        **({"feature": feature_s} if feature_s is not None else {}),
        **({"numeric": numeric_s} if numeric_s is not None else {}),
    }
    return total, breakdown


def _feature_score(candidate: dict[str, Any], soft_facts: dict[str, Any]) -> float:
    desired = {f: v for f in _BOOLEAN_FEATURE_FIELDS if (v := soft_facts.get(f)) is not None}
    if not desired:
        return 1.0
    hits = sum(1 for f, v in desired.items() if bool(candidate.get(f)) == bool(v))
    return hits / len(desired)


def _numeric_score(
    candidate: dict[str, Any],
    soft_facts: dict[str, Any],
) -> float:
    components: list[float] = []

    price = candidate.get("price")
    max_price = soft_facts.get("max_price")
    min_price = soft_facts.get("min_price")
    if price is not None and (max_price is not None or min_price is not None):
        target = max_price or min_price
        components.append(_gaussian(price, target, scale=max(target * 0.3, 1.0)))

    rooms = candidate.get("rooms")
    min_rooms = soft_facts.get("min_rooms")
    max_rooms = soft_facts.get("max_rooms")
    if rooms is not None and (min_rooms is not None or max_rooms is not None):
        target = min_rooms or max_rooms
        components.append(_gaussian(rooms, target, scale=2.0))

    area = candidate.get("area")
    min_area = soft_facts.get("min_area")
    max_area = soft_facts.get("max_area")
    if area is not None and (min_area is not None or max_area is not None):
        target = min_area or max_area
        components.append(_gaussian(area, target, scale=max(target * 0.3, 1.0)))

    bedrooms = candidate.get("bedrooms")
    min_bedrooms = soft_facts.get("min_bedrooms")
    max_bedrooms = soft_facts.get("max_bedrooms")
    if bedrooms is not None and (min_bedrooms is not None or max_bedrooms is not None):
        target = min_bedrooms or max_bedrooms
        components.append(_gaussian(bedrooms, target, scale=1.5))

    bathrooms = candidate.get("bathrooms")
    min_bathrooms = soft_facts.get("min_bathrooms")
    max_bathrooms = soft_facts.get("max_bathrooms")
    if bathrooms is not None and (min_bathrooms is not None or max_bathrooms is not None):
        target = min_bathrooms or max_bathrooms
        components.append(_gaussian(bathrooms, target, scale=1.0))

    year_built = candidate.get("year_built")
    min_year_built = soft_facts.get("min_year_built")
    max_year_built = soft_facts.get("max_year_built")
    if year_built is not None and (min_year_built is not None or max_year_built is not None):
        target = min_year_built or max_year_built
        components.append(_gaussian(year_built, target, scale=25.0))

    return sum(components) / len(components) if components else 1.0


def _gaussian(value: float, target: float, scale: float) -> float:
    if scale <= 0:
        return 1.0
    return math.exp(-((value - target) ** 2) / (2 * scale ** 2))


def _normalized_component_weight(component_weights: dict[str, float], key: str) -> float:
    raw = component_weights.get(key, _BASE_WEIGHTS.get(key, 1.0))
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return _BASE_WEIGHTS.get(key, 1.0)
    return max(0.0, min(2.0, value))


def _poi_score(
    candidate: dict[str, Any],
    poi_locations: list[tuple[float, float, float, str]],
) -> float:
    if not poi_locations:
        return 1.0

    lat = candidate.get("latitude")
    lon = candidate.get("longitude")
    if lat is None or lon is None:
        return 0.0

    components: list[float] = []
    for poi_lat, poi_lon, radius_km, _weight_key in poi_locations:
        dist = geodesic((lat, lon), (poi_lat, poi_lon)).km
        score = 1.0 / (1.0 + (dist / max(radius_km, 0.1)) ** 2)
        components.append(score)

    return sum(components) / len(components) if components else 1.0


def _keyword_score(candidate: dict[str, Any], query_terms: Counter) -> float:
    if not query_terms:
        return 1.0
    text = " ".join(filter(None, [candidate.get("title"), candidate.get("description")]))
    listing_terms = _tokenize(text)
    if not listing_terms:
        return 0.0
    overlap = sum(min(query_terms[t], listing_terms[t]) for t in query_terms)
    return min(1.0, overlap / sum(query_terms.values()))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _tokenize(text: str) -> Counter:
    tokens = re.findall(r"[a-zäöüéàâêèùîïôûçæœß]+", text.lower())
    return Counter(t for t in tokens if t not in _STOP_WORDS and len(t) > 2)


def _terms_from_soft(soft_facts: dict[str, Any]) -> Counter:
    parts: list[str] = []
    for key in ("city", "canton", "postal_code", "object_category", "offer_type"):
        val = soft_facts.get(key)
        if val:
            parts.append(str(val))
    pois = soft_facts.get("points_of_interest") or []
    for poi in pois:
        if isinstance(poi, dict):
            parts.append(poi.get("query", ""))
            parts.append(poi.get("type", ""))
    return _tokenize(" ".join(parts))


def _clean_poi_query(query: str) -> str:
    return re.sub(r"'s?\b", "", query).strip()


def _geocode_pois(pois: list[dict[str, Any]]) -> list[tuple[float, float, float, str]]:
    if not pois:
        return []
    geolocator = Nominatim(user_agent="datathon2026-ranker")
    results: list[tuple[float, float, float, str]] = []
    for index, poi in enumerate(pois):
        query = poi.get("query", "")
        radius_km = float(poi.get("radius_km", 1.0))
        weight_key = f"poi:{index}:radius_km"
        if not query:
            continue
        try:
            loc = geolocator.geocode(_clean_poi_query(query), country_codes="ch", timeout=5)
            if loc:
                results.append((loc.latitude, loc.longitude, radius_km, weight_key))
        except Exception:
            pass
    return results


def _format_breakdown(breakdown: dict[str, float]) -> str:
    return " | ".join(f"{k}={v:.2f}" for k, v in breakdown.items())


# ---------------------------------------------------------------------------
# Data mapping
# ---------------------------------------------------------------------------

def _to_listing_data(candidate: dict[str, Any]) -> ListingData:
    return ListingData(
        id=str(candidate["listing_id"]),
        title=candidate["title"],
        description=candidate.get("description"),
        street=candidate.get("street"),
        city=candidate.get("city"),
        postal_code=candidate.get("postal_code"),
        canton=candidate.get("canton"),
        latitude=candidate.get("latitude"),
        longitude=candidate.get("longitude"),
        price_chf=candidate.get("price"),
        rooms=candidate.get("rooms"),
        living_area_sqm=_coerce_int(candidate.get("area")),
        available_from=candidate.get("available_from"),
        image_urls=_coerce_image_urls(candidate.get("image_urls")),
        hero_image_url=candidate.get("hero_image_url"),
        original_listing_url=candidate.get("original_url"),
        features=candidate.get("features") or [],
        offer_type=candidate.get("offer_type"),
        object_category=candidate.get("object_category"),
        object_type=candidate.get("object_type"),
    )


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return None


def _coerce_image_urls(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return [value]
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
    return None
