from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from app.core.hard_filters import HardFilterParams, search_listings
from app.models.schemas import ConversationTurn, HardFilters, ListingsResponse
from app.participant.hard_filter import search_with_relaxation
from app.participant.query_parser import parse_query
from app.participant.ranking import rank_listings
from app.participant.soft_fact_extraction import extract_soft_facts
from app.participant.soft_filtering import filter_soft_facts

LOGGER = logging.getLogger(__name__)


def filter_hard_facts(db_path: Path, hard_facts: HardFilters) -> list[dict[str, Any]]:
    return search_listings(db_path, to_hard_filter_params(hard_facts))


def query_from_text(
    *,
    db_path: Path,
    query: str,
    conversation: list[ConversationTurn],
    soft_preference_weights: dict[str, float] | None,
    limit: int,
    offset: int,
) -> ListingsResponse:
    LOGGER.info(
        "query_from_text start query=%r conversation_turns=%s limit=%s offset=%s",
        query[:200],
        len(conversation),
        limit,
        offset,
    )
    parsed = parse_query(query, conversation=conversation)
    LOGGER.info("query_from_text parsed hard=%s", parsed.hard_requirements.model_dump(exclude_none=True))
    LOGGER.info("query_from_text parsed soft=%s", parsed.soft_requirements.model_dump(exclude_none=True))

    result = search_with_relaxation(
        db_path,
        parsed.hard_requirements,
        parsed.soft_requirements,
    )
    if result.relaxation_log:
        LOGGER.info("query_from_text relaxation_log=%s", result.relaxation_log)
    LOGGER.info("query_from_text candidates=%s total=%s", len(result.listings), result.total_before_page)

    soft_dict = result.effective_soft.model_dump(exclude_none=True)
    # POIs can't be SQL-filtered; rescue any that the parser placed in hard
    hard_pois = result.effective_hard.points_of_interest
    soft_pois = soft_dict.get("points_of_interest") or []
    if hard_pois:
        soft_dict["points_of_interest"] = hard_pois + soft_pois
        LOGGER.info("query_from_text merged %s hard POIs into soft for ranking", len(hard_pois))

    current_soft_preference_weights = _sanitize_score_component_weights(soft_preference_weights or {})
    ranked = rank_listings(
        result.listings,
        soft_dict,
        query_text=query,
        component_weights=current_soft_preference_weights,
    )
    ranked = ranked[offset : offset + limit]
    LOGGER.info("query_from_text ranked_results=%s (limit=%s offset=%s)", len(ranked), limit, offset)
    effective_hard_filters = result.effective_hard.model_dump(exclude_none=True)
    effective_soft_filters = result.effective_soft.model_dump(exclude_none=True)
    return ListingsResponse(
        listings=ranked,
        meta={
            "effective_hard_filters": effective_hard_filters,
            "effective_soft_filters": effective_soft_filters,
            "score_component_weights": current_soft_preference_weights,
            "score_weight_controls": build_score_weight_controls(
                effective_soft_filters=effective_soft_filters,
                current_weights=current_soft_preference_weights,
            ),
            "assistant_summary": build_assistant_summary(
                effective_hard_filters=effective_hard_filters,
                effective_soft_filters=effective_soft_filters,
                result_count=len(ranked),
            ),
            "relaxation_log": result.relaxation_log,
            "total_before_page": result.total_before_page,
            "conversation_turn_count": len(conversation) + 1,
        },
    )


def query_from_filters(
    *,
    db_path: Path,
    hard_facts: HardFilters | None,
) -> ListingsResponse:
    structured_hard_facts = hard_facts or HardFilters()
    LOGGER.info("query_from_filters hard_facts=%s", structured_hard_facts.model_dump())
    soft_facts = extract_soft_facts("")
    candidates = filter_hard_facts(db_path, structured_hard_facts)
    LOGGER.info("query_from_filters hard_filter_candidates=%s", len(candidates))
    candidates = filter_soft_facts(candidates, soft_facts)
    LOGGER.info("query_from_filters post_soft_filter_candidates=%s", len(candidates))
    ranked = rank_listings(candidates, soft_facts)
    LOGGER.info("query_from_filters ranked_results=%s", len(ranked))
    return ListingsResponse(
        listings=ranked,
        meta={},
    )


def to_hard_filter_params(hard_facts: HardFilters) -> HardFilterParams:
    return HardFilterParams(
        city=hard_facts.city,
        postal_code=hard_facts.postal_code,
        canton=hard_facts.canton,
        min_price=hard_facts.min_price,
        max_price=hard_facts.max_price,
        min_rooms=hard_facts.min_rooms,
        max_rooms=hard_facts.max_rooms,
        min_area_sqm=hard_facts.min_area_sqm,
        max_area_sqm=hard_facts.max_area_sqm,
        latitude=hard_facts.latitude,
        longitude=hard_facts.longitude,
        radius_km=hard_facts.radius_km,
        features=hard_facts.features,
        offer_type=hard_facts.offer_type,
        object_category=hard_facts.object_category,
        limit=hard_facts.limit,
        offset=hard_facts.offset,
        sort_by=hard_facts.sort_by,
    )


def build_assistant_summary(
    *,
    effective_hard_filters: dict[str, Any],
    effective_soft_filters: dict[str, Any],
    result_count: int,
) -> str:
    return (
        f"Previous hard filters: {json.dumps(effective_hard_filters, ensure_ascii=False)}. "
        f"Previous soft filters: {json.dumps(effective_soft_filters, ensure_ascii=False)}. "
        f"Returned {result_count} listings."
    )


_SCORE_COMPONENT_CONFIG: dict[str, dict[str, str]] = {
    "text": {
        "label": "Text",
        "description": "How strongly semantic and keyword relevance should affect reranking.",
    },
    "poi": {
        "label": "POI",
        "description": "How strongly closeness to places like schools, shops, stations, or landmarks should affect reranking.",
    },
    "numeric": {
        "label": "Numeric",
        "description": "How strongly soft numeric preferences like price, size, rooms, or year should affect reranking.",
    },
    "feature": {
        "label": "Features",
        "description": "How strongly soft features like pets allowed, elevator, balcony, or furnished should affect reranking.",
    },
}


def _sanitize_score_component_weights(weights: dict[str, float]) -> dict[str, float]:
    sanitized: dict[str, float] = {}
    for key, value in weights.items():
        if key not in _SCORE_COMPONENT_CONFIG:
            continue
        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            continue
        sanitized[key] = max(0.0, min(2.0, numeric_value))
    return sanitized


def build_score_weight_controls(
    *,
    effective_soft_filters: dict[str, Any],
    current_weights: dict[str, float],
) -> list[dict[str, Any]]:
    active_components = {
        "text": True,
        "poi": bool(effective_soft_filters.get("points_of_interest")),
        "feature": any(
            effective_soft_filters.get(key) is not None
            for key in (
                "feature_balcony",
                "feature_elevator",
                "feature_parking",
                "feature_garage",
                "feature_fireplace",
                "feature_child_friendly",
                "feature_pets_allowed",
                "feature_temporary",
                "feature_new_build",
                "feature_wheelchair_accessible",
                "feature_private_laundry",
                "feature_minergie_certified",
                "is_furnished",
                "brightness",
                "has_view",
                "is_quiet",
                "family_friendly",
                "student_friendly",
                "near_nature",
                "near_lake",
                "central_location",
                "has_outdoor_space",
                "pets_allowed",
                "has_parking",
                "has_elevator",
            )
        ),
        "numeric": any(
            isinstance(effective_soft_filters.get(key), (int, float))
            for key in (
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
                "preferred_rooms",
                "preferred_area_sqm",
                "min_floor",
            )
        ),
    }
    controls: list[dict[str, Any]] = []
    for key, active in active_components.items():
        if not active:
            continue
        config = _SCORE_COMPONENT_CONFIG[key]
        controls.append(
            {
                "key": key,
                "label": config["label"],
                "description": config["description"],
                "weight": current_weights.get(key, 1.0),
                "min_weight": 0.0,
                "max_weight": 2.0,
                "default_weight": 1.0,
            }
        )
    return controls
