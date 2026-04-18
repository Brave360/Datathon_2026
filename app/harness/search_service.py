from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from app.core.hard_filters import HardFilterParams, search_listings
from app.models.schemas import ConversationTurn, HardFilters, ListingsResponse
from app.participant.hard_fact_extraction import extract_hard_facts
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
    hard_facts = extract_hard_facts(query, conversation=conversation)
    hard_facts.limit = limit
    hard_facts.offset = offset
    LOGGER.info("query_from_text extracted_hard_facts=%s", hard_facts.model_dump())
    soft_facts = extract_soft_facts(query)
    candidates = filter_hard_facts(db_path, hard_facts)
    LOGGER.info("query_from_text hard_filter_candidates=%s", len(candidates))
    candidates = filter_soft_facts(candidates, soft_facts)
    LOGGER.info("query_from_text post_soft_filter_candidates=%s", len(candidates))
    ranked = rank_listings(candidates, soft_facts)
    LOGGER.info("query_from_text ranked_results=%s", len(ranked))
    return ListingsResponse(
        listings=ranked,
        meta={
            "extracted_hard_filters": hard_facts.model_dump(),
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
