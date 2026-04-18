from __future__ import annotations

import logging

from fastapi import APIRouter

from app.config import get_settings
from app.harness.search_service import query_from_filters, query_from_text
from app.models.schemas import (
    HealthResponse,
    ListingsQueryRequest,
    ListingsResponse,
    ListingsSearchRequest,
)

router = APIRouter()
LOGGER = logging.getLogger(__name__)


@router.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(status="ok")


@router.post("/listings", response_model=ListingsResponse)
def listings(request: ListingsQueryRequest) -> ListingsResponse:
    settings = get_settings()
    LOGGER.info(
        "Handling /listings query_len=%s conversation_turns=%s limit=%s offset=%s",
        len(request.query),
        len(request.conversation),
        request.limit,
        request.offset,
    )
    response = query_from_text(
        db_path=settings.db_path,
        query=request.query,
        conversation=request.conversation,
        limit=request.limit,
        offset=request.offset,
    )
    LOGGER.info(
        "/listings completed listings_count=%s extracted_hard_filters=%s",
        len(response.listings),
        response.meta.get("extracted_hard_filters"),
    )
    return response


@router.post("/listings/search/filter", response_model=ListingsResponse)
def listings_search(request: ListingsSearchRequest) -> ListingsResponse:
    settings = get_settings()
    LOGGER.info(
        "Handling /listings/search/filter hard_filters_present=%s",
        request.hard_filters is not None,
    )
    response = query_from_filters(
        db_path=settings.db_path,
        hard_facts=request.hard_filters,
    )
    LOGGER.info(
        "/listings/search/filter completed listings_count=%s",
        len(response.listings),
    )
    return response
