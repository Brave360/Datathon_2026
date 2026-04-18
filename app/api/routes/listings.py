from __future__ import annotations

import logging

from fastapi import APIRouter
from fastapi import HTTPException

from app.harness.conversation_store import append_turns, get_turns
from app.config import get_settings
from app.harness.search_service import query_from_filters, query_from_text
from app.models.schemas import (
    ConversationHistoryResponse,
    ConversationTurn,
    HealthResponse,
    ListingsQueryRequest,
    ListingsRerankRequest,
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
        soft_preference_weights=request.soft_preference_weights,
        limit=request.limit,
        offset=request.offset,
    )
    if request.conversation_id:
        assistant_summary = response.meta.get("assistant_summary")
        assistant_turn = ConversationTurn(
            role="assistant",
            content=assistant_summary if isinstance(assistant_summary, str) else "",
        )
        append_turns(
            request.conversation_id,
            [
                ConversationTurn(role="user", content=request.query),
                assistant_turn,
            ],
        )
    LOGGER.info(
        "/listings completed listings_count=%s effective_hard_filters=%s",
        len(response.listings),
        response.meta.get("effective_hard_filters"),
    )
    return response


@router.post("/listings/rerank", response_model=ListingsResponse)
def listings_rerank(request: ListingsRerankRequest) -> ListingsResponse:
    settings = get_settings()
    LOGGER.info(
        "Handling /listings/rerank query_len=%s conversation_turns=%s limit=%s offset=%s score_component_weights=%s",
        len(request.query),
        len(request.conversation),
        request.limit,
        request.offset,
        sorted(request.soft_preference_weights.keys()),
    )
    response = query_from_text(
        db_path=settings.db_path,
        query=request.query,
        conversation=request.conversation,
        soft_preference_weights=request.soft_preference_weights,
        limit=request.limit,
        offset=request.offset,
    )
    LOGGER.info(
        "/listings/rerank completed listings_count=%s score_component_weights=%s",
        len(response.listings),
        response.meta.get("score_component_weights"),
    )
    return response


@router.get("/listings/history/{conversation_id}", response_model=ConversationHistoryResponse)
def listings_history(conversation_id: str) -> ConversationHistoryResponse:
    messages = get_turns(conversation_id)
    if not messages:
        raise HTTPException(status_code=404, detail="Conversation history not found")
    return ConversationHistoryResponse(conversation_id=conversation_id, messages=messages)


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
