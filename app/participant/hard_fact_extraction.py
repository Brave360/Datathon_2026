from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import anthropic

from app.config import get_settings
from app.models.schemas import ConversationTurn, HardFilters

LOGGER = logging.getLogger(__name__)

SYSTEM_PROMPT = """
You extract only hard listing-search constraints from a user query.
Return JSON only. Do not include markdown, prose, or explanations.

Use exactly these keys:
- city: array of strings or null
- postal_code: array of strings or null
- canton: string or null
- min_price: integer or null
- max_price: integer or null
- min_rooms: number or null
- max_rooms: number or null
- min_area_sqm: number or null
- max_area_sqm: number or null
- latitude: number or null
- longitude: number or null
- radius_km: number or null
- features: array of strings or null
- offer_type: string or null
- object_category: array of strings or null
- sort_by: string or null

Rules:
- Extract only explicit, objective constraints from the query.
- Supported feature values: balcony, elevator, parking, garage, fireplace,
  child_friendly, pets_allowed, temporary, new_build,
  wheelchair_accessible, private_laundry, minergie_certified.
- Supported sort_by values: price_asc, price_desc, rooms_asc, rooms_desc.
- Supported object_category values: Wohnung; Gewerbeobjekt; Wohnnebenraeume; Parkplatz; Parkplatz, Garage; Möblierte Wohnung; Tiefgarage; Haus; Haus; Bastelraum; Dachwohnung; Einzelzimmer; Maisonette; Einzelgarage; Studio; Attika; WG-Zimmer; Loft; Villa; Diverses; Doppeleinfamilienhaus; Reihenhaus; Bauernhaus; Mehrfamilienhaus; Wohnnebenraeume; Terrassenwohnung; Ferienwohnung; Gastgewerbe; Grundstück; Ferienimmobilie; Terrassenhaus
- Supported offer_type values: RENT, SALE
- Do not guess values that are not stated or strongly implied.
- Do not include limit or offset.
- Be generous in assigning values to object_category based on the query. For example, if the user mentions "Apartment", you should output "Wohnung", "Möblierte Wohnung", "Dachwohnung", ... and any relevant others as object categories.
- If a field is not present, set it to null.
""".strip()


def extract_hard_facts(
    query: str,
    *,
    conversation: list[ConversationTurn] | None = None,
) -> HardFilters:
    settings = get_settings()
    conversation = conversation or []
    if not settings.claude_api_key:
        hard_filters = HardFilters()
        _append_debug_record(
            debug_log_path=settings.hard_facts_debug_log_path,
            query=query,
            conversation=conversation,
            hard_filters=hard_filters,
            source="fallback_no_api_key",
        )
        return hard_filters

    try:
        payload = _call_claude_for_hard_filters(
            query=query,
            conversation=conversation,
            api_key=settings.claude_api_key,
            model=settings.claude_model,
            api_base_url=settings.claude_api_base_url,
            timeout_seconds=settings.claude_timeout_seconds,
        )
        hard_filters = HardFilters.model_validate(payload)
        _append_debug_record(
            debug_log_path=settings.hard_facts_debug_log_path,
            query=query,
            conversation=conversation,
            hard_filters=hard_filters,
            source="claude",
            raw_payload=payload,
        )
        return hard_filters
    except Exception as exc:
        LOGGER.warning("Claude hard fact extraction failed: %s", exc)
        hard_filters = HardFilters()
        _append_debug_record(
            debug_log_path=settings.hard_facts_debug_log_path,
            query=query,
            conversation=conversation,
            hard_filters=hard_filters,
            source="fallback_error",
            error=str(exc),
        )
        return hard_filters


def _call_claude_for_hard_filters(
    *,
    query: str,
    conversation: list[ConversationTurn],
    api_key: str,
    model: str,
    api_base_url: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    client = anthropic.Anthropic(
        api_key=api_key,
        base_url=api_base_url.removesuffix("/v1/messages"),
        timeout=timeout_seconds,
    )
    message = client.messages.create(
        model=model,
        max_tokens=400,
        system=SYSTEM_PROMPT,
        messages=_build_messages(conversation=conversation, query=query),
    )
    return _extract_json_payload(message.model_dump())


def _build_messages(
    *,
    conversation: list[ConversationTurn],
    query: str,
) -> list[dict[str, str]]:
    messages: list[dict[str, str]] = []
    for turn in conversation:
        messages.append(
            {
                "role": turn.role,
                "content": turn.content,
            }
        )
    messages.append(
        {
            "role": "user",
            "content": query,
        }
    )
    return messages


def _extract_json_payload(data: dict[str, Any]) -> dict[str, Any]:
    text_chunks: list[str] = []
    for block in data.get("content", []):
        if isinstance(block, dict) and block.get("type") == "text":
            text = block.get("text")
            if isinstance(text, str) and text.strip():
                text_chunks.append(text.strip())

    if not text_chunks:
        raise ValueError("Claude response did not contain a text payload")

    raw_text = "\n".join(text_chunks).strip()
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        parsed = json.loads(_extract_first_json_object(raw_text))

    if not isinstance(parsed, dict):
        raise ValueError("Claude payload was not a JSON object")
    return parsed


def _extract_first_json_object(text: str) -> str:
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("No JSON object found in Claude response")
    return text[start : end + 1]


def _append_debug_record(
    *,
    debug_log_path: Path,
    query: str,
    conversation: list[ConversationTurn],
    hard_filters: HardFilters,
    source: str,
    raw_payload: dict[str, Any] | None = None,
    error: str | None = None,
) -> None:
    record: dict[str, Any] = {
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "query": query,
        "conversation": [turn.model_dump() for turn in conversation],
        "source": source,
        "hard_filters": hard_filters.model_dump(),
    }
    if raw_payload is not None:
        record["raw_payload"] = raw_payload
    if error is not None:
        record["error"] = error

    try:
        debug_log_path.parent.mkdir(parents=True, exist_ok=True)
        with debug_log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=True) + "\n")
    except OSError as exc:
        LOGGER.warning("Failed to write hard facts debug log: %s", exc)
