from __future__ import annotations

import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)

_TOOL_NAME = "extract_soft_requirements"

_TOOL_SCHEMA = {
    "name": _TOOL_NAME,
    "description": (
        "Extract soft (non-deal-breaker) preferences from a real estate search query. "
        "All output fields must be in English regardless of the query language."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "proximity": {
                "type": "array",
                "description": "List of proximity requirements to specific places or POI types.",
                "items": {
                    "type": "object",
                    "properties": {
                        "poi": {
                            "type": "string",
                            "description": "Specific place name (e.g. 'ETHZ', 'Zurich HB') or generic type (e.g. 'hospital', 'school', 'lake', 'park').",
                        },
                        "poi_type": {
                            "type": "string",
                            "enum": [
                                "named_place",
                                "school",
                                "university",
                                "hospital",
                                "public_transport",
                                "shop",
                                "park",
                                "lake",
                                "nature",
                                "kindergarten",
                                "other",
                            ],
                        },
                        "max_minutes": {
                            "type": "integer",
                            "description": "Maximum acceptable travel time in minutes. Null if not specified.",
                        },
                        "transport_mode": {
                            "type": "string",
                            "enum": ["walking", "public_transport", "cycling", "driving", "any"],
                        },
                    },
                    "required": ["poi", "poi_type", "transport_mode"],
                },
            },
            "brightness": {
                "type": "boolean",
                "description": "True if tenant prefers a bright/sunny apartment.",
            },
            "min_floor": {
                "type": "integer",
                "description": "Minimum floor number preferred (e.g. 2 = not ground floor).",
            },
            "has_view": {
                "type": "boolean",
                "description": "True if tenant prefers a flat with a view.",
            },
            "is_quiet": {
                "type": "boolean",
                "description": "True if tenant prefers a quiet environment.",
            },
            "is_furnished": {
                "type": "boolean",
                "description": "True if tenant needs the flat to be furnished.",
            },
            "min_year_built": {
                "type": "integer",
                "description": "Minimum construction year (e.g. 2000 for 'modern').",
            },
            "max_year_built": {
                "type": "integer",
                "description": "Maximum construction year (e.g. 1950 for 'historic/old building').",
            },
            "family_friendly": {
                "type": "boolean",
                "description": "True if listing should be suitable for families with children.",
            },
            "student_friendly": {
                "type": "boolean",
                "description": "True if listing should be student-friendly (affordable, central, well-connected).",
            },
            "near_nature": {
                "type": "boolean",
                "description": "True if tenant wants proximity to nature, forests, or countryside.",
            },
            "near_lake": {
                "type": "boolean",
                "description": "True if tenant wants proximity to a lake.",
            },
            "central_location": {
                "type": "boolean",
                "description": "True if tenant prefers a central, urban location.",
            },
            "has_outdoor_space": {
                "type": "boolean",
                "description": "True if tenant wants a balcony, terrace, or garden.",
            },
            "pets_allowed": {
                "type": "boolean",
                "description": "True if tenant has or plans to have pets.",
            },
            "has_parking": {
                "type": "boolean",
                "description": "True if tenant needs parking or a garage.",
            },
            "has_elevator": {
                "type": "boolean",
                "description": "True if tenant needs an elevator (e.g. for accessibility).",
            },
            "preferred_rooms": {
                "type": "number",
                "description": "Preferred number of rooms as a soft preference (distinct from the hard filter min/max).",
            },
            "preferred_area_sqm": {
                "type": "number",
                "description": "Preferred living area in square metres as a soft preference.",
            },
            "neighborhood_vibes": {
                "type": "array",
                "description": "Qualitative neighbourhood descriptors in English (e.g. 'lively', 'residential', 'green', 'trendy', 'safe').",
                "items": {"type": "string"},
            },
            "extracted_preferences": {
                "type": "array",
                "description": "Short English phrases summarising each detected preference (used for display/logging).",
                "items": {"type": "string"},
            },
            "original_language": {
                "type": "string",
                "description": "ISO 639-1 language code of the query (e.g. 'en', 'fr', 'de', 'it').",
            },
        },
        "required": ["extracted_preferences", "original_language"],
    },
}

_SYSTEM_PROMPT = """You are an expert real estate preference analyst specialising in the Swiss rental market (cities: Zurich, Geneva, Basel, Bern, Lausanne, etc.).

Your task is to analyse a tenant's natural-language search query and extract their **soft preferences** — things that matter to them but are not absolute deal-breakers — and map them to structured fields.

Guidelines:
- Output every field in **English**, regardless of the query language.
- Be conservative: only populate a field when the query clearly implies it.
- For proximity requirements, infer a reasonable `max_minutes` when not stated (e.g. "close to school" → 10 min walking; "near the lake" → 15 min walking). Leave null if truly unknown.
- Distinguish between hard facts already expressed as filters (price, rooms, city) and soft preferences. Only capture the soft layer.
- Common Swiss soft signals:
  • "ruhig" / "calme" / "tranquillo" → is_quiet
  • "hell" / "lumineux" / "luminoso" → brightness
  • "modern" / "neuf" → min_year_built ≈ 2000
  • "historisch" / "ancien" / "vecchio" → max_year_built ≈ 1950
  • "familienfreundlich" / "pour famille" → family_friendly
  • "Seenähe" / "bord du lac" / "vicino al lago" → near_lake
  • "Balkon" / "balcon" / "balcone" or "Terrasse" → has_outdoor_space
  • "Parking" / "Garage" → has_parking
  • "Haustiere erlaubt" / "animaux acceptés" → pets_allowed
- Always populate `extracted_preferences` with a short English summary list of what you found.
- Always populate `original_language` with the detected ISO 639-1 code."""


def extract_soft_facts(query: str) -> dict[str, Any]:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        logger.warning("ANTHROPIC_API_KEY not set — returning raw query only.")
        return {"raw_query": query}

    try:
        import anthropic
    except ImportError:
        logger.warning("anthropic package not installed — returning raw query only.")
        return {"raw_query": query}

    model = os.getenv("LISTINGS_SOFT_EXTRACT_MODEL", "claude-opus-4-7")
    client = anthropic.Anthropic(api_key=api_key)

    response = client.messages.create(
        model=model,
        max_tokens=1024,
        system=[
            {
                "type": "text",
                "text": _SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        tools=[{**_TOOL_SCHEMA, "cache_control": {"type": "ephemeral"}}],
        tool_choice={"type": "tool", "name": _TOOL_NAME},
        messages=[
            {
                "role": "user",
                "content": f"Extract soft preferences from this query:\n\n{query}",
            }
        ],
    )

    for block in response.content:
        if block.type == "tool_use" and block.name == _TOOL_NAME:
            result: dict[str, Any] = dict(block.input)
            result["raw_query"] = query
            return result

    logger.warning("Claude did not return a tool call — falling back to raw query.")
    return {"raw_query": query}
