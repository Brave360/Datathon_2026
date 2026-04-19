from __future__ import annotations

from typing import Any
from pydantic import BaseModel, Field
import anthropic

from app.config import get_settings
from app.models.schemas import ConversationTurn


class PointOfInterest(BaseModel):
    type: str = Field(description="Category of the place, e.g. 'school', 'hospital', 'park', 'supermarket'")
    query: str = Field(description="Geocoding-ready search string, e.g. 'primary school Zurich'")
    radius_km: float = Field(description="Maximum acceptable distance in km to the nearest such place")


class Requirements(BaseModel):
    city: str | None = Field(None, description="City name, e.g. 'Zurich'")
    canton: str | None = Field(None, description="Swiss canton abbreviation, e.g. 'ZH'")
    postal_code: str | None = Field(None, description="Swiss postal code, e.g. '8001'")
    min_price: int | None = Field(None, description="Minimum price/rent in CHF")
    max_price: int | None = Field(None, description="Maximum price/rent in CHF")
    min_rooms: float | None = Field(None, description="Minimum number of rooms")
    max_rooms: float | None = Field(None, description="Maximum number of rooms")
    min_area: float | None = Field(None, description="Minimum area in m²")
    max_area: float | None = Field(None, description="Maximum area in m²")
    offer_type: str | None = Field(None, description="'RENT' or 'BUY'")
    object_category: str | None = Field(None, description="Property category, e.g. 'APARTMENT', 'HOUSE'")
    min_bedrooms: int | None = Field(None)
    max_bedrooms: int | None = Field(None)
    min_bathrooms: int | None = Field(None)
    max_bathrooms: int | None = Field(None)
    min_year_built: int | None = Field(None)
    max_year_built: int | None = Field(None)
    feature_balcony: bool | None = Field(None)
    feature_elevator: bool | None = Field(None)
    feature_parking: bool | None = Field(None)
    feature_garage: bool | None = Field(None)
    feature_fireplace: bool | None = Field(None)
    feature_child_friendly: bool | None = Field(None)
    feature_pets_allowed: bool | None = Field(None)
    feature_temporary: bool | None = Field(None)
    feature_new_build: bool | None = Field(None)
    feature_wheelchair_accessible: bool | None = Field(None)
    feature_private_laundry: bool | None = Field(None)
    feature_minergie_certified: bool | None = Field(None)
    is_furnished: bool | None = Field(None, description="True if furnished, False if explicitly unfurnished")
    points_of_interest: list[PointOfInterest] = Field(
        default_factory=list,
        description="Nearby places the user wants to be close to",
    )


class ParsedQuery(BaseModel):
    hard_requirements: Requirements = Field(
        description="Non-negotiable requirements; listings not matching these are excluded"
    )
    soft_requirements: Requirements = Field(
        description="Nice-to-have preferences; listings matching more score higher"
    )


_SYSTEM_PROMPT = """\
You are a Swiss real estate assistant. Parse the user's natural-language property query into \
structured hard and soft requirements.

Database schema (SQLite table `listings`):
- listing_id, platform_id, scrape_source
- title, description
- street, city, postal_code, canton (2-letter Swiss abbreviation, e.g. ZH, BE, GE)
- price (CHF integer — monthly rent or sale price)
- rooms (float), area (float, m²)
- available_from (ISO date)
- latitude, longitude
- feature_balcony, feature_elevator, feature_parking, feature_garage, feature_fireplace,
  feature_child_friendly, feature_pets_allowed, feature_temporary, feature_new_build,
  feature_wheelchair_accessible, feature_private_laundry, feature_minergie_certified
  (INTEGER 0/1 or NULL)
- is_furnished (INTEGER 0/1): derived from title/description/category; use this when
  the user mentions wanting a furnished or unfurnished property
- bedrooms, bathrooms (integers)
- year_built, last_renovation (integers)
- offer_type (TEXT: "RENT" or "BUY")
- object_category (TEXT: "APARTMENT", "HOUSE", "STUDIO", "ROOM", "LOFT",
  "GARAGE", "PARKING", "COMMERCIAL", "VACATION", "STORAGE", "LAND", "OTHER")
  Note: furnished is captured by is_furnished, not by object_category.

Rules:
1. Hard requirements: the user explicitly states a constraint as mandatory (must, only, maximum,
   minimum, no more than, at least, etc.). Listings failing a hard requirement are rejected.
2. Soft requirements: the user expresses a preference, wish, or nice-to-have. Listings matching
   more soft requirements rank higher.
3. If a requirement is ambiguous, place it in soft_requirements.
4. When prior conversation turns are present, treat the latest user message as a modification to
   the existing search unless it clearly starts over. Preserve earlier constraints and preferences
   by default, and change only the parts the user explicitly changes.
   Example: previous search says "Zurich with balcony under 3200 CHF" and the latest user says
   "make it cheaper" -> keep Zurich and balcony, lower the budget.
   Example: previous search says "3-room apartment in Zurich" and the latest user says
   "actually in Winterthur" -> replace Zurich with Winterthur.
5. points_of_interest MUST always go in soft_requirements, never in hard_requirements.
   Proximity cannot be filtered in the database — it is scored at ranking time.
   If the user mentions wanting to be near any place — a category, a specific chain, or a
   named landmark — add an entry to soft_requirements.points_of_interest.
   - Category (generic): set `type` to the category and `query` to "<category> <city>".
     Examples: school → `{"type":"school","query":"primary school Bern","radius_km":0.5}`
               hospital → `{"type":"hospital","query":"hospital Zurich","radius_km":2.0}`
   - Specific chain (e.g. Denner, Aldi, Migros, Lidl, Coop, McDonald's): set `type` to the
     category (e.g. "supermarket", "fast_food") and `query` to "<BrandName> <city>".
     Example: "Denner in Zurich" → `{"type":"supermarket","query":"Denner Zurich","radius_km":0.5}`
   - Named landmark (e.g. ETH Zurich Zentrum, HB Zürich, EPFL Lausanne, Zürichsee): use the
     exact landmark name as `query` (add city only if needed for disambiguation) and set `type`
     to a descriptive category ("university", "train_station", "lake", etc.).
     Example: "ETH Zurich Zentrum" → `{"type":"university","query":"ETH Zurich Zentrum","radius_km":1.0}`
   - If no city context is available, keep `query` as specific as possible without inventing a city.
   - Set `radius_km` to whatever the user specifies, or 1.0 by default.
6. Set unmentioned fields to null / empty list — do not invent values.
"""


_REQUIREMENTS_SCHEMA = {
    "type": "object",
    "properties": {
        "city": {"type": ["string", "null"]},
        "canton": {"type": ["string", "null"]},
        "postal_code": {"type": ["string", "null"]},
        "min_price": {"type": ["integer", "null"]},
        "max_price": {"type": ["integer", "null"]},
        "min_rooms": {"type": ["number", "null"]},
        "max_rooms": {"type": ["number", "null"]},
        "min_area": {"type": ["number", "null"]},
        "max_area": {"type": ["number", "null"]},
        "offer_type": {"type": ["string", "null"], "enum": ["RENT", "BUY", None]},
        "object_category": {
            "type": ["string", "null"],
            "enum": ["APARTMENT", "HOUSE", "STUDIO", "ROOM", "LOFT",
                     "GARAGE", "PARKING", "COMMERCIAL", "VACATION", "STORAGE", "LAND", "OTHER", None],
        },
        "min_bedrooms": {"type": ["integer", "null"]},
        "max_bedrooms": {"type": ["integer", "null"]},
        "min_bathrooms": {"type": ["integer", "null"]},
        "max_bathrooms": {"type": ["integer", "null"]},
        "min_year_built": {"type": ["integer", "null"]},
        "max_year_built": {"type": ["integer", "null"]},
        "feature_balcony": {"type": ["boolean", "null"]},
        "feature_elevator": {"type": ["boolean", "null"]},
        "feature_parking": {"type": ["boolean", "null"]},
        "feature_garage": {"type": ["boolean", "null"]},
        "feature_fireplace": {"type": ["boolean", "null"]},
        "feature_child_friendly": {"type": ["boolean", "null"]},
        "feature_pets_allowed": {"type": ["boolean", "null"]},
        "feature_temporary": {"type": ["boolean", "null"]},
        "feature_new_build": {"type": ["boolean", "null"]},
        "feature_wheelchair_accessible": {"type": ["boolean", "null"]},
        "feature_private_laundry": {"type": ["boolean", "null"]},
        "feature_minergie_certified": {"type": ["boolean", "null"]},
        "is_furnished": {"type": ["boolean", "null"]},
        "points_of_interest": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "type": {"type": "string"},
                    "query": {"type": "string"},
                    "radius_km": {"type": "number"},
                },
                "required": ["type", "query", "radius_km"],
            },
        },
    },
}

_TOOL = {
    "name": "parse_requirements",
    "description": "Output the parsed hard and soft requirements from the user's property query.",
    "input_schema": {
        "type": "object",
        "properties": {
            "hard_requirements": _REQUIREMENTS_SCHEMA,
            "soft_requirements": _REQUIREMENTS_SCHEMA,
        },
        "required": ["hard_requirements", "soft_requirements"],
    },
}


def _build_messages(
    *,
    query: str,
    conversation: list[ConversationTurn] | None = None,
) -> list[dict[str, str]]:
    messages: list[dict[str, str]] = []
    for turn in conversation or []:
        messages.append({"role": turn.role, "content": turn.content})
    messages.append({"role": "user", "content": query})
    return messages


def parse_query(
    query: str,
    conversation: list[ConversationTurn] | None = None,
) -> ParsedQuery:
    settings = get_settings()
    client = anthropic.Anthropic(
        api_key=settings.claude_api_key,
        base_url=settings.claude_api_base_url.removesuffix("/v1/messages"),
    )
    response = client.messages.create(
        model="claude-opus-4-7",
        max_tokens=2048,
        system=_SYSTEM_PROMPT,
        tools=[_TOOL],
        tool_choice={"type": "tool", "name": "parse_requirements"},
        messages=_build_messages(query=query, conversation=conversation),
    )
    tool_block = next(b for b in response.content if b.type == "tool_use")
    data = tool_block.input
    return ParsedQuery(
        hard_requirements=Requirements(**data.get("hard_requirements", {})),
        soft_requirements=Requirements(**data.get("soft_requirements", {})),
    )


def parse_query_to_dict(
    query: str,
    conversation: list[ConversationTurn] | None = None,
) -> dict[str, Any]:
    result = parse_query(query, conversation=conversation)
    return result.model_dump(exclude_none=False)


if __name__ == "__main__":
    import json

    sample = (
        "I'm looking for a 3-room apartment to rent in Zurich, "
        "maximum 2500 CHF per month. I'd love a balcony and pets are allowed. "
        "Must be close to a primary school (within 1 km) and ideally near a park."
    )
    parsed = parse_query_to_dict(sample)
    print(json.dumps(parsed, indent=2, ensure_ascii=False))
