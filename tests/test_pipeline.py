import json
from unittest.mock import patch
from pathlib import Path

from app.models.schemas import ConversationTurn, HardFilters
from app.participant.hard_fact_extraction import extract_hard_facts
from app.participant.ranking import rank_listings
from app.participant.soft_fact_extraction import extract_soft_facts
from app.participant.soft_filtering import filter_soft_facts
from app.harness.search_service import to_hard_filter_params


def test_extract_hard_facts_returns_stub_structure() -> None:
    result = extract_hard_facts("3 room flat in zurich")

    assert isinstance(result, HardFilters)


def test_extract_hard_facts_uses_claude_payload_when_api_key_is_set() -> None:
    with (
        patch.dict("os.environ", {"CLAUDE_API_KEY": "test-key"}, clear=False),
        patch(
            "app.participant.hard_fact_extraction._call_claude_for_hard_filters",
            return_value={
                "city": ["Zurich"],
                "max_price": 2800,
                "min_rooms": 3.0,
                "min_area_sqm": 65.0,
                "features": ["balcony"],
                "postal_code": None,
                "canton": None,
                "min_price": None,
                "max_rooms": None,
                "max_area_sqm": None,
                "latitude": None,
                "longitude": None,
                "radius_km": None,
                "offer_type": None,
                "object_category": None,
                "sort_by": None,
            },
        ),
    ):
        result = extract_hard_facts("3 room flat in Zurich with balcony under 2800 CHF")

    assert result.city == ["Zurich"]
    assert result.max_price == 2800
    assert result.min_rooms == 3.0
    assert result.min_area_sqm == 65.0
    assert result.features == ["balcony"]


def test_extract_hard_facts_passes_conversation_to_claude() -> None:
    with (
        patch.dict("os.environ", {"CLAUDE_API_KEY": "test-key"}, clear=False),
        patch(
            "app.participant.hard_fact_extraction._call_claude_for_hard_filters",
            return_value={
                "city": ["Zurich"],
                "postal_code": None,
                "canton": None,
                "min_price": None,
                "max_price": 2400,
                "min_rooms": None,
                "max_rooms": None,
                "min_area_sqm": None,
                "max_area_sqm": None,
                "latitude": None,
                "longitude": None,
                "radius_km": None,
                "features": None,
                "offer_type": None,
                "object_category": None,
                "sort_by": None,
            },
        ) as mocked_call,
    ):
        extract_hard_facts(
            "make it cheaper",
            conversation=[
                ConversationTurn(role="user", content="3 room flat in Zurich with balcony")
            ],
        )

    _, kwargs = mocked_call.call_args
    assert len(kwargs["conversation"]) == 1
    assert kwargs["conversation"][0].role == "user"
    assert kwargs["conversation"][0].content == "3 room flat in Zurich with balcony"


def test_extract_hard_facts_writes_debug_log(tmp_path: Path) -> None:
    debug_log_path = tmp_path / "hard_facts_debug.jsonl"
    with (
        patch.dict(
            "os.environ",
            {
                "CLAUDE_API_KEY": "test-key",
                "HARD_FACTS_DEBUG_LOG_PATH": str(debug_log_path),
            },
            clear=False,
        ),
        patch(
            "app.participant.hard_fact_extraction._call_claude_for_hard_filters",
            return_value={
                "city": ["Winterthur"],
                "postal_code": None,
                "canton": None,
                "min_price": None,
                "max_price": 2000,
                "min_rooms": None,
                "max_rooms": None,
                "min_area_sqm": 70.0,
                "max_area_sqm": None,
                "latitude": None,
                "longitude": None,
                "radius_km": None,
                "features": ["balcony"],
                "offer_type": None,
                "object_category": None,
                "sort_by": None,
            },
        ),
    ):
        extract_hard_facts("apartment in Winterthur under 2000 CHF with balcony")

    lines = debug_log_path.read_text(encoding="utf-8").strip().splitlines()
    assert lines
    record = json.loads(lines[-1])
    assert record["source"] == "claude"
    assert record["query"] == "apartment in Winterthur under 2000 CHF with balcony"
    assert record["conversation"] == []
    assert record["hard_filters"]["city"] == ["Winterthur"]
    assert record["hard_filters"]["max_price"] == 2000
    assert record["hard_filters"]["min_area_sqm"] == 70.0
    assert record["hard_filters"]["features"] == ["balcony"]


def test_participant_soft_fact_modules_are_importable() -> None:
    candidates = [{"listing_id": "1", "title": "Example"}]

    soft_facts = extract_soft_facts("bright flat")
    filtered = filter_soft_facts(candidates, soft_facts)
    ranked = rank_listings(filtered, soft_facts)

    assert isinstance(soft_facts, dict)
    assert isinstance(filtered, list)
    assert all(item["listing_id"] in {"1"} for item in filtered)
    assert isinstance(ranked, list)
    assert ranked
    assert all(item.listing_id for item in ranked)
    assert all(isinstance(item.score, float) for item in ranked)


def test_harness_service_converts_hard_filters_to_search_params() -> None:
    filters = HardFilters(
        city=["Zurich"],
        features=["balcony"],
        min_area_sqm=65,
        max_area_sqm=90,
        limit=5,
        offset=2,
    )

    params = to_hard_filter_params(filters)

    assert params.city == ["Zurich"]
    assert params.features == ["balcony"]
    assert params.min_area_sqm == 65
    assert params.max_area_sqm == 90
    assert params.limit == 5
    assert params.offset == 2
