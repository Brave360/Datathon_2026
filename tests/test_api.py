import os
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from app.models.schemas import ListingsResponse


def test_health_endpoint(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    os.environ["LISTINGS_RAW_DATA_DIR"] = str(repo_root / "raw_data")
    os.environ["LISTINGS_DB_PATH"] = str(tmp_path / "listings.db")

    from app.main import app

    with TestClient(app) as client:
        response = client.get("/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_post_listings_returns_ranked_results(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    os.environ["LISTINGS_RAW_DATA_DIR"] = str(repo_root / "raw_data")
    os.environ["LISTINGS_DB_PATH"] = str(tmp_path / "listings.db")

    from app.main import app

    with (
        patch(
            "app.api.routes.listings.query_from_text",
            return_value=ListingsResponse(
                listings=[],
                meta={
                    "effective_hard_filters": {"city": ["Winterthur"]},
                    "effective_soft_filters": {"feature_balcony": True},
                    "assistant_summary": "Summary",
                    "conversation_turn_count": 1,
                },
            ),
        ),
        TestClient(app) as client,
    ):
        response = client.post("/listings", json={"query": "3 room flat in winterthur"})

    assert response.status_code == 200
    body = response.json()
    assert isinstance(body, dict)
    assert "listings" in body
    assert "meta" in body
    assert isinstance(body["listings"], list)
    assert len(body["listings"]) <= 25
    assert body["meta"]["effective_hard_filters"]["city"] == ["Winterthur"]
    assert body["meta"]["effective_soft_filters"]["feature_balcony"] is True
    assert body["meta"]["conversation_turn_count"] == 1


def test_post_listings_accepts_conversation_turns(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    os.environ["LISTINGS_RAW_DATA_DIR"] = str(repo_root / "raw_data")
    os.environ["LISTINGS_DB_PATH"] = str(tmp_path / "listings.db")

    from app.main import app

    with (
        patch(
            "app.api.routes.listings.query_from_text",
            return_value=ListingsResponse(
                listings=[],
                meta={
                    "effective_hard_filters": {"city": ["Zurich"]},
                    "effective_soft_filters": {},
                    "assistant_summary": "Summary",
                    "conversation_turn_count": 3,
                },
            ),
        ) as mocked_query,
        TestClient(app) as client,
    ):
        response = client.post(
            "/listings",
            json={
                "query": "make it cheaper",
                "conversation": [
                    {"role": "user", "content": "I want a flat in Zurich with balcony"},
                    {"role": "assistant", "content": "Previous hard filters: city Zurich, balcony"},
                ],
            },
        )

    assert response.status_code == 200
    _, kwargs = mocked_query.call_args
    assert len(kwargs["conversation"]) == 2
    assert kwargs["conversation"][0].content == "I want a flat in Zurich with balcony"


def test_post_listings_stores_and_returns_conversation_history(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    os.environ["LISTINGS_RAW_DATA_DIR"] = str(repo_root / "raw_data")
    os.environ["LISTINGS_DB_PATH"] = str(tmp_path / "listings.db")

    from app.main import app

    with (
        patch(
            "app.api.routes.listings.query_from_text",
            return_value=ListingsResponse(
                listings=[],
                meta={
                    "effective_hard_filters": {"city": ["Zurich"]},
                    "effective_soft_filters": {"feature_balcony": True},
                    "assistant_summary": 'Previous hard filters: {"city": ["Zurich"]}.',
                    "conversation_turn_count": 1,
                },
            ),
        ),
        TestClient(app) as client,
    ):
        post_response = client.post(
            "/listings",
            json={
                "query": "flat in Zurich",
                "conversation_id": "test-conversation",
            },
        )
        history_response = client.get("/listings/history/test-conversation")

    assert post_response.status_code == 200
    assert history_response.status_code == 200
    body = history_response.json()
    assert body["conversation_id"] == "test-conversation"
    assert body["messages"] == [
        {"role": "user", "content": "flat in Zurich"},
        {"role": "assistant", "content": 'Previous hard filters: {"city": ["Zurich"]}.'},
    ]


def test_post_listings_search_filter_applies_explicit_hard_filters(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    os.environ["LISTINGS_RAW_DATA_DIR"] = str(repo_root / "raw_data")
    os.environ["LISTINGS_DB_PATH"] = str(tmp_path / "listings.db")

    from app.main import app

    with TestClient(app) as client:
        response = client.post(
            "/listings/search/filter",
            json={
                "hard_filters": {"city": ["Winterthur"], "limit": 5},
            },
        )

    assert response.status_code == 200
    body = response.json()
    assert isinstance(body, dict)
    assert "listings" in body
    assert "meta" in body
    assert isinstance(body["listings"], list)
    assert len(body["listings"]) <= 5
    assert body["listings"]
    assert {"listing_id", "score", "reason", "listing"} <= set(body["listings"][0].keys())
    assert {"id", "title", "city"} <= set(body["listings"][0]["listing"].keys())
    assert all(
        (item["listing"].get("city") or "").lower() == "winterthur"
        for item in body["listings"]
    )


def test_raw_data_images_are_served_from_local_static_mount(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[1]
    os.environ["LISTINGS_RAW_DATA_DIR"] = str(repo_root / "raw_data")
    os.environ["LISTINGS_DB_PATH"] = str(tmp_path / "listings.db")

    from app.main import app

    with TestClient(app) as client:
        response = client.get("/raw-data-images/4154142.jpeg")

    assert response.status_code == 200
    assert response.headers["content-type"] == "image/jpeg"
