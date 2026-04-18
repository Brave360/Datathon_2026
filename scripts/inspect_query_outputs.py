from __future__ import annotations

import argparse
import json
import sys
import textwrap
import urllib.error
import urllib.request
from typing import Any


QUERIES = [
    "I am looking for a 1.5-room apartment in Zurich near ETH, ideally under CHF 2200.",
    "I want to live close to my workplace, max 20 minutes by public transport.",
    "Looking for something in Zurich that feels quiet and bright.",
    "We are looking for an apartment for 3 people with at least 2 bedrooms.",
    "I would like something near a lake, preferably quiet.",
    "Looking for an apartment in Winterthur, from 70 m², ideally with a balcony.",
    "I want something that feels modern, but not sterile or uncomfortable.",
    "I am looking for an apartment in Oerlikon with an elevator and a washing machine in the unit.",
    "What matters most to me is that the area feels safe and pleasant to live in.",
    "I am looking for something small in Zurich, ideally furnished.",
    "I am looking for a 2.5 to 3-room apartment in Zurich or directly adjacent, ideally with a max 25-minute commute to Zurich HB, at least 65 m², a balcony or loggia, and if possible not on the ground floor.",
    "We are a family with one child and are looking in Zug or Baar for an apartment with 3.5 or 4 rooms, at least 85 m², budget up to CHF 3600, ideally in an area with good schools, little traffic, and some greenery nearby.",
    "I am looking for something in Lausanne near EPFL or with a good metro connection there, ideally 1 to 2 rooms, furnished, and overall more practical than fancy.",
    "We would like to live in Basel, ideally with 2 bedrooms, good tram connections, a washer-dryer tower, and it would be great if shopping were easily reachable on foot.",
    "I am looking for an apartment that simply feels pleasant: lots of light, a quiet street, not too anonymous, more like a neighborhood where you would enjoy staying long term.",
    "Looking for something in Zurich Districts 3, 4, or 5, under CHF 3200, with at least 2.5 rooms and ideally a modern kitchen.",
    "Above all, I want to avoid a long commute; everything else is relatively open.",
    "We are looking for something family-friendly, where living with children works well.",
    "I would like an apartment with a good layout, large windows, and if possible a balcony.",
    "Looking for an apartment in Winterthur, not too far from the train station, but please not directly on a major road.",
    "I am looking for a 4-room apartment in Zurich North, ideally from 95 m², with an elevator; 2 bathrooms would be a plus, rent up to CHF 4200, and the trip to Kantonsschule Rämibühl should be doable by public transport in around 30 minutes.",
    "We are looking for something for 2 adults and 1 child in the Thalwil, Horgen, or Wädenswil area, ideally near the lake, with at least 3.5 rooms, from 90 m², budget up to CHF 3800, a balcony or small garden, and in an environment that feels quiet, safe, and not too densely built.",
    "I am looking for something more affordable in Basel. It does not need to be large, but the location should be practical.",
    "The most important thing to me is that the apartment is quiet and not dark.",
    "I am looking for an apartment in Bern, ideally with max 15 minutes to the train station, 2 to 3 rooms, and no more than CHF 2500.",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the predefined apartment-search queries against the local API."
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8000",
        help="Base URL of the FastAPI server. Default: http://127.0.0.1:8000",
    )
    parser.add_argument(
        "--start-at",
        type=int,
        default=1,
        help="1-based query number to start from. Default: 1",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Limit sent to the /listings endpoint. Default: 5",
    )
    parser.add_argument(
        "--show-listings",
        type=int,
        default=3,
        help="How many top listings to print per query. Default: 3",
    )
    parser.add_argument(
        "--no-pause",
        action="store_true",
        help="Run through all queries without waiting for Enter between them.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.start_at < 1 or args.start_at > len(QUERIES):
        raise ValueError(f"--start-at must be between 1 and {len(QUERIES)}")

    for index in range(args.start_at - 1, len(QUERIES)):
        query_number = index + 1
        query = QUERIES[index]
        print("=" * 100)
        print(f"Question {query_number}/{len(QUERIES)}")
        print(textwrap.fill(query, width=100))
        print("-" * 100)

        try:
            payload = call_listings_api(
                base_url=args.base_url,
                query=query,
                limit=args.limit,
            )
        except Exception as exc:
            print(f"Request failed: {exc}")
        else:
            print_response_summary(payload, show_listings=args.show_listings)

        if index == len(QUERIES) - 1 or args.no_pause:
            continue

        user_input = input("Press Enter for next query, or type q then Enter to quit: ").strip().lower()
        if user_input == "q":
            break

    return 0


def call_listings_api(*, base_url: str, query: str, limit: int) -> dict[str, Any]:
    url = f"{base_url.rstrip('/')}/listings"
    request_body = json.dumps(
        {
            "query": query,
            "limit": limit,
            "offset": 0,
        }
    ).encode("utf-8")

    request = urllib.request.Request(
        url,
        data=request_body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {body}") from exc


def print_response_summary(payload: dict[str, Any], *, show_listings: int) -> None:
    meta = payload.get("meta", {})
    listings = payload.get("listings", [])

    print("Extracted hard filters:")
    print(json.dumps(meta.get("extracted_hard_filters", {}), indent=2, ensure_ascii=False))
    print()
    print(f"Returned listings: {len(listings)}")

    for idx, item in enumerate(listings[:show_listings], start=1):
        listing = item.get("listing", {})
        print()
        print(f"Top result #{idx}")
        print(f"  listing_id: {item.get('listing_id')}")
        print(f"  score: {item.get('score')}")
        print(f"  reason: {item.get('reason')}")
        print(f"  title: {listing.get('title')}")
        print(f"  city: {listing.get('city')}")
        print(f"  price_chf: {listing.get('price_chf')}")
        print(f"  rooms: {listing.get('rooms')}")
        print(f"  features: {listing.get('features')}")
        print(f"  url: {listing.get('original_listing_url')}")

    if not listings:
        print("No listings returned.")


if __name__ == "__main__":
    raise SystemExit(main())
