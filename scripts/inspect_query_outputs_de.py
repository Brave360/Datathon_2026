from __future__ import annotations

import argparse
import json
import textwrap
import urllib.error
import urllib.request
from typing import Any


QUERIES = [
    "Ich suche eine 1.5 Zimmer Wohnung in Zürich in der Nähe der ETH, am besten unter 2200 CHF.",
    "Ich möchte nah an meiner Arbeit wohnen, max 20 Minuten mit dem ÖV.",
    "Suche was in Zürich, das sich ruhig und hell anfühlt.",
    "Wir suchen eine Wohnung für 3 Personen mit mindestens 2 Schlafzimmern.",
    "Ich hätte gern etwas in Seenähe, gern eher ruhig.",
    "Suche Wohnung in Winterthur, ab 70 m², gern mit Balkon.",
    "Ich möchte etwas, das modern, aber nicht ungemütlich ist.",
    "Ich suche eine Wohnung in Oerlikon mit Lift und Waschmaschine in der Wohnung.",
    "Mir ist vor allem wichtig, dass die Gegend sicher ist und sich gut zum Wohnen anfühlt.",
    "Ich suche was Kleines in Zürich, gern möbliert.",
    "Ich suche eine 2.5 bis 3 Zimmer Wohnung in Zürich oder direkt angrenzend, idealerweise mit max 25 Minuten Pendelzeit zum HB, mindestens 65 m², Balkon oder Loggia, und wenn möglich nicht im Erdgeschoss.",
    "Wir sind eine Familie mit einem Kind und suchen in Zug oder Baar eine Wohnung mit 3.5 oder 4 Zimmern, mindestens 85 m², Budget bis 3600 CHF, gern in einer Gegend mit guten Schulen, wenig Verkehr und etwas Grün in der Nähe.",
    "Ich suche etwas in Lausanne in der Nähe von EPFL oder mit guter Metro-Anbindung dorthin, am besten 1 bis 2 Zimmer, möbliert, und insgesamt eher praktisch als fancy.",
    "Wir möchten in Basel wohnen, am liebsten mit 2 Schlafzimmern, guter Tram-Anbindung, Waschturm, und es wäre super, wenn Einkaufen zu Fuß gut möglich ist.",
    "Ich suche eine Wohnung, die sich einfach angenehm anfühlt - viel Licht, ruhige Straße, nicht zu anonym, eher ein Quartier, in dem man gern länger bleibt.",
    "Suche was in Zürich Kreis 3, 4 oder 5, unter 3200 CHF, mit mindestens 2.5 Zimmern und möglichst moderner Küche.",
    "Ich will vor allem keinen langen Arbeitsweg, der Rest ist relativ offen.",
    "Wir suchen etwas familienfreundliches, wo man mit Kindern gut leben kann.",
    "Ich hätte gern eine Wohnung mit gutem Schnitt, großen Fenstern und wenn möglich Balkon.",
    "Suche Wohnung in Winterthur, nicht zu weit vom Bahnhof, aber bitte nicht direkt an einer großen Straße.",
    "Ich suche eine 4 Zimmer Wohnung in Zürich Nord, idealerweise ab 95 m², Lift, 2 Badezimmer wären ein Plus, Miete bis 4200 CHF, und die Fahrt zur Kantonsschule Rämibühl sollte mit dem ÖV in ungefähr 30 Minuten machbar sein.",
    "Wir suchen eine Wohnung für 2 Erwachsene und 1 Kind im Raum Thalwil, Horgen oder Wädenswil, gern nahe am See, mit mindestens 3.5 Zimmern, ab 90 m², Budget bis 3800 CHF, Balkon oder kleiner Garten, und in einer Umgebung, die sich ruhig, sicher und nicht zu dicht bebaut anfühlt.",
    "Ich suche was Günstigeres in Basel, muss nicht groß sein, aber die Lage sollte praktisch sein.",
    "Am wichtigsten ist mir, dass die Wohnung ruhig ist und nicht dunkel.",
    "Ich suche eine Wohnung in Bern, am besten mit max 15 Minuten zum Bahnhof, 2 bis 3 Zimmern und nicht mehr als 2500 CHF.",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the predefined German apartment-search queries against the local API."
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
