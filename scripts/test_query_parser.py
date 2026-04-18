#!/usr/bin/env python3
"""Quick test of the query parser with German-language queries."""
import json
import os
from pathlib import Path

# Load .env manually (no python-dotenv dependency needed)
env_path = Path(__file__).resolve().parents[1] / ".env"
for line in env_path.read_text(encoding="utf-8").splitlines():
    line = line.strip()
    if not line or line.startswith("#") or "=" not in line:
        continue
    key, _, value = line.partition("=")
    value = value.strip().strip('"').strip("'")
    os.environ.setdefault(key.strip(), value)

# Map CLAUDE_API_KEY → ANTHROPIC_API_KEY if needed
if "ANTHROPIC_API_KEY" not in os.environ and "CLAUDE_API_KEY" in os.environ:
    os.environ["ANTHROPIC_API_KEY"] = os.environ["CLAUDE_API_KEY"]

from app.participant.query_parser import parse_query_to_dict  # noqa: E402

QUERIES = [
    (
        "1 – Raum Zürich, ÖV Stadelhofen, ruhige Lage",
        "Ich suche eine Wohnung im Raum Zürich, Dübendorf oder Wallisellen, idealerweise 2.5 bis 3.5 Zimmer, "
        "ab 70 m², Budget bis 3100 CHF, max 25 Minuten mit dem ÖV bis Stadelhofen, gern mit Balkon, "
        "Waschmaschine in der Wohnung oder eigenem Waschturm, und wenn möglich in einer Gegend, "
        "die sich ruhig und nicht zu urban hektisch anfühlt.",
    ),
    (
        "2 – Familie Kilchberg/Rüschlikon/Thalwil, See, Schulen",
        "Wir suchen als Familie zu dritt etwas im Raum Kilchberg, Rüschlikon oder Thalwil, "
        "am liebsten nahe am See oder mit schneller Verbindung nach Zürich, mindestens 3.5 Zimmer, "
        "ab 90 m², Budget bis 4300 CHF, gern mit Balkon / Terrasse, Lift, Keller, und wichtig wären uns "
        "gute Schulen, Parks oder Spielplätze in der Nähe sowie eine Umgebung, in der man sich auch abends sicher fühlt.",
    ),
    (
        "3 – Oerlikon/Altstetten/Schlieren, commute Zürich HB",
        "I'm looking for an apartment in the greater Zurich area, ideally somewhere like Oerlikon, Altstetten, "
        "or Schlieren, with at least 60 sqm, preferably 2 to 3 rooms, a commute under 30 minutes to Zurich HB "
        "door to door, and it would be great if the place had a balcony, good light, and access to shops and "
        "public transport within walking distance.",
    ),
    (
        "4 – Familie Basel, 2-3 Schlafzimmer, kein Auto nötig",
        "We are a family of 3 looking around Basel for something with 2 or 3 bedrooms, ideally 85 sqm or more, "
        "budget up to CHF 3500, in an area with good schools, quiet streets, and enough nearby amenities that "
        "daily life is easy without needing a car all the time.",
    ),
    (
        "5 – Lausanne, EPFL, möbliert, sicher",
        "Ich suche etwas Kleineres in Lausanne, möglichst in der Nähe von EPFL, gern möbliert, "
        "unter 2100 CHF, mit guter Anbindung, und am besten in einer Ecke, die sich sicher, "
        "entspannt und nicht komplett anonym anfühlt.",
    ),
]

for label, query in QUERIES:
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    print(f"Query: {query}\n")
    result = parse_query_to_dict(query)
    print(json.dumps(result, indent=2, ensure_ascii=False))
