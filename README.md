# Swiss Real Estate Search — Datathon 2026

**[Demo video](https://drive.google.com/file/d/1oHXUQN5mF1ih4IcYYAJVZHC4teNaRtvP/view)**

AI-powered real estate search for the Swiss market. Natural-language queries are parsed into hard filters and soft preferences by Claude Opus, candidates are retrieved from a SQLite database, and results are ranked by a multi-signal scorer combining semantic text embeddings, image embeddings, POI proximity, feature matching, and numeric preferences.

---

## Architecture

```
User query (natural language)
        │
        ▼
┌───────────────────┐
│  Query Parser     │  Claude Opus — tool use → hard + soft requirements
│  query_parser.py  │  Conversation history preserved across turns
└────────┬──────────┘
         │ hard requirements
         ▼
┌───────────────────┐
│  Hard Filter      │  SQLite WHERE clauses (city, price, rooms, features…)
│  hard_filter.py   │  Auto-relaxation when < 5 results
└────────┬──────────┘
         │ candidate listings (≤ 200)
         ▼
┌───────────────────────────────────────────────┐
│  Multi-Signal Ranker   ranking.py             │
│                                               │
│  • Text score      (25%)  Amazon Titan embed  │
│  • POI proximity   (35%)  Nominatim + Gauss   │
│  • Feature match   (20%)  boolean overlap     │
│  • Numeric prefs   (15%)  Gaussian decay      │
│  • Image embed      (7%)  Titan vision        │
│                                               │
│  Weights rebalance automatically when a       │
│  signal is absent (no images, no POIs…)       │
└────────┬──────────────────────────────────────┘
         │ ranked results
         ▼
     POST /listings  →  JSON response
```

### Data enrichment (offline, run once)

Raw CSVs from COMPARIS and SRED are enriched before being loaded into SQLite:

| Script | What it does |
|---|---|
| `scripts/enrich_csvs.py` | Extracts 14 features per listing (floor, dates, 8 boolean amenities) using **Claude Haiku** via the Batch API — 50% cheaper than synchronous calls |
| `scripts/enrich_locations_from_geo.py` | Reverse-geocodes SRED listings: GPS coordinates → street, city, postal code, canton via Nominatim |
| `scripts/fill_canton_from_zip.py` | Forward-geocodes missing cantons from postal codes |
| `scripts/add_is_furnished.py` | Classifies furnished / unfurnished / unknown using priority-ordered text patterns across 4 languages |

---

## Running with Docker

### Prerequisites

- Docker and Docker Compose installed
- A `.env` file in the project root (see below)
- `raw_data/` present (extracted from the organizer-provided bundle)

### 1. Create a `.env` file

```env
# Required — either key name works
CLAUDE_API_KEY=sk-ant-...

# Optional Claude config
CLAUDE_MODEL=claude-opus-4-7
CLAUDE_API_BASE_URL=https://api.anthropic.com
CLAUDE_TIMEOUT_SECONDS=30

# Optional AWS (for semantic search via OpenSearch + Titan embeddings)
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_DEFAULT_REGION=eu-central-2
```

If `CLAUDE_API_KEY` is not set, the API still runs but query parsing falls back to empty filters.
If AWS credentials are not set, semantic and image ranking fall back to keyword matching.

### 2. Start all services

```bash
docker compose up --build
```

This starts three containers:

| Container | Port | Description |
|---|---|---|
| `api` | 8000 | FastAPI search service |
| `mcp` | 8001 | MCP bridge for Claude Desktop / ChatGPT |
| `frontend` | 8081 | React widget (map + ranked list) |

The SQLite database is built automatically from `raw_data/` on first startup and stored in a named Docker volume (`listings_data`) so it persists across restarts.

### 3. Query the API

```bash
curl -X POST http://localhost:8000/listings \
  -H "content-type: application/json" \
  -d '{
    "query": "3-room apartment in Zurich under 2500 CHF, close to a primary school",
    "limit": 10,
    "offset": 0
  }'
```

### 4. Reset the database

```bash
docker compose down -v   # removes the listings_data volume
docker compose up --build
```

---

## API

### `POST /listings`

Natural-language search. Runs the full pipeline: query parsing → hard filter → ranking.

**Request**
```json
{
  "query": "bright 4-room flat in Basel, pets allowed, max 2800 CHF",
  "limit": 25,
  "offset": 0,
  "conversation": []
}
```

**Response**
```json
{
  "listings": [
    {
      "listing_id": "abc123",
      "score": 0.87,
      "listing": { "title": "...", "city": "Basel", "price": 2650, "rooms": 4.0 }
    }
  ],
  "meta": {
    "hard_requirements": { "city": "Basel", "max_price": 2800 },
    "relaxation_log": []
  }
}
```

### `POST /listings/search/filter`

Low-level structured search, bypasses query parsing.

```bash
curl -X POST http://localhost:8000/listings/search/filter \
  -H "content-type: application/json" \
  -d '{
    "hard_filters": {
      "city": ["Zurich"],
      "max_price": 3000,
      "min_rooms": 2.5,
      "features": ["balcony", "pets_allowed"],
      "limit": 10
    }
  }'
```

### `GET /health`

```bash
curl http://localhost:8000/health
```

---

## Project structure

```
app/
  api/routes/listings.py        API endpoints
  harness/
    bootstrap.py                DB bootstrap on startup
    csv_import.py               CSV → SQLite import + schema
    search_service.py           Orchestration (parse → filter → rank)
  participant/
    query_parser.py             Claude Opus — NL → hard + soft requirements
    hard_filter.py              SQLite filtering with auto-relaxation
    ranking.py                  Multi-signal scorer
    listing_row_parser.py       CSV row → structured listing
    soft_fact_extraction.py     Claude Opus — soft preferences + POIs
  core/
    hard_filters.py             Filter SQL builder
    s3.py                       AWS S3 image helpers
  models/schemas.py             Pydantic request/response models

scripts/
  enrich_csvs.py                Claude Haiku batch feature extraction
  enrich_locations_from_geo.py  GPS → address (Nominatim)
  fill_canton_from_zip.py       Postal code → canton
  add_is_furnished.py           Furnished classification

apps_sdk/
  server/main.py                MCP server (search_listings tool)
  web/                          Vite + React widget

raw_data/                       Source CSVs (from organizer bundle)
data/                           Generated SQLite database (gitignored)
presentation/                   Architecture diagrams
```

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `CLAUDE_API_KEY` | — | Anthropic API key (also accepts `ANTHROPIC_API_KEY`) |
| `CLAUDE_MODEL` | `claude-opus-4-7` | Model used for query parsing |
| `CLAUDE_API_BASE_URL` | `https://api.anthropic.com` | API base URL |
| `CLAUDE_TIMEOUT_SECONDS` | `30` | Request timeout |
| `LISTINGS_DB_PATH` | `data/listings.db` | SQLite database path |
| `LISTINGS_RAW_DATA_DIR` | `raw_data/` | Directory containing source CSVs |
| `LISTINGS_ALLOW_ALL_CORS` | `false` | Set to `true` to allow all origins |
| `AWS_ACCESS_KEY_ID` | — | For Titan embeddings + OpenSearch |
| `AWS_SECRET_ACCESS_KEY` | — | |
| `AWS_DEFAULT_REGION` | `eu-central-2` | |
