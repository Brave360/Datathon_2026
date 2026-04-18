from __future__ import annotations

import copy
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from app.db import get_connection
from app.participant.query_parser import Requirements

# Parser output → actual DB object_category values
# DB unique values (with counts): Wohnung(7757), Gewerbeobjekt(2273),
# Parkplatz/Garage variants(~1456), Möblierte Wohnung(414), Haus(358),
# Bastelraum(250), Einzelzimmer(232), Dachwohnung(200), Maisonette(140),
# Einzelgarage(97), Studio(94), Attika(63), WG-Zimmer(62), Loft(48),
# Villa(35), Diverses(33), Doppeleinfamilienhaus(25), Reihenhaus(13),
# Bauernhaus(7), Ferienwohnung(5), Terrassenwohnung(4), Mehrfamilienhaus(4),
# Wohnnebenraeume(3), Grundstück(2), Ferienimmobilie(2), Terrassenhaus(1),
# Gastgewerbe(1) — plus None(12064, the largest group)
_CATEGORY_MAP: dict[str, list[str]] = {
    # Möblierte Wohnung is included in APARTMENT — furnished is an attribute, not a type
    "APARTMENT": [
        "Wohnung", "Möblierte Wohnung", "Dachwohnung", "Maisonette",
        "Attika", "Loft", "Studio", "Terrassenwohnung",
    ],
    "HOUSE": [
        "Haus", "Villa", "Doppeleinfamilienhaus", "Reihenhaus",
        "Mehrfamilienhaus", "Bauernhaus", "Terrassenhaus",
    ],
    "STUDIO": ["Studio"],
    "ROOM": ["Einzelzimmer", "WG-Zimmer"],
    "LOFT": ["Loft"],
    "PARKING": ["Parkplatz"],
    "GARAGE": ["Parkplatz, Garage", "Parkplatz", "Tiefgarage", "Einzelgarage"],
    "COMMERCIAL": ["Gewerbeobjekt", "Gastgewerbe"],
    "VACATION": ["Ferienwohnung", "Ferienimmobilie"],
    "STORAGE": ["Bastelraum", "Wohnnebenraeume"],
    "LAND": ["Grundstück"],
    "OTHER": ["Diverses"],
}

# Residential categories used for default filtering when user doesn't specify
RESIDENTIAL_CATEGORIES = [
    "Wohnung", "Möblierte Wohnung", "Dachwohnung", "Maisonette", "Attika",
    "Loft", "Studio", "Terrassenwohnung", "Haus", "Villa",
    "Doppeleinfamilienhaus", "Reihenhaus", "Mehrfamilienhaus", "Bauernhaus",
    "Terrassenhaus", "Einzelzimmer", "WG-Zimmer",
]

# Common parser city output → canonical DB city
_CITY_ALIASES: dict[str, str] = {
    "zurich": "Zürich",
    "zürich": "Zürich",
    "zuerich": "Zürich",
    "geneva": "Genève",
    "geneve": "Genève",
    "genf": "Genève",
    "bern": "Bern",
    "basel": "Basel",
    "lausanne": "Lausanne",
    "lugano": "Lugano",
    "winterthur": "Winterthur",
    "st. gallen": "Sankt Gallen",
    "st gallen": "Sankt Gallen",
    "sankt gallen": "Sankt Gallen",
}

_SELECT = """
    SELECT
        listing_id, title, description, street, city, postal_code, canton,
        price, rooms, area, available_from, latitude, longitude,
        feature_balcony, feature_elevator, feature_parking, feature_garage,
        feature_fireplace, feature_child_friendly, feature_pets_allowed,
        feature_temporary, feature_new_build, feature_wheelchair_accessible,
        feature_private_laundry, feature_minergie_certified, is_furnished,
        bedrooms, bathrooms, year_built, last_renovation,
        offer_type, object_category, object_type, original_url,
        features_json, images_json
    FROM listings
"""

_FEATURE_ATTRS = [
    "feature_balcony", "feature_elevator", "feature_parking", "feature_garage",
    "feature_fireplace", "feature_child_friendly", "feature_pets_allowed",
    "feature_temporary", "feature_new_build", "feature_wheelchair_accessible",
    "feature_private_laundry", "feature_minergie_certified",
    # is_furnished is a derived column (see scripts/add_is_furnished.py)
    "is_furnished",
]

# Relaxation sequence: (field, action, delta)
# action: "clear" | "increase" | "decrease"
_RELAX_SEQUENCE: list[tuple[str, str, Any]] = [
    ("min_area", "decrease", 10.0),
    ("max_price", "increase", 200),
    ("min_rooms", "decrease", 0.5),
    ("max_rooms", "increase", 0.5),
    ("min_bedrooms", "decrease", 1),
    ("max_bedrooms", "increase", 1),
    ("min_bathrooms", "decrease", 1),
    ("object_category", "clear", None),
    ("feature_balcony", "clear", None),
    ("feature_elevator", "clear", None),
    ("feature_parking", "clear", None),
    ("feature_garage", "clear", None),
    ("feature_fireplace", "clear", None),
    ("feature_new_build", "clear", None),
    ("feature_private_laundry", "clear", None),
    ("feature_minergie_certified", "clear", None),
    ("is_furnished", "clear", None),
    ("feature_child_friendly", "clear", None),
    ("feature_temporary", "clear", None),
    ("feature_pets_allowed", "clear", None),
    ("feature_wheelchair_accessible", "clear", None),
    ("city", "clear", None),
    ("canton", "clear", None),
    ("postal_code", "clear", None),
    ("offer_type", "clear", None),
]

# Tightening sequence: promote soft→hard in this order
_TIGHTEN_SEQUENCE = [
    "offer_type", "city", "min_rooms", "max_price", "object_category",
]


def _normalize_city(city: str) -> str:
    return _CITY_ALIASES.get(city.lower(), city)


def _normalize_category(cat: str) -> list[str]:
    return _CATEGORY_MAP.get(cat.upper(), [cat])


def _build_where(
    req: Requirements,
    default_offer_type: str | None = None,
    default_categories: list[str] | None = None,
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []

    if req.city:
        normalized = _normalize_city(req.city)
        clauses.append("LOWER(city) = ?")
        params.append(normalized.lower())

    if req.canton:
        clauses.append("UPPER(canton) = ?")
        params.append(req.canton.upper())

    if req.postal_code:
        clauses.append("postal_code = ?")
        params.append(req.postal_code)

    if req.min_price is not None:
        clauses.append("price >= ?")
        params.append(req.min_price)

    if req.max_price is not None:
        clauses.append("price <= ?")
        params.append(req.max_price)

    if req.min_rooms is not None:
        clauses.append("rooms >= ?")
        params.append(req.min_rooms)

    if req.max_rooms is not None:
        clauses.append("rooms <= ?")
        params.append(req.max_rooms)

    if req.min_area is not None:
        clauses.append("area >= ?")
        params.append(req.min_area)

    if req.max_area is not None:
        clauses.append("area <= ?")
        params.append(req.max_area)

    offer_type = req.offer_type or default_offer_type
    if offer_type:
        clauses.append("UPPER(offer_type) = ?")
        params.append(offer_type.upper())

    if req.object_category:
        db_cats = _normalize_category(req.object_category)
        placeholders = ", ".join("?" for _ in db_cats)
        clauses.append(f"object_category IN ({placeholders})")
        params.extend(db_cats)
    elif default_categories:
        placeholders = ", ".join("?" for _ in default_categories)
        clauses.append(f"object_category IN ({placeholders})")
        params.extend(default_categories)

    if req.min_bedrooms is not None:
        clauses.append("bedrooms >= ?")
        params.append(req.min_bedrooms)

    if req.max_bedrooms is not None:
        clauses.append("bedrooms <= ?")
        params.append(req.max_bedrooms)

    if req.min_bathrooms is not None:
        clauses.append("bathrooms >= ?")
        params.append(req.min_bathrooms)

    if req.max_bathrooms is not None:
        clauses.append("bathrooms <= ?")
        params.append(req.max_bathrooms)

    if req.min_year_built is not None:
        clauses.append("year_built >= ?")
        params.append(req.min_year_built)

    if req.max_year_built is not None:
        clauses.append("year_built <= ?")
        params.append(req.max_year_built)

    for attr in _FEATURE_ATTRS:
        val = getattr(req, attr, None)
        if val is True:
            clauses.append(f"{attr} = 1")
        elif val is False:
            # NULL means unknown (not confirmed to have the feature), so treat as acceptable
            clauses.append(f"({attr} = 0 OR {attr} IS NULL)")

    where = " WHERE " + " AND ".join(clauses) if clauses else ""
    return where, params


def _count(
    db_path: Path,
    req: Requirements,
    default_offer_type: str | None = None,
    default_categories: list[str] | None = None,
) -> int:
    where, params = _build_where(req, default_offer_type, default_categories)
    sql = f"SELECT COUNT(*) FROM listings{where}"
    with get_connection(db_path) as conn:
        return conn.execute(sql, params).fetchone()[0]


def _fetch(
    db_path: Path,
    req: Requirements,
    default_offer_type: str | None = None,
    default_categories: list[str] | None = None,
) -> list[dict[str, Any]]:
    where, params = _build_where(req, default_offer_type, default_categories)
    sql = _SELECT + where + " ORDER BY listing_id ASC"
    with get_connection(db_path) as conn:
        rows = conn.execute(sql, params).fetchall()
    return [_parse_row(dict(row)) for row in rows]


def _parse_row(row: dict[str, Any]) -> dict[str, Any]:
    features_json = row.pop("features_json", "[]")
    images_json = row.pop("images_json", None)
    try:
        row["features"] = json.loads(features_json) if features_json else []
    except json.JSONDecodeError:
        row["features"] = []
    row["image_urls"] = _extract_image_urls(images_json)
    row["hero_image_url"] = row["image_urls"][0] if row["image_urls"] else None
    return row


def _extract_image_urls(images_json: Any) -> list[str]:
    if not images_json:
        return []
    try:
        parsed = json.loads(images_json) if isinstance(images_json, str) else images_json
    except json.JSONDecodeError:
        return []
    if not isinstance(parsed, dict):
        return []
    urls: list[str] = []
    for item in parsed.get("images", []) or []:
        if isinstance(item, dict) and item.get("url"):
            urls.append(str(item["url"]))
        elif isinstance(item, str) and item:
            urls.append(item)
    for item in parsed.get("image_paths", []) or []:
        if isinstance(item, str) and item:
            urls.append(item)
    return urls


def _relax_step(hard: Requirements, soft: Requirements) -> bool:
    """Move one criterion from hard to soft. Returns True if something changed."""
    for field_name, action, delta in _RELAX_SEQUENCE:
        val = getattr(hard, field_name)
        if val is None:
            continue
        if action == "clear":
            setattr(hard, field_name, None)
            if getattr(soft, field_name) is None:
                setattr(soft, field_name, val)
            return True
        elif action == "decrease" and isinstance(val, (int, float)):
            new_val = val - delta
            if new_val <= 0:
                setattr(hard, field_name, None)
                if getattr(soft, field_name) is None:
                    setattr(soft, field_name, val)
            else:
                setattr(hard, field_name, type(val)(new_val))
            return True
        elif action == "increase" and isinstance(val, (int, float)):
            setattr(hard, field_name, type(val)(val + delta))
            return True
    return False


def _tighten_step(hard: Requirements, soft: Requirements) -> bool:
    """Promote one criterion from soft to hard. Returns True if something changed."""
    for field_name in _TIGHTEN_SEQUENCE:
        val = getattr(soft, field_name)
        if val is None:
            continue
        if getattr(hard, field_name) is None:
            setattr(hard, field_name, val)
            setattr(soft, field_name, None)
            return True
    return False


@dataclass
class SearchResult:
    listings: list[dict[str, Any]]
    effective_hard: Requirements
    effective_soft: Requirements
    relaxation_log: list[str] = field(default_factory=list)
    total_before_page: int = 0


def search_with_relaxation(
    db_path: Path,
    hard: Requirements,
    soft: Requirements,
    *,
    min_results: int = 5,
    too_many: int = 300,
) -> SearchResult:
    eff_hard = copy.deepcopy(hard)
    eff_soft = copy.deepcopy(soft)
    log: list[str] = []

    # Defaults applied when the user/parser didn't specify: RENT + residential categories
    def_offer = "RENT" if not eff_hard.offer_type and not eff_soft.offer_type else None
    def_cats = RESIDENTIAL_CATEGORIES if not eff_hard.object_category and not eff_soft.object_category else None

    count = _count(db_path, eff_hard, def_offer, def_cats)

    tighten_rounds = 0
    while count > too_many and tighten_rounds < 10:
        if not _tighten_step(eff_hard, eff_soft):
            break
        count = _count(db_path, eff_hard, def_offer, def_cats)
        tighten_rounds += 1
        log.append(f"tightened → {count} results")

    relax_rounds = 0
    while count < min_results and relax_rounds < len(_RELAX_SEQUENCE):
        if not _relax_step(eff_hard, eff_soft):
            break
        count = _count(db_path, eff_hard, def_offer, def_cats)
        relax_rounds += 1
        log.append(f"relaxed → {count} results")

    listings = _fetch(db_path, eff_hard, def_offer, def_cats)
    return SearchResult(
        listings=listings,
        effective_hard=eff_hard,
        effective_soft=eff_soft,
        relaxation_log=log,
        total_before_page=count,
    )
