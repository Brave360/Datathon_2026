from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


@lru_cache(maxsize=1)
def _load_dotenv() -> None:
    dotenv_path = _project_root() / ".env"
    if not dotenv_path.exists():
        return

    for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            continue

        if (
            len(value) >= 2
            and value[0] == value[-1]
            and value[0] in {"'", '"'}
        ):
            value = value[1:-1]

        os.environ.setdefault(key, value)


def _find_default_raw_data_dir() -> Path:
    root = _project_root()
    configured = os.getenv("LISTINGS_RAW_DATA_DIR")
    if configured:
        return Path(configured)
    return root / "raw_data"


def _default_db_path() -> Path:
    configured = os.getenv("LISTINGS_DB_PATH")
    if configured:
        return Path(configured)
    return _project_root() / "data" / "listings.db"


@dataclass(slots=True)
class Settings:
    raw_data_dir: Path
    db_path: Path
    s3_bucket: str
    s3_region: str
    s3_prefix: str
    claude_api_key: str | None
    claude_model: str
    claude_api_base_url: str
    claude_timeout_seconds: float
    hard_facts_debug_log_path: Path


def get_settings() -> Settings:
    _load_dotenv()
    return Settings(
        raw_data_dir=_find_default_raw_data_dir(),
        db_path=_default_db_path(),
        s3_bucket=os.getenv(
            "LISTINGS_S3_BUCKET",
            "crawl-data-951752554117-eu-central-2-an",
        ),
        s3_region=os.getenv("LISTINGS_S3_REGION", "eu-central-2"),
        s3_prefix=os.getenv("LISTINGS_S3_PREFIX", "prod"),
        claude_api_key=os.getenv("CLAUDE_API_KEY") or os.getenv("ANTHROPIC_API_KEY"),
        claude_model=os.getenv("CLAUDE_MODEL", "claude-sonnet-4-5"),
        claude_api_base_url=os.getenv(
            "CLAUDE_API_BASE_URL",
            "https://api.anthropic.com",
        ),
        claude_timeout_seconds=float(os.getenv("CLAUDE_TIMEOUT_SECONDS", "20")),
        hard_facts_debug_log_path=Path(
            os.getenv(
                "HARD_FACTS_DEBUG_LOG_PATH",
                str(_project_root() / "data" / "hard_facts_debug.jsonl"),
            )
        ),
    )
