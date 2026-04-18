from __future__ import annotations

from contextlib import asynccontextmanager
import logging
import time

from fastapi import FastAPI
from fastapi import Request
from fastapi.middleware.cors import CORSMiddleware
from starlette.responses import Response
from starlette.staticfiles import StaticFiles

from app.api.routes.listings import router as listings_router
from app.config import get_settings
from app.harness.bootstrap import bootstrap_database

LOGGER = logging.getLogger(__name__)
ALLOWED_CORS_ORIGINS = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "http://localhost:8001",
    "http://127.0.0.1:8001",
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    LOGGER.info(
        "Starting listings harness with raw_data_dir=%s db_path=%s claude_configured=%s",
        settings.raw_data_dir,
        settings.db_path,
        bool(settings.claude_api_key),
    )
    bootstrap_database(db_path=settings.db_path, raw_data_dir=settings.raw_data_dir)
    LOGGER.info("Bootstrap completed successfully")
    yield


app = FastAPI(
    title="Datathon 2026 Listings Harness",
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_CORS_ORIGINS,
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(listings_router)


@app.middleware("http")
async def log_requests(request: Request, call_next) -> Response:
    start = time.perf_counter()
    client_host = request.client.host if request.client else "unknown"
    LOGGER.info(
        "Incoming request method=%s path=%s client=%s origin=%s",
        request.method,
        request.url.path,
        client_host,
        request.headers.get("origin"),
    )
    try:
        response = await call_next(request)
    except Exception:
        duration_ms = (time.perf_counter() - start) * 1000
        LOGGER.exception(
            "Request failed method=%s path=%s client=%s duration_ms=%.2f",
            request.method,
            request.url.path,
            client_host,
            duration_ms,
        )
        raise

    duration_ms = (time.perf_counter() - start) * 1000
    LOGGER.info(
        "Completed request method=%s path=%s status=%s duration_ms=%.2f",
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
    )
    return response

_sred_images_dir = get_settings().raw_data_dir / "sred_images"
if _sred_images_dir.exists():
    LOGGER.info("Mounting local SRED images from %s", _sred_images_dir)
    app.mount(
        "/raw-data-images",
        StaticFiles(directory=str(_sred_images_dir)),
        name="raw-data-images",
    )
