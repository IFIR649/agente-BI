from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from backend.app.config import Settings, get_settings
from backend.app.core.active_dataset import ActiveDatasetStore
from backend.app.core.audit import AuditLogger
from backend.app.core.cache import TTLCache
from backend.app.core.database import DuckDBManager
from backend.app.core.fx import BanxicoFxResolver
from backend.app.core.gemini_client import GeminiClient
from backend.app.core.rate_limiter import InMemoryRateLimiter
from backend.app.routers.datasets import router as datasets_router
from backend.app.routers.health import router as health_router
from backend.app.routers.metrics import router as metrics_router
from backend.app.routers.query import router as query_router
from backend.app.services.dataset_profiler import DatasetProfiler
from backend.app.services.intent_parser import IntentParser
from backend.app.services.query_executor import QueryExecutor
from backend.app.services.response_builder import ResponseBuilder
from backend.app.services.summary_writer import SummaryWriter


STATIC_DIR = Path(__file__).resolve().parent / "static"


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    settings.ensure_directories()

    app = FastAPI(title=settings.app_name)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    db_manager = DuckDBManager(settings)
    gemini_client = GeminiClient(settings)
    fx_resolver = BanxicoFxResolver(
        db_path=settings.audit_db_path,
        service_url=settings.banxico_fix_url,
        timeout_seconds=settings.banxico_timeout_seconds,
    )

    app.state.settings = settings
    app.state.db_manager = db_manager
    app.state.gemini_client = gemini_client
    app.state.fx_resolver = fx_resolver
    app.state.cache = TTLCache(settings.cache_ttl_seconds)
    app.state.rate_limiter = InMemoryRateLimiter(settings.rate_limit_requests, settings.rate_limit_window_seconds)
    app.state.audit_logger = AuditLogger(settings.audit_db_path, fx_resolver=fx_resolver)
    app.state.active_dataset_store = ActiveDatasetStore(settings.audit_db_path)
    app.state.dataset_profiler = DatasetProfiler(settings, db_manager, gemini_client)
    app.state.intent_parser = IntentParser(settings, gemini_client)
    app.state.query_executor = QueryExecutor(settings, db_manager)
    app.state.response_builder = ResponseBuilder()
    app.state.summary_writer = SummaryWriter(settings, gemini_client)

    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/")
    async def root() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/test")
    async def test_ui() -> FileResponse:
        return FileResponse(STATIC_DIR / "test.html")

    @app.get("/analytics")
    async def analytics() -> FileResponse:
        return FileResponse(STATIC_DIR / "analytics.html")

    @app.get("/api-info")
    async def api_info() -> dict[str, str]:
        return {
            "status": "ok",
            "message": "CSV Analysis Agent API",
            "ui_url": "/",
            "test_ui_url": "/test",
            "analytics_ui_url": "/analytics",
            "docs_url": "/docs",
            "health_url": "/health",
            "datasets_url": "/datasets",
            "query_url": "/query",
            "metrics_queries_url": "/metrics/queries",
            "metrics_summary_url": "/metrics/summary",
            "metrics_timeseries_url": "/metrics/timeseries",
        }

    app.include_router(health_router)
    app.include_router(datasets_router)
    app.include_router(query_router)
    app.include_router(metrics_router)

    return app


app = create_app()
