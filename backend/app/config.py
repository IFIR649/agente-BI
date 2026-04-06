from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


APP_DIR = Path(__file__).resolve().parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="AGENT_",
        env_file=".env",
        extra="ignore",
    )

    app_name: str = "CSV Analysis Agent"
    app_env: str = "development"
    data_dir: Path = APP_DIR / "data"
    uploads_dir: Path = APP_DIR / "data" / "uploads"
    catalogs_dir: Path = APP_DIR / "data" / "catalogs"
    logs_dir: Path = APP_DIR / "logs"
    audit_db_path: Path = APP_DIR / "logs" / "audit.db"

    duckdb_database: str = ":memory:"
    query_timeout_seconds: int = 5
    max_upload_size_mb: int = 25
    max_top_n: int = 100

    cache_ttl_seconds: int = 600
    rate_limit_requests: int = 20
    rate_limit_window_seconds: int = 60

    gemini_api_key: str = ""
    gemini_flash_model: str = "gemini-2.5-flash"
    gemini_pro_model: str = "gemini-2.5-flash"
    gemini_lite_model: str = "gemini-2.5-flash-lite"
    gemini_timeout_seconds: int = 90
    gemini_context_cache_ttl_hours: int = 2
    gemini_context_cache_failure_cooldown_seconds: int = 900
    gemini_temperature_intent: float = 0.1
    gemini_temperature_summary: float = 0.2
    allow_local_gemini_fallback: bool = True
    banxico_fix_url: str = "https://www.banxico.org.mx/tipcamb/tipCamIHAction.do"
    banxico_timeout_seconds: int = 10

    cors_origins: list[str] = Field(default_factory=lambda: ["*"])

    def ensure_directories(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.uploads_dir.mkdir(parents=True, exist_ok=True)
        self.catalogs_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)


@lru_cache
def get_settings() -> Settings:
    settings = Settings()
    settings.ensure_directories()
    return settings
