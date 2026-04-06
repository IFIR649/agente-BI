from __future__ import annotations

import functools
import json
import logging
import time
from dataclasses import dataclass
from typing import Any, TypeVar

from pydantic import BaseModel

from backend.app.config import Settings

logger = logging.getLogger(__name__)


T = TypeVar("T", bound=BaseModel)


@dataclass(frozen=True)
class GeminiUsageMetrics:
    model: str
    latency_ms: int
    prompt_token_count: int = 0
    output_token_count: int = 0
    thoughts_token_count: int = 0
    tool_use_prompt_token_count: int = 0
    cached_content_token_count: int = 0
    total_token_count: int = 0
    status: str = "ok"


@dataclass(frozen=True)
class GeminiCallResult:
    payload: Any
    metrics: GeminiUsageMetrics


class GeminiClientError(RuntimeError):
    """Base exception for Gemini client failures."""


class GeminiUnavailableError(GeminiClientError):
    """Raised when Gemini is not configured in the current environment."""


class GeminiResponseFormatError(GeminiClientError):
    """Raised when Gemini responds but not in the expected structured format."""


class GeminiClient:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    @property
    def configured(self) -> bool:
        return bool(self.settings.gemini_api_key)

    def _load_sdk(self) -> tuple[Any, Any]:
        try:
            from google import genai
            from google.genai import types
        except ImportError as exc:
            raise GeminiUnavailableError("google-genai no esta instalado.") from exc
        return genai, types

    def generate_structured(
        self,
        *,
        system_instruction: str,
        prompt: str,
        response_model: type[T],
        model: str,
        temperature: float,
        cached_content_name: str | None = None,
    ) -> T:
        result = self.generate_structured_result(
            system_instruction=system_instruction,
            prompt=prompt,
            response_model=response_model,
            model=model,
            temperature=temperature,
            cached_content_name=cached_content_name,
        )
        return result.payload

    @functools.cached_property
    def _client(self) -> Any:
        genai, types = self._load_sdk()
        return genai.Client(
            api_key=self.settings.gemini_api_key,
            http_options=types.HttpOptions(timeout=self.settings.gemini_timeout_seconds * 1000),
        )

    def create_cached_content(
        self,
        *,
        system_instruction: str,
        model: str,
        ttl_hours: int = 2,
    ) -> str:
        if not self.configured:
            raise GeminiUnavailableError("GEMINI_API_KEY no configurada.")

        _, types = self._load_sdk()
        try:
            cached = self._client.caches.create(
                model=model,
                config=types.CreateCachedContentConfig(
                    system_instruction=system_instruction,
                    ttl=f"{ttl_hours * 3600}s",
                ),
            )
        except Exception as exc:  # pragma: no cover - external service
            raise GeminiClientError(f"Error creando cache en Gemini: {exc}") from exc

        cache_name = str(getattr(cached, "name", "") or "")
        if not cache_name:
            raise GeminiClientError("Gemini no devolvio un nombre de cache reutilizable.")
        return cache_name

    def generate_structured_result(
        self,
        *,
        system_instruction: str,
        prompt: str,
        response_model: type[T],
        model: str,
        temperature: float,
        cached_content_name: str | None = None,
    ) -> GeminiCallResult:
        if not self.configured:
            raise GeminiUnavailableError("GEMINI_API_KEY no configurada.")

        _, types = self._load_sdk()
        started_at = time.perf_counter()

        try:
            config_kwargs: dict[str, Any] = {
                "temperature": temperature,
                "response_mime_type": "application/json",
                "response_schema": response_model,
            }
            if cached_content_name:
                config_kwargs["cached_content"] = cached_content_name
            else:
                config_kwargs["system_instruction"] = system_instruction
            response = self._client.models.generate_content(
                model=model,
                contents=prompt,
                config=types.GenerateContentConfig(**config_kwargs),
            )
        except Exception as exc:  # pragma: no cover - external service
            raise GeminiClientError(f"Error llamando Gemini: {exc}") from exc

        metrics = self._build_metrics(model=model, response=response, started_at=started_at)
        parsed = getattr(response, "parsed", None)
        if parsed is not None:
            if isinstance(parsed, response_model):
                logger.debug("generate_structured model=%s parsed_type=direct", model)
                return GeminiCallResult(payload=parsed, metrics=metrics)
            if isinstance(parsed, BaseModel):
                return GeminiCallResult(payload=response_model.model_validate(parsed.model_dump()), metrics=metrics)
            if isinstance(parsed, dict):
                return GeminiCallResult(payload=response_model.model_validate(parsed), metrics=metrics)

        raw_text = getattr(response, "text", None)
        if not raw_text:
            logger.warning("generate_structured model=%s error=no_content", model)
            raise GeminiResponseFormatError("Gemini no devolvio contenido estructurado.")

        try:
            payload = json.loads(raw_text)
            return GeminiCallResult(payload=response_model.model_validate(payload), metrics=metrics)
        except Exception as exc:  # pragma: no cover - external service
            logger.warning("generate_structured model=%s error=invalid_json raw=%s", model, raw_text[:200])
            raise GeminiResponseFormatError("La respuesta estructurada de Gemini no fue valida.") from exc

    def generate_text(
        self,
        *,
        system_instruction: str,
        prompt: str,
        model: str,
        temperature: float,
    ) -> str:
        result = self.generate_text_result(
            system_instruction=system_instruction,
            prompt=prompt,
            model=model,
            temperature=temperature,
        )
        return result.payload

    def generate_text_result(
        self,
        *,
        system_instruction: str,
        prompt: str,
        model: str,
        temperature: float,
    ) -> GeminiCallResult:
        if not self.configured:
            raise GeminiUnavailableError("GEMINI_API_KEY no configurada.")

        _, types = self._load_sdk()
        started_at = time.perf_counter()

        try:
            response = self._client.models.generate_content(
                model=model,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=temperature,
                    system_instruction=system_instruction,
                ),
            )
        except Exception as exc:  # pragma: no cover - external service
            raise GeminiClientError(f"Error llamando Gemini: {exc}") from exc

        metrics = self._build_metrics(model=model, response=response, started_at=started_at)
        text = getattr(response, "text", None)
        if not text:
            raise GeminiClientError("Gemini no devolvio texto.")
        return GeminiCallResult(payload=text.strip(), metrics=metrics)

    def _build_metrics(self, *, model: str, response: Any, started_at: float) -> GeminiUsageMetrics:
        usage = getattr(response, "usage_metadata", None)
        prompt_token_count = int(getattr(usage, "prompt_token_count", 0) or 0)
        output_token_count = int(
            getattr(usage, "candidates_token_count", None)
            or getattr(usage, "response_token_count", None)
            or 0
        )
        thoughts_token_count = int(getattr(usage, "thoughts_token_count", 0) or 0)
        tool_use_prompt_token_count = int(getattr(usage, "tool_use_prompt_token_count", 0) or 0)
        cached_content_token_count = int(getattr(usage, "cached_content_token_count", 0) or 0)
        total_token_count = int(
            getattr(usage, "total_token_count", 0)
            or (
                prompt_token_count
                + output_token_count
                + thoughts_token_count
                + tool_use_prompt_token_count
            )
        )
        return GeminiUsageMetrics(
            model=str(getattr(response, "model_version", None) or model),
            latency_ms=int((time.perf_counter() - started_at) * 1000),
            prompt_token_count=prompt_token_count,
            output_token_count=output_token_count,
            thoughts_token_count=thoughts_token_count,
            tool_use_prompt_token_count=tool_use_prompt_token_count,
            cached_content_token_count=cached_content_token_count,
            total_token_count=total_token_count,
        )
