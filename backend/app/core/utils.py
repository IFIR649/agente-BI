from __future__ import annotations

import hashlib
import re
import unicodedata
from datetime import date, datetime
from decimal import Decimal
from typing import Any

_WEEKDAY_CANONICAL = {
    "lunes": "Lunes",
    "monday": "Lunes",
    "martes": "Martes",
    "tuesday": "Martes",
    "miercoles": "Miercoles",
    "miércoles": "Miercoles",
    "wednesday": "Miercoles",
    "jueves": "Jueves",
    "thursday": "Jueves",
    "viernes": "Viernes",
    "friday": "Viernes",
    "sabado": "Sabado",
    "sábado": "Sabado",
    "saturday": "Sabado",
    "domingo": "Domingo",
    "sunday": "Domingo",
}


def normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    normalized = normalized.lower()
    normalized = re.sub(r"[_\-/]+", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def slugify(value: str) -> str:
    normalized = normalize_text(value)
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized)
    normalized = re.sub(r"-{2,}", "-", normalized).strip("-")
    return normalized or "dataset"


def singularize(value: str) -> str:
    if value.endswith("ies") and len(value) > 3:
        return f"{value[:-3]}y"
    if value.endswith("es") and len(value) > 3:
        return value[:-2]
    if value.endswith("s") and len(value) > 3:
        return value[:-1]
    return value


def pluralize(value: str) -> str:
    if value.endswith("y") and len(value) > 1:
        return f"{value[:-1]}ies"
    if value.endswith("s"):
        return value
    if value[-1:] in {"a", "e", "i", "o", "u"}:
        return f"{value}s"
    return f"{value}es"


def humanize_identifier(value: str) -> str:
    text = value.strip()
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", text)
    text = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1 \2", text)
    text = re.sub(r"([A-Za-z])(\d)", r"\1 \2", text)
    text = re.sub(r"(\d)([A-Za-z])", r"\1 \2", text)
    text = re.sub(r"[_\-/]+", " ", text)
    return re.sub(r"\s+", " ", text).strip().title()


def normalize_weekday_name(value: str | None) -> str | None:
    if value is None:
        return None
    return _WEEKDAY_CANONICAL.get(normalize_text(value))


def build_cache_key(dataset_id: str, question: str, catalog_version: str, context: str | None = None) -> str:
    payload = f"{dataset_id}|{normalize_text(question)}|{normalize_text(context or '')}|{catalog_version}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def jsonable_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return value


def percent_change(current: float | int | None, previous: float | int | None) -> tuple[str | None, str | None]:
    if current is None or previous is None:
        return None, None
    if previous == 0:
        if current == 0:
            return "0.0%", "flat"
        return None, "up"
    delta = ((float(current) - float(previous)) / float(previous)) * 100
    direction = "flat"
    if delta > 0.001:
        direction = "up"
    elif delta < -0.001:
        direction = "down"
    return f"{delta:+.1f}%", direction
