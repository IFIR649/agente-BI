from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


ColumnType = Literal["string", "integer", "float", "date", "datetime", "boolean", "unknown"]
SemanticRole = Literal["time", "measure", "flag", "category", "identifier", "unknown"]
DimensionKind = Literal["column", "time_granularity"]
Granularity = Literal["day", "week", "month", "year", "day_of_week"]
MetricAggregator = Literal["sum", "avg", "min", "max", "count"]
_UPLOADS_DIR = Path("uploads")


def _normalize_logical_path(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    normalized = normalized.lstrip("/")
    if normalized.startswith("data/"):
        normalized = normalized[len("data/"):]
    return normalized or None


class ColumnProfile(BaseModel):
    name: str
    type: ColumnType
    label: str | None = None
    nullable: bool = True
    cardinality: int | None = None
    min_value: Any | None = None
    max_value: Any | None = None
    sample_values: list[Any] = Field(default_factory=list)
    detected_date_format: str | None = None
    detected_number_format: str | None = None  # "european" (1.234,56) | None
    non_null_ratio: float = 0.0
    uniqueness_ratio: float = 0.0
    numeric_parse_success_rate: float = 0.0
    date_parse_success_rate: float = 0.0
    zero_ratio: float = 0.0
    decimal_ratio: float = 0.0
    boolean_like: bool = False
    semantic_role: SemanticRole = "unknown"


class DimensionDefinition(BaseModel):
    name: str
    label: str
    expression: str
    order_expression: str | None = None
    kind: DimensionKind = "column"
    source_column: str | None = None
    granularity: Granularity | None = None


class MetricDefinition(BaseModel):
    name: str
    label: str
    formula: str
    description: str
    aggregator: MetricAggregator
    source_column: str | None = None


class UploadMetadata(BaseModel):
    display_name: str | None = None
    description: str | None = None
    default_date_column: str | None = None
    aliases: dict[str, str] = Field(default_factory=dict)


class DatasetCatalog(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: str
    filename: str
    display_name: str
    description: str | None = None
    storage_path: str | None = None
    logical_path: str | None = None
    row_count: int
    columns: dict[str, ColumnProfile]
    date_columns: list[str] = Field(default_factory=list)
    default_date_column: str | None = None
    dimensions_allowed: list[str]
    dimension_definitions: dict[str, DimensionDefinition]
    metrics_allowed: list[MetricDefinition]
    aliases: dict[str, str] = Field(default_factory=dict)
    manual_aliases: dict[str, str] = Field(default_factory=dict)
    label_overrides: dict[str, str] = Field(default_factory=dict)
    sample_rows: list[dict[str, Any]] = Field(default_factory=list)
    default_metric: str | None = None
    suggested_metrics: list[str] = Field(default_factory=list)
    suggested_dimensions: list[str] = Field(default_factory=list)
    created_at: datetime
    catalog_version: str

    @property
    def metrics_index(self) -> dict[str, MetricDefinition]:
        return {metric.name: metric for metric in self.metrics_allowed}

    def canonical_logical_path(self, *, data_dir: Path) -> str | None:
        normalized = _normalize_logical_path(self.logical_path)
        if normalized is not None:
            return normalized

        legacy_filename = Path(self.storage_path).name if self.storage_path else ""
        if legacy_filename:
            legacy_candidate = _UPLOADS_DIR / legacy_filename
            if (data_dir / legacy_candidate).exists():
                return legacy_candidate.as_posix()

        fallback_candidate = _UPLOADS_DIR / f"{self.id}.csv"
        if (data_dir / fallback_candidate).exists():
            return fallback_candidate.as_posix()
        return None

    def resolve_csv_path(self, *, data_dir: Path) -> Path | None:
        logical_path = _normalize_logical_path(self.logical_path)
        if logical_path is not None:
            candidate = data_dir / Path(logical_path)
            return candidate if candidate.exists() else None

        recovered_logical_path = self.canonical_logical_path(data_dir=data_dir)
        if recovered_logical_path is None:
            return None

        candidate = data_dir / Path(recovered_logical_path)
        return candidate if candidate.exists() else None

    def to_summary(self) -> "DatasetSummary":
        logical_path = _normalize_logical_path(self.logical_path)
        if logical_path is None:
            raise ValueError("El catalogo no tiene logical_path disponible.")
        return DatasetSummary(
            id=self.id,
            filename=self.filename,
            display_name=self.display_name,
            description=self.description,
            logical_path=logical_path,
            row_count=self.row_count,
            columns=self.columns,
            date_columns=self.date_columns,
            default_date_column=self.default_date_column,
            dimensions_allowed=self.dimensions_allowed,
            metrics_allowed=self.metrics_allowed,
            aliases=self.aliases,
            default_metric=self.default_metric,
            suggested_metrics=self.suggested_metrics,
            suggested_dimensions=self.suggested_dimensions,
            created_at=self.created_at,
            catalog_version=self.catalog_version,
        )


class DatasetSummary(BaseModel):
    id: str
    filename: str
    display_name: str
    description: str | None = None
    logical_path: str
    row_count: int
    columns: dict[str, ColumnProfile]
    date_columns: list[str] = Field(default_factory=list)
    default_date_column: str | None = None
    dimensions_allowed: list[str]
    metrics_allowed: list[MetricDefinition]
    aliases: dict[str, str] = Field(default_factory=dict)
    default_metric: str | None = None
    suggested_metrics: list[str] = Field(default_factory=list)
    suggested_dimensions: list[str] = Field(default_factory=list)
    created_at: datetime
    catalog_version: str
