from __future__ import annotations

import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from backend.app.config import Settings
from backend.app.core.database import DuckDBManager, quote_identifier, quote_literal
from backend.app.core.gemini_client import GeminiClient
from backend.app.core.utils import humanize_identifier, normalize_text, pluralize, singularize, slugify
from backend.app.models.dataset import (
    ColumnProfile,
    DatasetCatalog,
    DatasetSummary,
    DimensionDefinition,
    MetricDefinition,
    UploadMetadata,
)
from pydantic import BaseModel, Field


DATE_FORMATS = [
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%Y/%m/%d",
    "%d-%m-%Y",
    "%Y-%m-%d %H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%Y-%m-%d %H:%M",
    "%m/%d/%Y %H:%M",
]

_EUROPEAN_NUMBER_RE = re.compile(r"^-?\d{1,3}(?:\.\d{3})*(?:,\d+)?$|^-?\d+(?:,\d+)?$")
_STANDARD_NUMBER_RE = re.compile(r"^-?\d{1,3}(?:,\d{3})*(?:\.\d+)?$|^-?\d+(?:\.\d+)?$")
_NUMERIC_NULL_TOKENS = {"", "null", "none", "n/a", "na", "-", "--"}
_BOOLEAN_TRUE = {"true", "verdadero", "yes", "si", "sí", "1"}
_BOOLEAN_FALSE = {"false", "falso", "no", "0"}
_MAX_PROFILE_SAMPLES = 50
_GRANULARITY_LABELS = {
    "day": "Dia",
    "week": "Semana",
    "month": "Mes",
    "year": "Año",
    "day_of_week": "Dia de la Semana",
}
_WEEKDAY_LABELS = {
    1: "Lunes",
    2: "Martes",
    3: "Miercoles",
    4: "Jueves",
    5: "Viernes",
    6: "Sabado",
    7: "Domingo",
}

NUMERIC_TYPES = {
    "tinyint",
    "smallint",
    "integer",
    "bigint",
    "hugeint",
    "utinyint",
    "usmallint",
    "uinteger",
    "ubigint",
    "float",
    "double",
    "decimal",
    "real",
}
DATE_TYPES = {"date"}
DATETIME_TYPES = {"timestamp", "timestamp_s", "timestamp_ms", "timestamp_ns", "timestamptz", "timestamp with time zone"}
BOOLEAN_TYPES = {"boolean", "bool"}
STRING_TYPES = {"varchar", "text", "blob", "uuid"}


class ColumnLabelsResponse(BaseModel):
    labels: dict[str, str] = Field(default_factory=dict)


class DatasetProfiler:
    def __init__(self, settings: Settings, db_manager: DuckDBManager, gemini_client: GeminiClient) -> None:
        self.settings = settings
        self.db_manager = db_manager
        self.gemini_client = gemini_client

    def profile_and_store(
        self,
        *,
        filename: str,
        content: bytes,
        metadata: UploadMetadata | None = None,
        generate_labels: bool = True,
    ) -> DatasetCatalog:
        metadata = metadata or UploadMetadata()
        dataset_id = self._generate_dataset_id(metadata.display_name or Path(filename).stem)
        csv_path = self.settings.uploads_dir / f"{dataset_id}.csv"
        csv_path.write_bytes(content)

        try:
            catalog = self._build_catalog(
                dataset_id=dataset_id,
                csv_path=csv_path,
                original_filename=filename,
                metadata=metadata,
                generate_labels=generate_labels,
            )
            self._save_catalog(catalog)
            return catalog
        except Exception:
            csv_path.unlink(missing_ok=True)
            raise

    def get_catalog(self, dataset_id: str) -> DatasetCatalog | None:
        catalog_path = self.settings.catalogs_dir / f"{dataset_id}.json"
        if not catalog_path.exists():
            return None
        return self._load_catalog(catalog_path)

    def list_catalogs(self) -> list[DatasetSummary]:
        catalogs: list[DatasetSummary] = []
        for path in sorted(self.settings.catalogs_dir.glob("*.json")):
            catalog = self._load_catalog(path)
            if catalog is not None:
                catalogs.append(catalog.to_summary())
        return sorted(catalogs, key=lambda item: item.created_at, reverse=True)

    def _save_catalog(self, catalog: DatasetCatalog) -> None:
        target = self.settings.catalogs_dir / f"{catalog.id}.json"
        target.write_text(
            json.dumps(catalog.model_dump(mode="json", exclude_none=True), ensure_ascii=True, indent=2),
            encoding="utf-8",
        )

    def _load_catalog(self, catalog_path: Path) -> DatasetCatalog | None:
        payload = json.loads(catalog_path.read_text(encoding="utf-8"))
        catalog = DatasetCatalog.model_validate(payload)
        catalog, normalized = self._normalize_catalog(catalog)
        csv_path = catalog.resolve_csv_path(data_dir=self.settings.data_dir)
        if csv_path is None:
            return None

        refreshed = self._refresh_catalog_if_needed(catalog, csv_path=csv_path)
        if refreshed is not catalog:
            return refreshed
        if normalized:
            self._save_catalog(catalog)
        return catalog

    def _normalize_catalog(self, catalog: DatasetCatalog) -> tuple[DatasetCatalog, bool]:
        updates: dict[str, str | None] = {}
        logical_path = catalog.canonical_logical_path(data_dir=self.settings.data_dir)
        if logical_path is not None and catalog.logical_path != logical_path:
            updates["logical_path"] = logical_path
        if logical_path is not None and catalog.storage_path is not None:
            updates["storage_path"] = None
        if not updates:
            return catalog, False
        return catalog.model_copy(update=updates), True

    def _refresh_catalog_if_needed(self, catalog: DatasetCatalog, *, csv_path: Path) -> DatasetCatalog:
        if not self._catalog_needs_refresh(catalog):
            return catalog
        refreshed = self._build_catalog(
            dataset_id=catalog.id,
            csv_path=csv_path,
            original_filename=catalog.filename,
            metadata=UploadMetadata(
                display_name=catalog.display_name,
                description=catalog.description,
                default_date_column=catalog.default_date_column,
            ),
            manual_aliases=catalog.manual_aliases or catalog.aliases,
            label_overrides=catalog.label_overrides,
            created_at=catalog.created_at,
        )
        self._save_catalog(refreshed)
        return refreshed

    def _catalog_needs_refresh(self, catalog: DatasetCatalog) -> bool:
        if not catalog.default_metric or not catalog.suggested_metrics or not catalog.suggested_dimensions:
            return True
        if any(profile.semantic_role == "unknown" for profile in catalog.columns.values()):
            return True
        if any(not (profile.label or "").strip() for profile in catalog.columns.values()):
            return True
        for date_column in catalog.date_columns:
            definition = catalog.dimension_definitions.get(f"{date_column}_day_of_week")
            if definition is None or definition.order_expression is None:
                return True
        return False

    def _build_catalog(
        self,
        *,
        dataset_id: str,
        csv_path: Path,
        original_filename: str,
        metadata: UploadMetadata,
        manual_aliases: dict[str, str] | None = None,
        label_overrides: dict[str, str] | None = None,
        created_at: datetime | None = None,
        generate_labels: bool = True,
    ) -> DatasetCatalog:
        manual_aliases = dict(manual_aliases or metadata.aliases)
        label_overrides = dict(label_overrides or {})
        with self.db_manager.session() as connection:
            try:
                connection.execute("SET statement_timeout='0s'")
            except Exception:
                pass
            safe_path = quote_literal(str(csv_path))
            connection.execute(
                f"CREATE TEMP TABLE dataset_view AS "
                f"SELECT * FROM read_csv_auto({safe_path}, SAMPLE_SIZE=-1, HEADER=TRUE)"
            )
            describe_rows = connection.execute("DESCRIBE SELECT * FROM dataset_view").fetchall()
            row_count = int(connection.execute("SELECT COUNT(*) FROM dataset_view").fetchone()[0])

            columns: dict[str, ColumnProfile] = {}
            for row in describe_rows:
                name = str(row[0])
                duckdb_type = str(row[1])
                columns[name] = self._profile_column(
                    connection,
                    name=name,
                    duckdb_type=duckdb_type,
                    row_count=row_count,
                )

            self._assign_semantic_roles(columns=columns, row_count=row_count)
            sample_rows = self._collect_sample_rows(connection, list(columns.keys()), n=3)
            generated_labels = (
                self._generate_column_labels(columns=columns, sample_rows=sample_rows)
                if generate_labels
                else {}
            )
            self._apply_column_labels(
                columns=columns,
                generated_labels=generated_labels,
                label_overrides=label_overrides,
            )
            date_columns = [name for name, profile in columns.items() if profile.semantic_role == "time"]
            default_date_column = self._select_default_date_column(date_columns=date_columns, columns=columns)
            if metadata.default_date_column and metadata.default_date_column in date_columns:
                default_date_column = metadata.default_date_column
            dimension_definitions = self._build_dimensions(columns=columns, date_columns=date_columns)
            metrics_allowed = self._build_metrics(columns)
            default_metric = self._select_default_metric(columns=columns, metrics_allowed=metrics_allowed)
            suggested_metrics = self._suggest_metrics(columns=columns, metrics_allowed=metrics_allowed, limit=4)
            suggested_dimensions = self._suggest_dimensions(
                columns=columns,
                dimension_definitions=dimension_definitions,
                date_columns=date_columns,
                limit=4,
            )
            aliases = self._build_aliases(
                columns=columns,
                dimension_definitions=dimension_definitions,
                metrics_allowed=metrics_allowed,
                default_date_column=default_date_column,
                manual_aliases=manual_aliases,
            )

        return DatasetCatalog(
            id=dataset_id,
            filename=original_filename,
            display_name=metadata.display_name or Path(original_filename).stem,
            description=metadata.description,
            logical_path=(Path("uploads") / csv_path.name).as_posix(),
            row_count=row_count,
            columns=columns,
            date_columns=date_columns,
            default_date_column=default_date_column,
            dimensions_allowed=sorted(dimension_definitions.keys()),
            dimension_definitions=dimension_definitions,
            metrics_allowed=metrics_allowed,
            aliases=aliases,
            manual_aliases=manual_aliases,
            label_overrides=label_overrides,
            sample_rows=sample_rows,
            default_metric=default_metric,
            suggested_metrics=suggested_metrics,
            suggested_dimensions=suggested_dimensions,
            created_at=created_at or datetime.now(timezone.utc),
            catalog_version=str(uuid.uuid4()),
        )

    def update_column_labels(self, dataset_id: str, column_labels: dict[str, str]) -> DatasetCatalog | None:
        catalog = self.get_catalog(dataset_id)
        if catalog is None:
            return None
        csv_path = catalog.resolve_csv_path(data_dir=self.settings.data_dir)
        if csv_path is None:
            return None

        invalid_columns = sorted(set(column_labels) - set(catalog.columns))
        if invalid_columns:
            raise ValueError(f"Columnas no disponibles para renombrar: {', '.join(invalid_columns)}.")

        overrides = dict(catalog.label_overrides)
        for name, label in column_labels.items():
            cleaned = self._clean_label(label)
            if not cleaned:
                raise ValueError(f"El label para {name} no puede estar vacio.")
            overrides[name] = cleaned

        updated = self._build_catalog(
            dataset_id=catalog.id,
            csv_path=csv_path,
            original_filename=catalog.filename,
            metadata=UploadMetadata(
                display_name=catalog.display_name,
                description=catalog.description,
                default_date_column=catalog.default_date_column,
            ),
            manual_aliases=catalog.manual_aliases or catalog.aliases,
            label_overrides=overrides,
            created_at=catalog.created_at,
        )
        self._save_catalog(updated)
        return updated

    def _profile_column(
        self,
        connection,
        *,
        name: str,
        duckdb_type: str,
        row_count: int,
    ) -> ColumnProfile:
        sql_name = quote_identifier(name)
        base_type = self._map_type(duckdb_type)
        null_count = int(connection.execute(f"SELECT COUNT(*) - COUNT({sql_name}) FROM dataset_view").fetchone()[0])
        non_null_count = max(row_count - null_count, 0)
        cardinality = int(connection.execute(f"SELECT COUNT(DISTINCT {sql_name}) FROM dataset_view").fetchone()[0]) if row_count else 0
        sample_values = self._collect_sample_values(connection, name)
        raw_samples = self._collect_raw_samples(connection, name)

        data_type = base_type
        detected_number_format = None
        detected_date_format = None
        numeric_parse_success_rate = 0.0
        date_parse_success_rate = 0.0
        zero_ratio = 0.0
        decimal_ratio = 0.0
        boolean_like = False
        min_value: Any | None = None
        max_value: Any | None = None

        if data_type in {"integer", "float"}:
            min_value, max_value, zero_ratio, decimal_ratio, numeric_parse_success_rate = self._compute_numeric_stats(
                connection=connection,
                source_column=name,
                numeric_expr=sql_name,
                non_null_count=non_null_count,
            )
            boolean_like = (
                non_null_count > 0
                and cardinality <= 2
                and min_value in {0, 0.0, False}
                and max_value in {1, 1.0, True}
            )
        elif data_type == "boolean":
            boolean_like = True
            min_value, max_value = connection.execute(f"SELECT MIN({sql_name}), MAX({sql_name}) FROM dataset_view").fetchone()
        elif data_type in {"date", "datetime"}:
            min_value, max_value = connection.execute(f"SELECT MIN({sql_name}), MAX({sql_name}) FROM dataset_view").fetchone()
            date_parse_success_rate = 1.0 if non_null_count else 0.0
        elif data_type == "string":
            number_format, number_rate = self._detect_number_format(raw_samples)
            date_format, detected_date_type, date_rate = self._detect_date_profile(raw_samples)
            boolean_like = self._detect_boolean_like(raw_samples)

            if date_format and date_rate >= max(0.8, number_rate):
                data_type = detected_date_type
                detected_date_format = date_format
                date_parse_success_rate = date_rate
                date_expr = self._build_date_parse_expr(name, date_format)
                try:
                    min_value, max_value = connection.execute(
                        f"SELECT MIN({date_expr}), MAX({date_expr}) FROM dataset_view WHERE {sql_name} IS NOT NULL"
                    ).fetchone()
                except Exception:
                    min_value = None
                    max_value = None
            elif number_format and number_rate >= 0.85:
                data_type = "float"
                detected_number_format = number_format
                min_value, max_value, zero_ratio, decimal_ratio, numeric_parse_success_rate = self._compute_numeric_stats(
                    connection=connection,
                    source_column=name,
                    numeric_expr=self._build_numeric_parse_expr(name, number_format),
                    non_null_count=non_null_count,
                )

        uniqueness_ratio = (cardinality / non_null_count) if non_null_count else 0.0
        non_null_ratio = (non_null_count / row_count) if row_count else 0.0

        return ColumnProfile(
            name=name,
            type=data_type,
            nullable=null_count > 0,
            cardinality=cardinality,
            min_value=min_value,
            max_value=max_value,
            sample_values=sample_values,
            detected_date_format=detected_date_format,
            detected_number_format=detected_number_format,
            non_null_ratio=non_null_ratio,
            uniqueness_ratio=uniqueness_ratio,
            numeric_parse_success_rate=numeric_parse_success_rate,
            date_parse_success_rate=date_parse_success_rate,
            zero_ratio=zero_ratio,
            decimal_ratio=decimal_ratio,
            boolean_like=boolean_like,
        )

    def _assign_semantic_roles(self, *, columns: dict[str, ColumnProfile], row_count: int) -> None:
        for profile in columns.values():
            if profile.type in {"date", "datetime"}:
                profile.semantic_role = "time"
                continue

            if profile.type == "boolean" or profile.boolean_like:
                profile.semantic_role = "flag"
                continue

            if profile.type in {"integer", "float"}:
                if self._looks_like_identifier(profile=profile, row_count=row_count, numeric=True):
                    profile.semantic_role = "identifier"
                else:
                    profile.semantic_role = "measure"
                continue

            if profile.type == "string":
                if self._looks_like_identifier(profile=profile, row_count=row_count, numeric=False):
                    profile.semantic_role = "identifier"
                else:
                    profile.semantic_role = "category"
                continue

            profile.semantic_role = "unknown"

    def _looks_like_identifier(self, *, profile: ColumnProfile, row_count: int, numeric: bool) -> bool:
        if row_count <= 1:
            return False
        if profile.cardinality is None or profile.cardinality <= 1:
            return False
        cardinality_threshold = min(20, max(3, int(row_count * 0.75)))
        if numeric:
            return (
                profile.uniqueness_ratio >= 0.85
                and profile.decimal_ratio <= 0.05
                and profile.boolean_like is False
                and profile.cardinality >= cardinality_threshold
            )
        return profile.uniqueness_ratio >= 0.85 and profile.cardinality >= cardinality_threshold

    def _build_dimensions(
        self,
        *,
        columns: dict[str, ColumnProfile],
        date_columns: list[str],
    ) -> dict[str, DimensionDefinition]:
        dimensions: dict[str, DimensionDefinition] = {}
        for name, profile in columns.items():
            if profile.semantic_role == "category":
                dimensions[name] = DimensionDefinition(
                    name=name,
                    label=self._column_label(profile),
                    expression=quote_identifier(name),
                    kind="column",
                    source_column=name,
                )

        for date_column in date_columns:
            profile = columns[date_column]
            if profile.detected_date_format:
                date_expr = self._build_date_parse_expr(date_column, profile.detected_date_format)
            else:
                date_expr = quote_identifier(date_column)

            dimensions[date_column] = DimensionDefinition(
                name=date_column,
                label=self._column_label(profile),
                expression=date_expr,
                kind="column",
                source_column=date_column,
            )

            for granularity in ("day", "week", "month", "year"):
                dimension_name = f"{date_column}_{granularity}"
                dimensions[dimension_name] = DimensionDefinition(
                    name=dimension_name,
                    label=f"{self._column_label(profile)} por {_GRANULARITY_LABELS[granularity]}",
                    expression=f"DATE_TRUNC('{granularity}', {date_expr})",
                    kind="time_granularity",
                    source_column=date_column,
                    granularity=granularity,
                )

            dow_name = f"{date_column}_day_of_week"
            dimensions[dow_name] = DimensionDefinition(
                name=dow_name,
                label=f"{self._column_label(profile)} por {_GRANULARITY_LABELS['day_of_week']}",
                expression=self._build_day_of_week_label_expr(date_expr),
                order_expression=f"isodow({date_expr})",
                kind="time_granularity",
                source_column=date_column,
                granularity="day_of_week",
            )

        return dimensions

    def _build_metrics(self, columns: dict[str, ColumnProfile]) -> list[MetricDefinition]:
        metrics = [
            MetricDefinition(
                name="row_count",
                label="Conteo de Registros",
                formula="COUNT(*)",
                description="Numero total de registros",
                aggregator="count",
                source_column=None,
            )
        ]
        for name, profile in columns.items():
            if profile.semantic_role not in {"measure", "flag"} or profile.type not in {"integer", "float", "boolean"}:
                continue
            numeric_expr = quote_identifier(name)
            if profile.detected_number_format:
                numeric_expr = self._build_numeric_parse_expr(name, profile.detected_number_format)
            column_label = self._column_label(profile)
            metrics.extend(
                [
                    MetricDefinition(
                        name=f"{name}_sum",
                        label=self._build_sum_metric_label(column_label),
                        formula=f"SUM({numeric_expr})",
                        description=f"Suma total de {name}",
                        aggregator="sum",
                        source_column=name,
                    ),
                    MetricDefinition(
                        name=f"{name}_avg",
                        label=f"Promedio de {column_label}",
                        formula=f"AVG({numeric_expr})",
                        description=f"Promedio de {name}",
                        aggregator="avg",
                        source_column=name,
                    ),
                    MetricDefinition(
                        name=f"{name}_min",
                        label=f"Minimo de {column_label}",
                        formula=f"MIN({numeric_expr})",
                        description=f"Minimo de {name}",
                        aggregator="min",
                        source_column=name,
                    ),
                    MetricDefinition(
                        name=f"{name}_max",
                        label=f"Maximo de {column_label}",
                        formula=f"MAX({numeric_expr})",
                        description=f"Maximo de {name}",
                        aggregator="max",
                        source_column=name,
                    ),
                ]
            )
        return metrics

    def _build_aliases(
        self,
        *,
        columns: dict[str, ColumnProfile],
        dimension_definitions: dict[str, DimensionDefinition],
        metrics_allowed: list[MetricDefinition],
        default_date_column: str | None,
        manual_aliases: dict[str, str],
    ) -> dict[str, str]:
        aliases: dict[str, str] = {}

        for name, profile in columns.items():
            normalized = normalize_text(name)
            aliases[normalized] = name
            aliases[singularize(normalized)] = name
            aliases[pluralize(normalized)] = name
            label_normalized = normalize_text(self._column_label(profile))
            aliases[label_normalized] = name
            aliases[singularize(label_normalized)] = name
            aliases[pluralize(label_normalized)] = name

        for name, definition in dimension_definitions.items():
            normalized = normalize_text(name)
            aliases[normalized] = name
            aliases[normalize_text(definition.label)] = name
            if definition.granularity and definition.source_column:
                aliases[f"{normalize_text(definition.source_column)} {definition.granularity}"] = name
                source_profile = columns.get(definition.source_column)
                if source_profile is not None:
                    aliases[
                        f"{normalize_text(self._column_label(source_profile))} por {normalize_text(_GRANULARITY_LABELS[definition.granularity])}"
                    ] = name
                if definition.granularity == "day_of_week":
                    aliases["dia de la semana"] = name
                    aliases["dia semana"] = name
                    aliases["dias de la semana"] = name
                    aliases["dias semana"] = name
                    for weekday in _WEEKDAY_LABELS.values():
                        aliases[normalize_text(weekday)] = name

        if default_date_column:
            for granularity in ("day", "week", "month", "year", "day_of_week"):
                aliases[granularity] = f"{default_date_column}_{granularity}"
            aliases["dia de la semana"] = f"{default_date_column}_day_of_week"
            aliases["dia semana"] = f"{default_date_column}_day_of_week"

        for metric in metrics_allowed:
            aliases[normalize_text(metric.name)] = metric.name
            aliases[normalize_text(metric.label)] = metric.name
            if metric.source_column and metric.aggregator == "sum":
                aliases[normalize_text(metric.source_column)] = metric.name
                source_profile = columns.get(metric.source_column)
                if source_profile is not None:
                    aliases[normalize_text(self._column_label(source_profile))] = metric.name
            elif metric.name == "row_count":
                aliases["conteo"] = metric.name
                aliases["registros"] = metric.name
                aliases["count"] = metric.name

        for raw_alias, target in manual_aliases.items():
            aliases[normalize_text(raw_alias)] = target

        return aliases

    def _generate_column_labels(
        self,
        *,
        columns: dict[str, ColumnProfile],
        sample_rows: list[dict[str, Any]],
    ) -> dict[str, str]:
        if not self.gemini_client.configured:
            return {}

        payload = {
            "columns": [
                {
                    "name": name,
                    "type": profile.type,
                    "semantic_role": profile.semantic_role,
                    "sample_values": [str(value) if value is not None else None for value in profile.sample_values[:5]],
                }
                for name, profile in columns.items()
            ],
            "sample_rows": sample_rows[:3],
        }
        system_instruction = (
            "Eres un asistente que genera etiquetas legibles en espanol para columnas de un dataset CSV.\n"
            "Para cada nombre de columna, genera una etiqueta corta y natural que un usuario no tecnico entienda.\n\n"
            "REGLAS:\n"
            "- Separa palabras pegadas: TOTALSINDESCUENTO -> 'Total sin Descuento'.\n"
            "- Traduce abreviaciones comunes: qty -> cantidad, amt -> monto, desc -> descuento.\n"
            "- Respeta el idioma original si es claro; si el nombre esta en ingles, traduce al espanol.\n"
            "- Usa las filas de muestra como contexto para entender que representa cada columna.\n"
            "- Maximo 4 palabras por label.\n"
            "- No uses ALL CAPS ni snake_case en el label; usa formato titulo natural.\n"
            "- Si el nombre ya es legible, dejalo en formato titulo.\n"
        )

        try:
            response = self.gemini_client.generate_structured(
                system_instruction=system_instruction,
                prompt=json.dumps(payload, ensure_ascii=True, indent=2),
                response_model=ColumnLabelsResponse,
                model=self.settings.gemini_flash_model,
                temperature=0.1,
            )
            return {
                name: cleaned
                for name, label in response.labels.items()
                if name in columns and (cleaned := self._clean_label(label))
            }
        except Exception:
            return {}

    def _apply_column_labels(
        self,
        *,
        columns: dict[str, ColumnProfile],
        generated_labels: dict[str, str],
        label_overrides: dict[str, str],
    ) -> None:
        for name, profile in columns.items():
            fallback_label = self._clean_label(humanize_identifier(name)) or name
            profile.label = generated_labels.get(name, fallback_label)
            if name in label_overrides:
                profile.label = self._clean_label(label_overrides[name]) or profile.label

    def _clean_label(self, value: str | None) -> str:
        if value is None:
            return ""
        return re.sub(r"\s+", " ", str(value)).strip()

    def _column_label(self, profile: ColumnProfile) -> str:
        return self._clean_label(profile.label) or humanize_identifier(profile.name)

    def _build_sum_metric_label(self, column_label: str) -> str:
        normalized_label = normalize_text(column_label)
        if any(token in normalized_label.split() for token in ("total", "subtotal", "suma")):
            return column_label
        return f"Total de {column_label}"

    def _build_day_of_week_label_expr(self, date_expr: str) -> str:
        cases = " ".join(
            f"WHEN {index} THEN '{label}'"
            for index, label in _WEEKDAY_LABELS.items()
        )
        return f"CASE isodow({date_expr}) {cases} END"

    def _detect_number_format(self, samples: list[str]) -> tuple[str | None, float]:
        clean_samples = [self._normalize_numeric_sample(sample) for sample in samples]
        clean_samples = [sample for sample in clean_samples if sample is not None]
        if not clean_samples:
            return None, 0.0

        european_matches = sum(1 for value in clean_samples if _EUROPEAN_NUMBER_RE.match(value))
        standard_matches = sum(1 for value in clean_samples if _STANDARD_NUMBER_RE.match(value))
        european_signal = any("," in value for value in clean_samples)
        standard_signal = any("." in value for value in clean_samples if "," not in value)

        european_rate = european_matches / len(clean_samples)
        standard_rate = standard_matches / len(clean_samples)

        if european_signal and european_rate >= 0.85:
            return "european", european_rate
        if standard_signal and standard_rate >= 0.85:
            return "standard", standard_rate
        if max(european_rate, standard_rate) >= 0.95:
            return "standard", max(european_rate, standard_rate)
        return None, max(european_rate, standard_rate)

    def _normalize_numeric_sample(self, value: str) -> str | None:
        sample = value.strip().replace("$", "").replace("%", "")
        if sample.lower() in _NUMERIC_NULL_TOKENS:
            return None
        return sample

    def _detect_boolean_like(self, samples: list[str]) -> bool:
        clean = [normalize_text(sample) for sample in samples if sample and sample.strip()]
        if not clean:
            return False
        unique = set(clean)
        return unique.issubset(_BOOLEAN_TRUE | _BOOLEAN_FALSE) and len(unique) <= 3

    def _compute_numeric_stats(
        self,
        *,
        connection,
        source_column: str,
        numeric_expr: str,
        non_null_count: int,
    ) -> tuple[Any | None, Any | None, float, float, float]:
        sql_name = quote_identifier(source_column)
        try:
            parsed_count, min_value, max_value, zero_ratio, decimal_ratio = connection.execute(
                f"""
                SELECT
                    COUNT(value),
                    MIN(value),
                    MAX(value),
                    AVG(CASE WHEN value = 0 THEN 1.0 ELSE 0.0 END),
                    AVG(CASE WHEN value IS NOT NULL AND ABS(value - ROUND(value)) > 1e-9 THEN 1.0 ELSE 0.0 END)
                FROM (
                    SELECT {numeric_expr} AS value
                    FROM dataset_view
                    WHERE {sql_name} IS NOT NULL
                ) t
                """
            ).fetchone()
        except Exception:
            return None, None, 0.0, 0.0, 0.0

        parse_success_rate = (float(parsed_count) / float(non_null_count)) if non_null_count else 0.0
        return min_value, max_value, float(zero_ratio or 0.0), float(decimal_ratio or 0.0), parse_success_rate

    def _normalize_date_sample(self, value: str) -> str:
        normalized = re.sub(r"(\d{1,2}/\d{1,2}/\d{4}) (\d):(\d{2})", r"\1 0\2:\3", value)
        normalized = re.sub(r"(\d{4}-\d{2}-\d{2}) (\d):(\d{2})", r"\1 0\2:\3", normalized)
        return normalized

    def _detect_date_profile(self, samples: list[str]) -> tuple[str | None, str, float]:
        from datetime import datetime as dt

        clean_samples = [self._normalize_date_sample(str(sample).strip()) for sample in samples if str(sample).strip()]
        if not clean_samples:
            return None, "string", 0.0

        best_format = None
        best_rate = 0.0
        best_type = "string"

        for fmt in DATE_FORMATS:
            parsed = 0
            for value in clean_samples:
                try:
                    dt.strptime(value, fmt)
                    parsed += 1
                except ValueError:
                    continue
            rate = parsed / len(clean_samples)
            if rate > best_rate:
                best_rate = rate
                best_format = fmt
                best_type = "datetime" if "%H" in fmt else "date"

        if best_format is None or best_rate < 0.8:
            return None, "string", best_rate
        return best_format, best_type, best_rate

    def _build_numeric_parse_expr(self, column_name: str, number_format: str) -> str:
        sql_name = quote_identifier(column_name)
        trimmed = f"TRIM(CAST({sql_name} AS VARCHAR))"
        null_guard = f"UPPER({trimmed}) IN ('', 'NULL', 'NONE', 'N/A', 'NA', '-', '--')"

        if number_format == "european":
            normalized = f"REPLACE(REPLACE({trimmed}, '.', ''), ',', '.')"
        else:
            normalized = f"REPLACE({trimmed}, ',', '')"

        return (
            "CASE "
            f"WHEN {sql_name} IS NULL OR {null_guard} THEN NULL "
            f"ELSE TRY_CAST({normalized} AS DOUBLE) "
            "END"
        )

    def _format_to_strptime_sql(self, python_fmt: str) -> str:
        mapping = {
            "%Y": "%Y",
            "%m": "%m",
            "%d": "%d",
            "%H": "%H",
            "%M": "%M",
            "%S": "%S",
        }
        result = python_fmt
        for key, val in mapping.items():
            result = result.replace(key, val)
        return result

    def _build_date_parse_expr(self, column_name: str, python_fmt: str) -> str:
        sql_name = quote_identifier(column_name)
        fmt_sql = self._format_to_strptime_sql(python_fmt)
        if "%H" in python_fmt and "%M" in python_fmt:
            normalized = f"regexp_replace({sql_name}, ' (\\d):(\\d{{2}})', ' 0\\1:\\2')"
            return f"STRPTIME({normalized}, '{fmt_sql}')"
        return f"STRPTIME({sql_name}, '{fmt_sql}')"

    def _collect_raw_samples(self, connection, column_name: str, n: int = _MAX_PROFILE_SAMPLES) -> list[str]:
        sql_name = quote_identifier(column_name)
        try:
            rows = connection.execute(
                f"SELECT {sql_name} FROM dataset_view WHERE {sql_name} IS NOT NULL LIMIT {n}"
            ).fetchall()
        except Exception:
            return []
        return [str(row[0]) for row in rows if row[0] is not None]

    def _collect_sample_values(self, connection, column_name: str, n: int = 10) -> list[Any]:
        sql_name = quote_identifier(column_name)
        try:
            rows = connection.execute(
                f"SELECT DISTINCT {sql_name} FROM dataset_view WHERE {sql_name} IS NOT NULL LIMIT {n}"
            ).fetchall()
        except Exception:
            return []
        return [row[0] for row in rows]

    def _collect_sample_rows(self, connection, column_names: list[str], n: int = 5) -> list[dict[str, Any]]:
        try:
            select = ", ".join(quote_identifier(col) for col in column_names)
            rows = connection.execute(f"SELECT {select} FROM dataset_view LIMIT {n}").fetchall()
        except Exception:
            return []

        result: list[dict[str, Any]] = []
        for row in rows:
            result.append({column_names[i]: (str(v) if v is not None else None) for i, v in enumerate(row)})
        return result

    def _select_default_date_column(self, *, date_columns: list[str], columns: dict[str, ColumnProfile]) -> str | None:
        if not date_columns:
            return None
        ranked = sorted(
            date_columns,
            key=lambda name: (
                columns[name].non_null_ratio,
                columns[name].date_parse_success_rate,
                self._date_span_score(columns[name]),
                -date_columns.index(name),
            ),
            reverse=True,
        )
        return ranked[0]

    def _date_span_score(self, profile: ColumnProfile) -> int:
        if not profile.min_value or not profile.max_value:
            return 0
        try:
            start = datetime.fromisoformat(str(profile.min_value).replace("Z", "+00:00"))
            end = datetime.fromisoformat(str(profile.max_value).replace("Z", "+00:00"))
        except ValueError:
            return 0
        return max(int((end - start).days), 0)

    def _select_default_metric(
        self,
        *,
        columns: dict[str, ColumnProfile],
        metrics_allowed: list[MetricDefinition],
    ) -> str | None:
        suggested = self._suggest_metrics(columns=columns, metrics_allowed=metrics_allowed, limit=1)
        return suggested[0] if suggested else None

    def _suggest_metrics(
        self,
        *,
        columns: dict[str, ColumnProfile],
        metrics_allowed: list[MetricDefinition],
        limit: int,
    ) -> list[str]:
        candidates = [
            metric
            for metric in metrics_allowed
            if metric.aggregator == "sum"
            and metric.source_column
            and columns[metric.source_column].semantic_role == "measure"
        ]
        ranked = sorted(
            candidates,
            key=lambda metric: self._metric_priority(columns[metric.source_column]),
            reverse=True,
        )
        names = [metric.name for metric in ranked[:limit]]
        if not names and "row_count" in {metric.name for metric in metrics_allowed}:
            names.append("row_count")
        return names

    def _metric_priority(self, profile: ColumnProfile) -> tuple[float, float, float, float]:
        return (
            profile.non_null_ratio,
            1.0 - profile.zero_ratio,
            profile.decimal_ratio,
            1.0 - min(profile.uniqueness_ratio, 1.0),
        )

    def _suggest_dimensions(
        self,
        *,
        columns: dict[str, ColumnProfile],
        dimension_definitions: dict[str, DimensionDefinition],
        date_columns: list[str],
        limit: int,
    ) -> list[str]:
        category_dimensions = [
            name
            for name, definition in dimension_definitions.items()
            if definition.kind == "column" and definition.source_column not in date_columns
        ]
        ranked = sorted(
            category_dimensions,
            key=lambda name: self._dimension_priority(columns[dimension_definitions[name].source_column or name]),
            reverse=True,
        )
        return ranked[:limit]

    def _dimension_priority(self, profile: ColumnProfile) -> tuple[float, float, int]:
        cardinality = profile.cardinality or 0
        within_preferred_range = 1 if 2 <= cardinality <= 25 else 0
        return (
            profile.non_null_ratio,
            float(within_preferred_range),
            -abs(cardinality - 8),
        )

    def _generate_dataset_id(self, source_name: str) -> str:
        return f"{slugify(source_name)}-{uuid.uuid4().hex[:8]}"

    def _map_type(self, duckdb_type: str) -> str:
        normalized = duckdb_type.strip().lower()
        root = normalized.split("(")[0].strip()
        if root in NUMERIC_TYPES:
            return "float" if root in {"float", "double", "decimal", "real"} else "integer"
        if root in DATE_TYPES:
            return "date"
        if root in DATETIME_TYPES:
            return "datetime"
        if root in BOOLEAN_TYPES:
            return "boolean"
        if root in STRING_TYPES:
            return "string"
        return "unknown"
