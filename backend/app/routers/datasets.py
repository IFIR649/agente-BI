from __future__ import annotations

import json

from fastapi import APIRouter, File, Form, HTTPException, Request, UploadFile, status
from pydantic import BaseModel, Field

from backend.app.core.auth import get_auth_context
from backend.app.models.dataset import DatasetSummary, UploadMetadata


router = APIRouter(prefix="/datasets", tags=["datasets"])


class LabelUpdate(BaseModel):
    column_labels: dict[str, str] = Field(default_factory=dict)


class ActiveDatasetUpdate(BaseModel):
    dataset_id: str


@router.get("", response_model=list[DatasetSummary])
async def list_datasets(request: Request) -> list[DatasetSummary]:
    return request.app.state.dataset_profiler.list_catalogs()


@router.get("/active", response_model=DatasetSummary)
async def get_active_dataset(request: Request) -> DatasetSummary:
    user_id = get_auth_context(request).actor_user_id
    dataset_id = request.app.state.active_dataset_store.get_active_dataset_id(user_id)
    if not dataset_id:
        raise HTTPException(status_code=404, detail="No hay dataset activo para este usuario.")

    catalog = request.app.state.dataset_profiler.get_catalog(dataset_id)
    if catalog is None:
        request.app.state.active_dataset_store.clear_active_dataset(user_id)
        raise HTTPException(status_code=404, detail="El dataset activo ya no esta disponible.")

    return catalog.to_summary()


@router.put("/active", response_model=DatasetSummary)
async def set_active_dataset(
    request: Request,
    body: ActiveDatasetUpdate,
) -> DatasetSummary:
    user_id = get_auth_context(request).actor_user_id
    catalog = request.app.state.dataset_profiler.get_catalog(body.dataset_id)
    if catalog is None:
        raise HTTPException(status_code=404, detail="Dataset no encontrado.")

    request.app.state.active_dataset_store.set_active_dataset(user_id, catalog.id)
    return catalog.to_summary()


@router.post("/upload", response_model=DatasetSummary, status_code=status.HTTP_201_CREATED)
async def upload_dataset(
    request: Request,
    file: UploadFile = File(...),
    metadata: str | None = Form(default=None),
) -> DatasetSummary:
    settings = request.app.state.settings
    filename = file.filename or "dataset.csv"
    if not filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Solo se aceptan archivos CSV.")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="El archivo esta vacio.")

    max_size_bytes = settings.max_upload_size_mb * 1024 * 1024
    if len(content) > max_size_bytes:
        raise HTTPException(status_code=413, detail=f"El archivo supera el limite de {settings.max_upload_size_mb} MB.")

    parsed_metadata = UploadMetadata()
    if metadata:
        try:
            parsed_metadata = UploadMetadata.model_validate(json.loads(metadata))
        except Exception as exc:
            raise HTTPException(status_code=422, detail="metadata no es un JSON valido.") from exc

    try:
        catalog = request.app.state.dataset_profiler.profile_and_store(
            filename=filename,
            content=content,
            metadata=parsed_metadata,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"No se pudo procesar el CSV: {exc}") from exc

    user_id = get_auth_context(request).actor_user_id
    request.app.state.active_dataset_store.set_active_dataset(user_id, catalog.id)
    return catalog.to_summary()


@router.patch("/{dataset_id}/labels", response_model=DatasetSummary)
async def update_labels(request: Request, dataset_id: str, body: LabelUpdate) -> DatasetSummary:
    if not body.column_labels:
        raise HTTPException(status_code=422, detail="column_labels no puede estar vacio.")

    try:
        catalog = request.app.state.dataset_profiler.update_column_labels(dataset_id, body.column_labels)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    if catalog is None:
        raise HTTPException(status_code=404, detail="Dataset no encontrado.")

    return catalog.to_summary()
