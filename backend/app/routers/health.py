from __future__ import annotations

from fastapi import APIRouter, Request


router = APIRouter(tags=["health"])


@router.get("/health")
async def health(request: Request) -> dict[str, object]:
    db_ok = request.app.state.db_manager.ping()
    return {
        "status": "ok",
        "duckdb": "ok" if db_ok else "error",
        "gemini_configured": request.app.state.gemini_client.configured,
    }
