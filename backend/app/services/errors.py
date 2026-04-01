from __future__ import annotations

from typing import Any


class PlanValidationError(ValueError):
    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class ClarificationNeeded(RuntimeError):
    def __init__(
        self,
        *,
        question: str,
        reason: str,
        hints: list[str] | None = None,
        meta: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(reason)
        self.question = question
        self.reason = reason
        self.hints = hints or []
        self.meta = meta or {}
