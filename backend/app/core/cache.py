from __future__ import annotations

import threading
import time
from typing import Any


class TTLCache:
    def __init__(self, default_ttl_seconds: int) -> None:
        self.default_ttl_seconds = default_ttl_seconds
        self._values: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()

    def _purge(self) -> None:
        now = time.time()
        expired = [key for key, (expires_at, _) in self._values.items() if expires_at <= now]
        for key in expired:
            self._values.pop(key, None)

    def get(self, key: str) -> Any | None:
        with self._lock:
            self._purge()
            item = self._values.get(key)
            if not item:
                return None
            expires_at, value = item
            if expires_at <= time.time():
                self._values.pop(key, None)
                return None
            return value

    def set(self, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        ttl = ttl_seconds if ttl_seconds is not None else self.default_ttl_seconds
        with self._lock:
            self._values[key] = (time.time() + ttl, value)

    def clear(self) -> None:
        with self._lock:
            self._values.clear()
