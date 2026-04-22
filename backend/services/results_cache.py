"""In-memory TTL cache for compute results.

Results live for 1 hour; the Results page re-fetches on demand for
CSV / GeoJSON downloads. On cache miss, the caller should 410 Gone.
"""
from __future__ import annotations

from typing import Any

from cachetools import TTLCache

_cache: TTLCache = TTLCache(maxsize=32, ttl=3600)


class ResultNotFound(Exception):
    """Raised when a result UUID is not in the cache."""


def save_result(result_id: str, response: Any) -> None:
    """Store a response. Accepts any object with ``model_dump()``."""
    _cache[result_id] = response.model_dump()


def load_result(result_id: str) -> dict:
    """Retrieve a previously-saved result. Raises ResultNotFound if missing/expired."""
    if result_id not in _cache:
        raise ResultNotFound(result_id)
    return _cache[result_id]
