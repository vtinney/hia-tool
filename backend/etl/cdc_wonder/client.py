"""Thin HTTP client for the CDC Wonder XML API.

Handles POSTing XML, rate-limiting, exponential-backoff retry on 429/5xx,
and on-disk caching so re-runs skip already-fetched combinations.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path

import requests

from backend.etl.cdc_wonder.constants import (
    CDC_WONDER_URL,
    MAX_RETRIES,
    REQUEST_DELAY_SECONDS,
)

logger = logging.getLogger("cdc_wonder.client")


class CdcWonderClient:
    """POST-and-cache client for the CDC Wonder XML API."""

    def __init__(
        self,
        cache_root: Path,
        request_delay: float = REQUEST_DELAY_SECONDS,
        max_retries: int = MAX_RETRIES,
    ) -> None:
        self.cache_root = Path(cache_root)
        self.request_delay = request_delay
        self.max_retries = max_retries

    def _cache_path(
        self, database: str, year: int, icd_group: str, age_bucket: str
    ) -> Path:
        return (
            self.cache_root / database / str(year) / f"{icd_group}_{age_bucket}.tsv"
        )

    def fetch(
        self,
        *,
        database: str,
        year: int,
        icd_group: str,
        age_bucket: str,
        xml_body: str,
    ) -> str:
        """Fetch a CDC Wonder query, returning the raw TSV body."""
        cached = self._cache_path(database, year, icd_group, age_bucket)
        if cached.exists():
            logger.debug("cache hit: %s", cached)
            return cached.read_text()

        url = CDC_WONDER_URL.format(db=database)
        headers = {"Content-Type": "application/xml"}
        delay = self.request_delay

        last_error: str | None = None
        for attempt in range(1, self.max_retries + 1):
            if attempt > 1:
                backoff = delay * (2 ** (attempt - 1))
                logger.warning(
                    "retry %d/%d after %.1fs (reason: %s)",
                    attempt, self.max_retries, backoff, last_error,
                )
                time.sleep(backoff)
            else:
                time.sleep(delay)

            resp = requests.post(url, data=xml_body, headers=headers, timeout=120)
            if resp.ok:
                cached.parent.mkdir(parents=True, exist_ok=True)
                cached.write_text(resp.text)
                return resp.text
            last_error = f"HTTP {resp.status_code}"
            if resp.status_code not in (429, 500, 502, 503, 504):
                break

        raise RuntimeError(
            f"CDC Wonder request failed after {self.max_retries} attempts: "
            f"{last_error} (db={database} year={year} "
            f"icd={icd_group} age={age_bucket})"
        )
