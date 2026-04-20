"""Per-domain rate limiting: 1 request per 2 seconds, domains run in parallel."""
from __future__ import annotations

import asyncio
import time
from collections import defaultdict

MIN_INTERVAL_SECONDS = 2.0


class DomainRateLimiter:
    def __init__(self, min_interval: float = MIN_INTERVAL_SECONDS) -> None:
        self._min_interval = min_interval
        self._locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._last_call: dict[str, float] = {}

    async def acquire(self, domain: str) -> None:
        async with self._locks[domain]:
            last = self._last_call.get(domain)
            if last is not None:
                wait = self._min_interval - (time.monotonic() - last)
                if wait > 0:
                    await asyncio.sleep(wait)
            self._last_call[domain] = time.monotonic()


GLOBAL_LIMITER = DomainRateLimiter()
