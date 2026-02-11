from __future__ import annotations

import os
import time
from typing import Any, Callable

from cachetools import TTLCache


def make_ttl_cache() -> TTLCache:
    ttl = int(os.getenv("CACHE_TTL_SECONDS", "300"))
    return TTLCache(maxsize=2048, ttl=ttl)


def cache_get_or_set(cache: TTLCache, key: str, fn: Callable[[], Any]) -> Any:
    hit = cache.get(key)
    if hit is not None:
        return hit
    val = fn()
    cache[key] = val
    return val


def now_ms() -> int:
    return int(time.time() * 1000)
