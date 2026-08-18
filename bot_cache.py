from __future__ import annotations

import json
import time
from typing import Any

from bot_urls import normalize_url


class MemoryCache:
    """Small async cache used for local operation and deterministic tests."""

    _max_entries = 4096

    def __init__(self) -> None:
        self._values: dict[str, tuple[Any, float]] = {}

    async def get(self, key: str) -> Any | None:
        item = self._values.get(key)
        if item is None:
            return None
        value, expires_at = item
        if expires_at <= time.monotonic():
            self._values.pop(key, None)
            return None
        return value

    async def set(self, key: str, value: Any, ttl: int) -> None:
        self._values[key] = (value, time.monotonic() + max(0, int(ttl)))
        if len(self._values) <= self._max_entries:
            return
        now = time.monotonic()
        expired = [
            cache_key
            for cache_key, (_, expires_at) in self._values.items()
            if expires_at <= now
        ]
        for cache_key in expired:
            self._values.pop(cache_key, None)
        while len(self._values) > self._max_entries:
            oldest = min(self._values, key=lambda cache_key: self._values[cache_key][1])
            self._values.pop(oldest, None)

    async def delete(self, key: str) -> None:
        self._values.pop(key, None)

    async def close(self) -> None:
        return None


class RedisCache:
    """Redis-backed cache storing Telegram file IDs and download metadata."""

    def __init__(self, client: Any) -> None:
        self.client = client

    async def get(self, key: str) -> Any | None:
        value = await self.client.get(key)
        if value is None:
            return None
        if isinstance(value, bytes):
            value = value.decode()
        try:
            return json.loads(value)
        except (TypeError, ValueError, UnicodeDecodeError):
            # A partially written or manually edited Redis value must behave
            # like a cache miss, not break the user's download workflow.
            try:
                await self.client.delete(key)
            except Exception:
                pass
            return None

    async def set(self, key: str, value: Any, ttl: int) -> None:
        await self.client.set(key, json.dumps(value), ex=ttl)

    async def delete(self, key: str) -> None:
        await self.client.delete(key)

    async def close(self) -> None:
        await self.client.aclose()


async def create_cache(settings: Any) -> MemoryCache | RedisCache:
    if not settings.enable_redis:
        return MemoryCache()
    client = None
    try:
        from redis.asyncio import Redis

        client = Redis.from_url(settings.redis_url, decode_responses=False)
        await client.ping()
        return RedisCache(client)
    except Exception:
        if client is not None:
            try:
                await client.aclose()
            except Exception:
                pass
        # Redis is an optional operational dependency. The bot remains usable
        # for a single process, while production deployments can set REDIS_URL.
        return MemoryCache()


def cache_key(url: str, audio_only: bool = False, quality: str = "auto") -> str:
    import hashlib
    from bot_quality import normalize_quality

    mode = "audio" if audio_only else "video"
    normalized_quality = normalize_quality(quality, audio_only)
    digest = hashlib.sha256(
        f"{mode}:{normalized_quality}:{normalize_url(url)}".encode()
    ).hexdigest()
    return f"ytdlbot:file:{digest}"