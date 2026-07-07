"""Idempotency key management via Redis
   Protocol:
    -Client generates a UUID before sending the request
    -Client includes it as : idempotency-key:<uuid>
    -Server checks Redis for a cached response under that key
    -Cache hit -> return cached response immediately, no processing
    -Cache miss -> process,store result in Redid, return response

Key TTL is 24 hours — long enough to cover any reasonable retry
window, short enough to avoid unbounded Redis growth.
"""

from decimal import Decimal
import json
from typing import Any

import redis.asyncio as aioredis
import structlog


logger = structlog.get_logger(__name__)

_IDEMPOTENCY_TTL_SECONDS = 86_400  # 24 HOURS
_KEY_PREFIX = "idempotency:"


class DecimalEncoder(json.JSONEncoder):
    """JSON encoder that handles Decimal field from pydantic models"""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, Decimal):
            return str(obj)
        return super().default(obj)


class IdempotencyService:
    """Redis-backed idempotency key store"""

    def __init__(self, redis: aioredis.Redis) -> None:
        self._redis = redis

    def _key(self, idempotency_key: str) -> str:
        return f"{_KEY_PREFIX}{idempotency_key}"

    async def get_cached_response(self, idempotency_key: str) -> dict[str, Any] | None:
        """Check whether this key was already processed"""
        raw = await self._redis.get(self._key(idempotency_key))
        if raw is None:
            return None
        logger.info(
            "idempotency_cache_hit",
            idempotency_key=idempotency_key,
        )
        return json.loads(raw)

    async def store_response(
        self,
        idempotency_key: str,
        response_data: dict[str, Any],
    ) -> None:
        """Cache the response for this key

        Uses SET NX (set if not exists) so a race condition between two
        concurrent requests with the same key cannot overwrite a result
        that is already being stored.
        """

        await self._redis.set(
            self._key(idempotency_key),
            json.dumps(response_data, cls=DecimalEncoder),
            ex=_IDEMPOTENCY_TTL_SECONDS,
            nx=True,  # only set if key does not exist -prevent overwrites
        )
        logger.info(
            "idempotency_response_cached",
            idempotency_key=idempotency_key,
            ttl=_IDEMPOTENCY_TTL_SECONDS,
        )
