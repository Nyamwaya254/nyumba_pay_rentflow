from __future__ import annotations
from typing import AsyncGenerator, cast

import structlog
from redis.asyncio import Redis
from redis.asyncio.connection import ConnectionPool
from redis.exceptions import ConnectionError, TimeoutError
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    async_sessionmaker,
    AsyncSession,
    create_async_engine,
)

from nyumbapay_core.core.config import Settings

logger = structlog.get_logger(__name__)


_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None
_redis_pool: Redis | None = None


def create_engine(settings: Settings, poolclass=None) -> AsyncEngine:
    """Create and configure an SQLAlchemy async database engine"""
    kwargs: dict = dict(
        echo=not settings.is_production,
        pool_size=settings.database_pool_size,
        max_overflow=settings.database_max_overflow,
        pool_timeout=settings.database_pool_timeout,
        pool_pre_ping=True,
        pool_recycle=3600,
        connect_args={"server_settings": {"application_name": "nyumbapay-core"}},
    )
    if poolclass is not None:
        for k in ("pool_size", "max_overflow", "pool_timeout"):
            kwargs.pop(k, None)
        kwargs["poolclass"] = poolclass
    return create_async_engine(settings.database_url_str, **kwargs)


async def init_db(settings: Settings) -> None:
    """Create the engine and sesion factory bound to the engine"""
    global _engine, _session_factory
    _engine = create_engine(settings)
    _session_factory = async_sessionmaker(
        _engine, expire_on_commit=False, autoflush=False, autocommit=False
    )
    logger.info("Database_pool_initialised")


async def close_db() -> None:
    """Close all connections in the pool gracefully"""
    global _engine
    if _engine:
        await _engine.dispose()
        logger.info("database_pool_closed")


async def init_redis(settings: Settings) -> None:
    """Initialise the async Redis client with connection pooling"""
    global _redis_pool
    client: Redis = Redis.from_url(
        settings.redis_url_str,
        max_connections=settings.redis_pool_size,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
        retry_on_timeout=True,
        health_check_interval=30,
    )

    try:
        response = cast(bool, await client.ping())  # type: ignore[redundant-cast]
    except (ConnectionError, TimeoutError) as exc:
        await client.aclose()
        raise RuntimeError(f"Redis unreachable at startup: {exc}") from exc

    if response is not True:
        await client.aclose()
        raise RuntimeError(f"Redis PING returned unexpected response: {response!r}")

    _redis_pool = client
    logger.info("redis_pool_initialised", max_connections=settings.redis_pool_size)


async def close_redis() -> None:
    """Gracefully close all Redis connections"""
    global _redis_pool
    if _redis_pool is not None:
        await _redis_pool.aclose()
        _redis_pool = None
        logger.info("redis_pool_closed")


async def get_redis() -> AsyncGenerator[Redis, None]:
    if _redis_pool is None:
        raise RuntimeError("Redis pool not initialised")
    yield _redis_pool


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    if _session_factory is None:
        raise RuntimeError("Database not initialised")
    async with _session_factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
