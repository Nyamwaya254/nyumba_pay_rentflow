"""Route to check liveness of the process"""

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from sqlalchemy import text
import structlog

logger = structlog.get_logger(__name__)
health_router = APIRouter(tags=["health"])


@health_router.get("/health", include_in_schema=False)
async def liveness():
    """Liveness probe called by K8/Railway to determine if process is alive"""
    return {"status": "ok", "service": "nyumbapay-core"}


@health_router.get("/ready", include_in_schema=False)
async def readiness(request: Request):
    checks: dict[str, str] = {}
    healthy = True

    try:
        async with request.app.state.db_engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        checks["postgres"] = "ok"
    except Exception as exc:
        checks["postgres"] = "degraded"
        logger.error("readiness_postgres_failed", error=str(exc))
        healthy = False

    try:
        await request.app.state.redis.ping()
        checks["redis"] = "ok"
    except Exception as exc:
        checks["redis"] = "degraded"
        logger.error("readiness_redis_failed", error=str(exc))
        healthy = False

    return JSONResponse(
        status_code=200 if healthy else 503,
        content={"status": "ready" if healthy else "unavailable", "checks": checks},
    )
