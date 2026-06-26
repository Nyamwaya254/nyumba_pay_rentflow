"""FastAPI application factory- Nyumbapay core"""

from contextlib import asynccontextmanager
from typing import AsyncGenerator
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
import sentry_sdk
import structlog

from app.core.config import get_settings
from app.core.database import close_db, close_redis, init_db, init_redis
from app.core.logging import configure_logging
from app.core.middleware import setup_middleware

from app.services.payment_client import PaymentServiceClient
from app.services.clerk_service import ClerkService
from nyumbapay_core.app.services.notification_service import NotificationService


logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    settings = get_settings()
    configure_logging(
        log_level=settings.app_log_level,
        json_logs=settings.is_production,
    )
    logger.info("nyumbapay_core_starting", env=settings.app_env)

    if settings.sentry_dsn:
        sentry_sdk.init(
            dsn=settings.sentry_dsn,
            environment=settings.app_env,
            traces_sample_rate=0.1,
        )

    # long lived HTTP clients -created once,shared across requests
    app.state.payment_client = PaymentServiceClient(settings=settings)
    app.state.clerk_service = ClerkService(settings=settings)
    app.state.notification_service = NotificationService(settings=settings)

    try:
        await init_db(settings)
        await init_redis(settings)
    except Exception:
        logger.error("nyumbapay_core_startup_failed", exc_info=True)
        await app.state.payment_client.aclose()
        await app.state.clerk_service.aclose()
        await app.state.notification_service.aclose()
        await close_db()
        await close_redis()
        raise

    logger.info("nyumbapay_core_ready")
    yield

    # graceful shutdown
    logger.info("nyumbapay_core_shutting_down")
    await app.state.payment_client.aclose()
    await app.state.clerk_service.aclose()
    await close_db()
    await close_redis()
    logger.info("nyumbapay_core_stopped")


def create_app() -> FastAPI:
    settings = get_settings()

    app = FastAPI(
        title="Nyumbapay core API",
        description="Multi-landlord rent management — core service.",
        version="2.0.0",
        lifespan=lifespan,
        docs_url=None if settings.is_production else "/docs",
        redoc_url=None if settings.is_production else "/redoc",
        openapi_url=None if settings.is_production else "/openapi.json",
    )

    setup_middleware(app, redis_url=settings.redis_url_str)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["https://app.nyumbapay.co.ke"],
        allow_credentials=True,
        allow_methods=["GET", "POST", "PATCH", "PUT", "DELETE"],
        allow_headers=["Authorization", "Content-Type", "X-Request-ID"],
    )

    return app


app = create_app()


@app.get("/")
async def root():
    return {
        "name": "NyumbaPay",
        "mission": "Simplifying rent collection across Africa.",
        "features": [
            "M-Pesa Rent Payments",
            "Automated Payment Verification",
            "Tenant Management",
            "Real-Time Reconciliation",
            "Financial Reporting",
        ],
        "status": "operational",
        "api_version": "v1",
    }


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        media_type="application/problem+json",
        content={
            "type": f"https://docs.nyumbapay.co.ke/errors/http-{exc.status_code}",
            "title": exc.detail,
            "status": exc.status_code,
            "instance": request.url.path,
        },
    )
