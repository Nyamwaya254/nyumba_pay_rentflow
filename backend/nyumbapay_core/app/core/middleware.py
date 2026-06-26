"""Middleware stack - Nyumbapay Core

Rate limiting uses slowapi (token bucket via Redis).
Auth endpoints: 5 req/min per IP.
General API: 300 req/min per user/IP.

Headers applied:
  Strict-Transport-Security  — force HTTPS for 1 year
  X-Content-Type-Options     — prevent MIME sniffing
  X-Frame-Options            — prevent clickjacking
  Referrer-Policy            — limit referer leakage
  Permissions-Policy         — deny unnecessary browser APIs
  X-XSS-Protection           - tells older browsers to block detected XSS attacks
  Cache-Control              — prevent caching of API responses
"""

import time
from typing import Callable, cast
import uuid
from fastapi import FastAPI, Request, Response
from starlette.types import ExceptionHandler
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from starlette.middleware.base import BaseHTTPMiddleware
import structlog

from app.core.config import get_settings
from app.core.exceptions import AppError


logger = structlog.get_logger(__name__)


def _rate_limit_key(request: Request) -> str:
    """Rate limit key selector
    Authenticated requests are keyed by user ID — ensuring limits are
    per-user regardless of which IP they connect from (mobile clients
    change IP frequently). Unauthenticated requests fall back to IP.
    """
    user_id = getattr(request.state, "user_id", None)
    if user_id is not None:  # Explicitly checks for "user is logged in"
        return f"user:{user_id}"
    if request.client:
        return f"ip:{request.client.host}"
    return "ip:unknown"


# Rate Limiter
# Uses user_id and falls toclient IP as the rate limit key (X‑Forwarded‑For respects reverse proxies)
limiter = Limiter(
    key_func=_rate_limit_key,
    default_limits=["300/minute"],
    storage_uri=get_settings().redis_url_str,  # set at construction time
)


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    """Middleware that injects security HTTP headers into every response"""

    SECURITY_HEADERS = {
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
        "X-XSS-Protection": "1; mode=block",
        "Strict-Transport-Security": "max-age=63072000; includeSubDomains; preload",
        "Referrer-Policy": "no-referrer",
        "Permissions-Policy": "geolocation=(), microphone=(), camera=()",
        "Cache-Control": "no-store",
    }

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Process the request and add security headers to the response"""
        response = await call_next(request)
        for header, value in self.SECURITY_HEADERS.items():
            response.headers[header] = value
        return response


class RequestContextMiddleware(BaseHTTPMiddleware):
    """Middleware that runs on every request and sets up the logging context
    Responsibilities:
        - Generate or forward a request ID (X‑Request-ID header).
        - Bind request metadata (method, path, client IP) to structlog context.
        - Measure request duration.
        - Log a structured event after the request completes.
        - Attach the request ID to the response headers.
    """

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Execute the request pipeline with context and logging"""
        request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
        start = time.perf_counter()

        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(
            request_id=request_id,
            method=request.method,
            path=request.url.path,
            client_ip=self._client_ip(request),
        )

        response = await call_next(request)

        logger.info(
            "request_completed",
            status_code=response.status_code,
            duration_ms=round((time.perf_counter() - start) * 1000, 2),
        )
        response.headers["X-Request-ID"] = request_id
        return response

    @staticmethod
    def _client_ip(request: Request) -> str:
        """Extract the real client IP, respecting the X‑Forwarded‑For header"""
        fwd = request.headers.get("X-Forwaded-For")
        if fwd:
            return fwd.split(",")[0].strip()
        return request.client.host if request.client else "unknown"


def register_exception_handlers(app: FastAPI) -> None:
    """Register global exception handlers for AppError and any unhandled exceptions"""

    @app.exception_handler(AppError)
    async def app_error_handler(request: Request, exc: AppError) -> JSONResponse:
        """Convert our domain AppError into RFC 7807 Problem Details response"""
        logger.warning(
            "app_error",
            error_code=exc.error_code,
            message=exc.message,
            status_code=exc.status_code,
        )
        return JSONResponse(
            status_code=exc.status_code,
            content=exc.to_problem_detail(),
            headers={"Content-Type": "application/problem+json"},
        )

    @app.exception_handler(Exception)
    async def unhandled_handler(request: Request, exc: Exception) -> JSONResponse:
        """Catch any unexpected exception, log it, and return a safe 500"""
        logger.exception("unhandled_exception", exc_info=exc)
        return JSONResponse(
            status_code=500,
            content={
                "type": "https://nyumbapay.co.ke/errors/internal_error",
                "title": "Internal Server Error",
                "status": 500,
                "detail": "An unexpected error occurred.",
            },
            headers={"Content-Type": "application/problem+json"},
        )


def setup_middleware(app: FastAPI, redis_url: str) -> None:
    """Register all middleware and exception handlers in the correct order.
    redis_url wires rate limiter to redis
    """
    limiter._storage_uri = redis_url
    app.state.limiter = limiter
    app.add_exception_handler(
        RateLimitExceeded, cast(ExceptionHandler, _rate_limit_exceeded_handler)
    )
    app.add_middleware(RequestContextMiddleware)
    app.add_middleware(SecurityHeadersMiddleware)
    register_exception_handlers(app)
