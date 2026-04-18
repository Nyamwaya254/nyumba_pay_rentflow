"""Application configuration - Rentflow core"""

from __future__ import annotations
from functools import lru_cache
from typing import Literal
from pydantic import Field, PostgresDsn, RedisDsn, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file="backend/.env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # application
    app_env: Literal["production", "sandbox", "development"] = "development"
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    app_log_level: str = "INFO"

    # database
    database_url: PostgresDsn
    database_pool_size: int = 20
    database_max_overflow: int = 10
    database_pool_timeout: int = 30

    # Redis
    redis_url: RedisDsn
    redis_pool_size: int = 20

    # clerk
    clerk_secret_key: str = Field(min_length=10)
    clerk_publishable_key: str = Field(min_length=10)
    clerk_webhook_secret: str = Field(min_length=10)
    clerk_jwks_url: str

    # payment servuce
    payment_service_url: str
    payment_service_api_key: str = Field(min_length=32)
    payment_events_channel: str = ""

    # celery
    celery_broker_url: str = ""
    celery_result_backend: str = ""

    # observability
    sentry_dsn: str = ""
    otel_exporter_otlp_endpoint: str = ""

    @model_validator(mode="after")
    def validate_production(self) -> "Settings":
        if self.app_env == "production" and not self.sentry_dsn:
            raise ValueError("SENTRY_DSN required in production")
        if not self.celery_broker_url:
            self.celery_broker_url = str(self.redis_url)
        if not self.celery_result_backend:
            self.celery_result_backend = str(self.redis_url)
        return self

    @property
    def is_production(self) -> bool:
        return self.app_env == "production"

    @property
    def database_url_str(self) -> str:
        return str(self.database_url)

    @property
    def redis_url_str(self) -> str:
        return str(self.redis_url)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the singleton settings instance"""
    return Settings()  # type: ignore[call-arg]
