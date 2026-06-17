"""HTTP client for payment service internal API
Called by RentFlow core during landlord onboarding to register
the landlord's Paybill with Daraja via the Payment Service.

Uses tenacity for retry with exponential backoff.
"""

from typing import NoReturn
import httpx
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from app.core.config import Settings
from app.core.exceptions import (
    PaymentServiceClientError,
    PaymentServiceError,
)


logger = structlog.get_logger(__name__)


class PaymentServiceClient:
    """Internal HTTP client for the Payment Service"""

    def __init__(self, settings: Settings) -> None:
        self._base_url = settings.payment_service_url.rstrip("/")
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            headers={
                "Authorization": f"Bearer {settings.payment_service_api_key}",
                "Content-Type": "application/json",
            },
            timeout=httpx.Timeout(
                connect=5.0,  # TCP connection timeout
                read=30.0,  # wait for response body
                write=10.0,  # sending request body
                pool=5.0,  # waiting for a connection from the pool
            ),
        )

    async def close(self) -> None:
        """Call on app shutdown to release connections"""
        await self._client.aclose()

    def _handle_http_error(self, e: httpx.HTTPStatusError) -> NoReturn:
        """Translate httpx errors to domain errors
        4xx errors are raised immediately - no retry
        5xx errors are raised as PaymentServicerError - tenacity retries these.
        """
        if e.response.status_code < 500:
            raise PaymentServiceClientError(
                message=f"Payment service rejected request: HTTP {e.response.status_code}",
                detail={"body": e.response.text},
            ) from e
        raise PaymentServiceError(
            message=f"Payment service HTTP {e.response.status_code}"
        ) from e

    @retry(
        retry=retry_if_exception_type(PaymentServiceError),
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=10),
        reraise=True,
    )
    async def register_paybill(
        self,
        landlord_id: str,
        paybill_number: str,
        consumer_key: str,
        consumer_secret: str,
        environment: str = "production",
    ) -> dict:
        """Register a landlord's Paybill with the payment service (C2B push)"""

        payload = {
            "landlord_id": landlord_id,
            "paybill_number": paybill_number,
            "consumer_key": consumer_key,
            "consumer_secret": consumer_secret,
            "environment": environment,
        }

        try:
            response = await self._client.post(
                "/api/v1/internal/register-paybill",
                json=payload,
            )
            response.raise_for_status()
            data = response.json()
            logger.info(
                "payment_service_register_paybill",
                landlord_id=landlord_id,
                paybill=paybill_number,
                c2b_registered=data.get("c2b_registered"),
            )
            return data

        except httpx.HTTPStatusError as e:
            self._handle_http_error(e)
        except httpx.RequestError as e:
            raise PaymentServiceError(message=f"Connection error: {e}") from e

    @retry(
        retry=retry_if_exception_type(PaymentServiceError),
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=10),
        reraise=True,
    )
    async def get_landlord_status(self, landlord_id: str) -> dict:
        """Get C2B registration status from Payment Service"""
        try:
            response = await self._client.get(
                f"/api/v1/internal/landlords/{landlord_id}/status"
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            self._handle_http_error(e)
        except httpx.RequestError as e:
            raise PaymentServiceError(message=f"Connection error: {e}") from e

    @retry(
        retry=retry_if_exception_type(PaymentServiceError),
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=10),
        reraise=True,
    )
    async def deactivate_landlord(self, landlord_id: str) -> None:
        """Deactivate landlord creddentials in Payment Service."""
        try:
            response = await self._client.delete(
                f"/api/v1/internal/landlords/{landlord_id}"
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            self._handle_http_error(e)
        except httpx.RequestError as e:
            raise PaymentServiceError(message=f"Connection error: {e}") from e

    @retry(
        retry=retry_if_exception_type(PaymentServiceError),
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=10),
        reraise=True,
    )
    async def retry_registration(self, landlord_id: str) -> dict:
        """Retry Daraja C2B URL registration for a landlord."""
        try:
            response = await self._client.patch(
                f"/api/v1/landlords/{landlord_id}/retry-registration"
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            self._handle_http_error(e)
        except httpx.RequestError as e:
            raise PaymentServiceError(message=f"Connection error: {e}") from e
