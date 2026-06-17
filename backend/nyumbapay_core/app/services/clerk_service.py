"""Clerk API service - user lifecycle management
Called during landlord onboarding to create the Clerk user account.
Clerk manages credentials (passwords, MFA) — i only manage roles in  DB
"""

import structlog
from clerk_backend_api import Clerk
from clerk_backend_api.models import CreateUserRequestBody, SDKError
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from app.core.config import Settings
from app.core.exceptions import ClerkError
from app.models.enums import UserRole


logger = structlog.get_logger(__name__)


class ClerkService:
    """Wrapper around Clerk Backend API"""

    def __init__(self, settings: Settings) -> None:
        self._client = Clerk(bearer_auth=settings.clerk_secret_key)

    @retry(
        retry=retry_if_exception_type(ClerkError),
        stop=stop_after_attempt(3),
        wait=wait_exponential_jitter(initial=1, max=10),
        reraise=True,
    )
    async def create_user(
        self,
        email: str,
        full_name: str,
        landlord_id: str,
    ) -> dict:
        """Creates a clerk user for a new landlord
        Creates the user with skip_password_requirement=True — Clerk sends
        an email invitation for the user to set their own password.
        """

        parts = full_name.strip().split(" ", 1)
        first_name = parts[0]
        last_name = parts[1] if len(parts) > 1 else ""

        try:
            response = await self._client.users.create_async(
                request=CreateUserRequestBody(
                    email_address=[email],
                    first_name=first_name,
                    last_name=last_name,
                    skip_password_requirement=True,
                    public_metadata={
                        "landlord_id": landlord_id,
                        "role": UserRole.LANDLORD.value,
                    },
                )
            )
            email_str = (
                response.email_addresses[0].email_address
                if response.email_addresses
                else email
            )
            logger.info(
                "clerk_user_created",
                clerk_user_id=response.id,
                email=email_str,
                landlord_id=landlord_id,
            )
            return {
                "clerk_user_id": response.id,
                "email": email_str,
            }

        except Exception as exc:
            raise ClerkError(
                message=f"Clerk user creation failed: {exc}", detail={"email": email}
            ) from exc

    async def delete_user(self, clerk_user_id: str) -> None:
        """Delete a clerk user on landlord deactivation"""
        try:
            await self._client.users.delete_async(user_id=clerk_user_id)
            logger.info("clerk_user_deleted", clerk_user_id=clerk_user_id)
        except SDKError as exc:
            logger.warning(
                "clerk_user_delete_failed",
                clerk_user_id=clerk_user_id,
                error=str(exc),
            )

    async def get_user(self, clerk_user_id: str) -> dict | None:
        """Fetch Clerk user by id"""
        try:
            user = await self._client.users.get_async(user_id=clerk_user_id)
            return {
                "clerk_user_id": user.id,
                "email": (
                    user.email_addresses[0].email_address
                    if user.email_addresses
                    else ""
                ),
            }
        except SDKError:
            return None
