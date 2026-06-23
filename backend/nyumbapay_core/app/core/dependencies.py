"""FastAPI dependency injection- Clerk based auth"""

from typing import Annotated
from fastapi import Depends, Security, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
import structlog

from sqlalchemy.ext.asyncio import AsyncSession
import redis.asyncio as aioredis

from nyumbapay_core.app import User
from nyumbapay_core.app.core.config import Settings, get_settings
from nyumbapay_core.app.core.database import get_db_session, get_redis
from nyumbapay_core.app.core.exceptions import AuthError, ForbiddenError
from nyumbapay_core.app.core.security import get_token_verifier
from nyumbapay_core.app.models.enums import UserRole
from nyumbapay_core.app.repositories.building_repo import BuildingRepository
from nyumbapay_core.app.repositories.landlord_repo import LandlordRepository
from nyumbapay_core.app.repositories.report_repo import ReportRepository
from nyumbapay_core.app.repositories.repos import (
    LeaseRepository,
    LedgerRepository,
    PaymentRepository,
    TenantRepository,
    UnitRepository,
    WaterReadingRepository,
)
from nyumbapay_core.app.repositories.user_repo import UserRepository
from nyumbapay_core.app.services.clerk_service import ClerkService
from nyumbapay_core.app.services.services import (
    BuildingService,
    LandlordService,
    LeaseService,
    ReconciliationService,
    ReportService,
    TenantService,
    UnitService,
    WaterReadingService,
)


logger = structlog.get_logger(__name__)
_bearer = HTTPBearer(auto_error=False)

# primitive dependencies
SettingsDep = Annotated[Settings, Depends(get_settings)]
DbSessionDep = Annotated[AsyncSession, Depends(get_db_session)]
RedisDep = Annotated[aioredis.Redis, Depends(get_redis)]


# Auth
async def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Security(_bearer)],
    settings: SettingsDep,
    db: DbSessionDep,
) -> User:
    """Verify Clerk JWT and return the authenticated User ORM object.

    Flow:
    1. Extract Bearer token from Authorization header
    2. Verify via Clerk JWKS (RS256) — cached signing key
    3. Extract clerk_user_id from `sub` claim
    4. Look up our DB User by clerk_user_id
    5. Bind user context to structlog for downstream logging
    """

    if not credentials or credentials.scheme.lower() != "bearer":
        raise AuthError(message="Missing or invalid Authorization header")

    verifier = get_token_verifier(settings.clerk_jwks_url)
    payload = verifier.verify(credentials.credentials)
    clerk_user_id: str = payload["sub"]

    user = await UserRepository(db).get_by_clerk_id(clerk_user_id)

    if user is None or not user.is_active:
        raise AuthError(message="User not found or deactivated")

    structlog.contextvars.bind_contextvars(
        user_id=str(user.id), clerk_user_id=clerk_user_id, role=user.role.value
    )
    return user


CurrentUserDep = Annotated[User, Depends(get_current_user)]


async def require_super_admin(current_user: CurrentUserDep):
    if current_user.role != UserRole.SUPER_ADMIN:
        raise ForbiddenError(message="Super-admin access required")
    return current_user


async def require_landlord(current_user: CurrentUserDep):
    if current_user.role != UserRole.LANDLORD:
        raise ForbiddenError(message="Landlord access required")
    return current_user


async def require_authenticated(current_user: CurrentUserDep) -> User:
    return current_user


SuperAdminDep = Annotated[User, Depends(require_super_admin)]
LandlordUserDep = Annotated[User, Depends(require_landlord)]
AuthenticatedUserDep = Annotated[User, Depends(require_authenticated)]

# long-lived HTTP client deps


def _get_payment_client(request: Request):
    """Retrieve the long-lived PaymentServiceClient from app.state"""
    return request.app.state.payment_client


def _get_clerk_service(request: Request):
    """Retrieve the long-lived ClerkService from app.state"""
    return request.app.state.clerk_service


# Service factories
def get_clerk_service(settings: SettingsDep):
    """Produce a clerkservice for clerk API operations"""
    return ClerkService(settings=settings)


def get_landlord_service(
    db: DbSessionDep,
    payment_client=Depends(_get_payment_client),
    clerk_service=Depends(_get_clerk_service),
):
    """Produce a LandlordService for landlord onboarding and management"""
    return LandlordService(
        user_repo=UserRepository(db),
        landlord_repo=LandlordRepository(db),
        payment_client=payment_client,
        clerk_service=clerk_service,
    )


def get_building_service(db: DbSessionDep, current_user: LandlordUserDep):
    """Produce a BuildingService scoed to the authenticated landlord"""
    return BuildingService(
        building_repo=BuildingRepository(db),
        report_repo=ReportRepository(db),
        landlord_repo=LandlordRepository(db),
        current_user=current_user,
    )


def get_unit_service(db: DbSessionDep, current_user: LandlordUserDep):
    """Produce a UnitService scoped to the authenticated landlord"""
    return UnitService(
        unit_repo=UnitRepository(db),
        landlord_repo=LandlordRepository(db),
        building_repo=BuildingRepository(db),
        current_user=current_user,
    )


def get_tenant_service(db: DbSessionDep, current_user: LandlordUserDep):
    """Produce a TenanctService scoped to the authenticated landlord"""

    return TenantService(
        tenant_repo=TenantRepository(db),
        landlord_repo=LandlordRepository(db),
        current_user=current_user,
    )


def get_lease_service(db: DbSessionDep, current_user: LandlordUserDep):
    """Produce a LeaseService scoped to the authenticated landlord"""
    return LeaseService(
        lease_repo=LeaseRepository(db),
        unit_repo=UnitRepository(db),
        tenant_repo=TenantRepository(db),
        building_repo=BuildingRepository(db),
        ledger_repo=LedgerRepository(db),
        water_readings_repo=WaterReadingRepository(db),
        current_user=current_user,
    )


def get_water_reading_service(db: DbSessionDep, current_user=Depends(require_landlord)):
    """Produce a WaterReadingService scoped to the authenticated landlord"""
    return WaterReadingService(
        reading_repo=WaterReadingRepository(db),
        lease_repo=LeaseRepository(db),
        unit_repo=UnitRepository(db),
        building_repo=BuildingRepository(db),
        ledger_repo=LedgerRepository(db),
        current_user=current_user,
    )


def get_reconciliation_service(
    db: DbSessionDep, current_user=Depends(require_landlord)
):
    """Produce a ReconciliationService scoped to the authenticated landlord"""
    return ReconciliationService(
        payment_repo=PaymentRepository(db),
        ledger_repo=LedgerRepository(db),
        lease_repo=LeaseRepository(db),
        landlord_repo=LandlordRepository(db),
        current_user=current_user,
    )


def get_report_service(db: DbSessionDep, current_user: LandlordUserDep):
    """Produce a ReportService scoped to the authenticated landlord"""
    return ReportService(repo=ReportRepository(db), current_user=current_user)
