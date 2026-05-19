"""Business services - Nyumbapay core"""

from datetime import date
from decimal import Decimal
import uuid
import structlog

from nyumbapay_core.app.core.exceptions import (
    ConflictError,
    ForbiddenError,
    NotFoundError,
    PaymentServiceError,
)
from nyumbapay_core.app.models.enums import UserRole
from nyumbapay_core.app.repositories.repos import (
    BuildingRepository,
    LandlordRepository,
    ReportRepository,
)
from nyumbapay_core.app.repositories.user_repo import UserRepository
from nyumbapay_core.app.schemas.validation import (
    BuildingDetailResponse,
    CreateBuildingRequest,
    CreateLandlordRequest,
    LandlordListResponse,
    LandlordResponse,
    PaginatedResponse,
    UpdateLandlordRequest,
)
from nyumbapay_core.app.services.clerk_service import ClerkService
from nyumbapay_core.app.services.payment_client import PaymentServiceClient


logger = structlog.get_logger(__name__)


# Landlord service
class LandlordService:
    """Business logic for landlord registration, profile management, and deactivation"""

    def __init__(
        self,
        user_repo: UserRepository,
        landlord_repo: LandlordRepository,
        payment_client: PaymentServiceClient,
        clerk_service: ClerkService,
    ) -> None:
        self._user_repo = user_repo
        self._landlord_repo = landlord_repo
        self._payment_client = payment_client
        self._clerk = clerk_service

    async def create_landlord(self, request: CreateLandlordRequest) -> tuple:
        """'Register a new landlord
        Steps:
            1. Validate email and paybill uniqueness (via repos).
            2. Create user in Clerk (receives email to set password).
            3. Create User record in local database (with clerk_user_id).
            4. Create Landlord record.
            5. Attempt to register Paybill with payment service (C2B).
            6. Return LandlordResponse and registration flag.
        """

        # uniqueness
        if await self._user_repo.email_exists(request.email):
            raise ConflictError(
                message=f"Email {request.email} already registered",
                detail={"field": "email"},
            )
        if await self._landlord_repo.paybill_exists(request.paybill_number):
            raise ConflictError(
                message=f"Paybill {request.paybill_number} already registered",
                detail={"field": "paybill_number"},
            )

        # create Clerk user - they receive email to set password
        clerk_data = await self._clerk.create_user(
            email=request.email,
            full_name=request.full_name,
            landlord_id="pending",  # updated after landlord row created
        )
        clerk_user_id = clerk_data["clerk_user_id"]

        user = await self._user_repo.create(
            clerk_user_id=clerk_user_id,
            email=request.email,
            role=UserRole.LANDLORD,
        )
        landlord = await self._landlord_repo.create(
            user_id=user.id,
            full_name=request.full_name,
            phone=request.phone,
            business_name=request.business_name,
            paybill_number=request.paybill_number,
        )
        c2b_registered = False
        try:
            payment_service = await self._payment_client.register_paybill(
                landlord_id=str(landlord.id),
                paybill_number=request.paybill_number,
                consumer_key=request.daraja_consumer_key,
                consumer_secret=request.daraja_consumer_secret,
                environment=request.daraja_environment,
            )
            c2b_registered = payment_service.get("c2b_registered", False)
        except Exception as exc:
            logger.error(
                "payment_service_registration_failed",
                landlord_id=str(landlord.id),
                error=str(exc),
            )
        response = LandlordResponse(
            id=landlord.id,
            user_id=landlord.user_id,
            full_name=landlord.full_name,
            phone=landlord.phone,
            business_name=landlord.business_name,
            paybill_number=landlord.paybill_number,
            is_active=landlord.is_active,
            c2b_registered=c2b_registered,
            created_at=landlord.created_at,
        )
        return response, c2b_registered

    async def list_landlords(self, page: int, page_size: int):
        """Paginated list of all landlords (admin only). Returns LandlordListResponse"""
        items, total = await self._landlord_repo.list_all(page, page_size)
        return LandlordListResponse(
            items=[
                LandlordResponse(
                    id=landlord.id,
                    user_id=landlord.user_id,
                    full_name=landlord.full_name,
                    phone=landlord.phone,
                    business_name=landlord.business_name,
                    paybill_number=landlord.paybill_number,
                    is_active=landlord.is_active,
                    c2b_registered=False,
                    created_at=landlord.created_at,
                )
                for landlord in items
            ],
            total=total,
            page=page,
            page_size=page_size,
        )

    async def get_landlord(self, landlord_id: uuid.UUID) -> LandlordResponse:
        """Retrieve a landlord by ID, including C2B registration status from payment service"""
        landlord = await self._landlord_repo.get_by_id(landlord_id)
        if not landlord:
            raise NotFoundError(message=f"Landlord {landlord_id} not found")
        c2b_registered = False
        try:
            payment_service = await self._payment_client.get_landlord_status(
                str(landlord_id)
            )
            c2b_registered = payment_service.get("c2b_registered", False)
        except PaymentServiceError as exc:
            logger.warning(
                "payment_service_unavailable",
                landlord_id=str(landlord_id),
                error=str(exc),
            )
        return LandlordResponse(
            id=landlord.id,
            user_id=landlord.user_id,
            full_name=landlord.full_name,
            phone=landlord.phone,
            business_name=landlord.business_name,
            paybill_number=landlord.paybill_number,
            is_active=landlord.is_active,
            c2b_registered=c2b_registered,
            created_at=landlord.created_at,
        )

    async def update_landlord(
        self, landlord_id: uuid.UUID, request: UpdateLandlordRequest
    ):
        """Partially update a landlord profile. Pass only fields to change"""
        await self.get_landlord(landlord_id)
        updates = request.model_dump(exclude_none=True)
        if updates:
            await self._landlord_repo.update(landlord_id, **updates)
        return await self.get_landlord(landlord_id)

    async def deactivate_landlord(self, landlord_id: uuid.UUID) -> None:
        """Soft‑delete a landlord: deactivate local user, delete Clerk user, disable payment service"""
        landlord = await self._landlord_repo.get_by_id(landlord_id)
        if not landlord:
            raise NotFoundError(message=f"Landlord {landlord_id} not found")
        await self._landlord_repo.update(landlord_id, is_active=False)
        await self._user_repo.deactivate(landlord.user_id)
        user = await self._user_repo.get_by_id(landlord.user_id)
        if user:
            await self._clerk.delete_user(user.clerk_user_id)
        try:
            await self._payment_client.deactivate_landlord(str(landlord_id))
        except Exception as exc:
            logger.warning("payment_deactivate_failed", error=str(exc))

    async def retry_registration(self, landlord_id: uuid.UUID) -> dict:
        await self.get_landlord(landlord_id)
        return await self._payment_client.retry_registration(str(landlord_id))


# Building service
class BuildingService:
    """Business logic for buildings owned by the authenticated landlord"""

    def __init__(
        self,
        building_repo: BuildingRepository,
        report_repo: ReportRepository,
        landlord_repo: LandlordRepository,
        current_user,
    ) -> None:
        self._building_repo = building_repo
        self._report_repo = report_repo
        self._landlord_repo = landlord_repo
        self._user = current_user

    async def _get_landlord(self):
        landlord = await self._landlord_repo.get_by_user_id(self._user.id)
        if not landlord:
            raise NotFoundError(message="Landlord profile not found for this user")
        return landlord

    async def create(self, request: CreateBuildingRequest) -> BuildingDetailResponse:
        """Create a new building for the authenticated landlord"""
        landlord = await self._get_landlord()
        if await self._building_repo.code_exists(landlord.id, request.code):
            raise ConflictError(message=f"Building code {request.code} already used")
        building = await self._building_repo.create(
            landlord.id,
            request.name,
            request.address,
            request.city,
            request.code,
        )
        await self._building_repo.create_charge_config(
            building.id,
            request.garbage_charge,
            request.water_rate_per_unit,
            date.today(),
        )
        return BuildingDetailResponse(
            id=building.id,
            landlord_id=landlord.id,
            name=building.name,
            address=building.address,
            city=building.city,
            code=building.code,
            created_at=building.created_at,
            water_rate_per_unit=request.water_rate_per_unit,
            garbage_charge=request.garbage_charge,
            total_units=0,
            occupied_units=0,
        )

    async def list(self, page: int, page_size: int):
        """Paginated list of buildings for the landlord, enriched with unit counts and current charges"""
        landlord = await self._get_landlord()
        rows, total = await self._report_repo.get_buildings_with_stats(
            landlord.id,
            page,
            page_size,
        )
        items = [BuildingDetailResponse(**row) for row in rows]
        return PaginatedResponse(
            items=items, total=total, page=page, page_size=page_size
        )

    async def get(self, building_id: uuid.UUID) -> BuildingDetailResponse:
        """Fetch a single building, verifying ownership, with current charges and unit counts"""
        building = await self._building_repo.get_by_id(building_id)
        if not building:
            raise NotFoundError(message=f"Building {building_id} not found")
        landlord = await self._get_landlord()
        if building.landlord_id != landlord.id:
            raise ForbiddenError(
                message="This Building does not belong to your account"
            )
        total, occupied = await self._building_repo.count_units(building_id)
        charge_config = await self._building_repo.get_latest_charge_config(building_id)
        return BuildingDetailResponse(
            id=building.id,
            landlord_id=building.landlord_id,
            name=building.name,
            address=building.address,
            city=building.city,
            code=building.code,
            created_at=building.created_at,
            water_rate_per_unit=charge_config.water_rate_per_unit
            if charge_config
            else Decimal("0"),
            garbage_charge=charge_config.garbage_charge
            if charge_config
            else Decimal("500"),
            total_units=total,
            occupied_units=occupied,
        )
