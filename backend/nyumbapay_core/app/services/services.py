"""Business services - Nyumbapay core"""

from datetime import date, datetime, timezone
from decimal import Decimal
import uuid
import structlog

from app.core.exceptions import (
    BusinessRuleError,
    ConflictError,
    ForbiddenError,
    NotFoundError,
    PaymentServiceError,
)
from app.models.enums import LeaseStatus, UnitStatus, UserRole
from app.repositories.repos import (
    BuildingRepository,
    LandlordRepository,
    LeaseRepository,
    LedgerRepository,
    ReportRepository,
    TenantRepository,
    UnitRepository,
    WaterReadingRepository,
)
from app.repositories.user_repo import UserRepository
from app.models.models import Landlord, Lease, User
from app.schemas.validation import (
    BuildingDetailResponse,
    CreateBuildingRequest,
    CreateLandlordRequest,
    CreateLeaseRequest,
    CreateTenantRequest,
    CreateUnitRequest,
    CreateWaterReadingsRequest,
    LandlordListResponse,
    LandlordResponse,
    LeaseResponse,
    LedgerEntryResponse,
    PaginatedResponse,
    TenantResponse,
    UnitResponse,
    UpdateLandlordRequest,
    UpdateUnitRequest,
    WaterReadingsResponse,
)
from app.services.clerk_service import ClerkService
from app.services.payment_client import PaymentServiceClient


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
        current_user: User,
    ) -> None:
        self._building_repo = building_repo
        self._report_repo = report_repo
        self._landlord_repo = landlord_repo
        self._user = current_user

    async def _get_landlord(self) -> Landlord:
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


# unit service
class UnitService:
    """Business logic for rentable units inside a building"""

    def __init__(
        self,
        unit_repo: UnitRepository,
        landlord_repo: LandlordRepository,
        building_repo: BuildingRepository,
        current_user: User,
    ) -> None:
        self._units = unit_repo
        self._landlords = landlord_repo
        self._buildings = building_repo
        self._user = current_user

    async def _assert_building_owned(self, building_id: uuid.UUID) -> None:
        """Raise NotFoundError if building does not exist or owner mismatch"""
        building = await self._buildings.get_by_id(building_id)
        if not building:
            raise NotFoundError(message=f"Building {building_id} not found")
        landlord = await self._landlords.get_by_user_id(self._user.id)
        if not landlord:
            # If the current user has no landlord profile, treat as not found for this context
            raise NotFoundError(message="Landlord profile not found for this user")
        if building.landlord_id != landlord.id:
            raise ForbiddenError(message="Building does not belong to your account")

    async def create(
        self, building_id: uuid.UUID, request: CreateUnitRequest
    ) -> UnitResponse:
        """Add a new unit to a building. Enforces unique unit_number within the building"""

        await self._assert_building_owned(building_id)
        if await self._units.unit_number_exists(building_id, request.unit_number):
            raise ConflictError(message=f"Unit {request.unit_number} already exists")
        unit = await self._units.create(
            building_id,
            request.unit_number,
            request.rent_amount,
            request.floor,
        )
        return UnitResponse.model_validate(unit)

    async def get(self, unit_id: uuid.UUID) -> UnitResponse:
        """Return unit details"""
        unit = await self._units.get_by_id(unit_id)
        if not unit:
            raise NotFoundError(message=f"Unit {unit_id} not found")
        await self._assert_building_owned(unit.building_id)
        return UnitResponse.model_validate(unit)

    async def list(
        self,
        building_id: uuid.UUID,
        page: int,
        page_size: int,
        status: UnitStatus | None = UnitStatus.VACANT,
    ):
        """List units in a building, optionally filtered by status (vacant/occupied)"""
        await self._assert_building_owned(building_id)
        items, total = await self._units.list_by_building(
            building_id,
            status,
            page,
            page_size,
        )
        return PaginatedResponse(
            items=[UnitResponse.model_validate(u) for u in items],
            total=total,
            page=page,
            page_size=page_size,
        )

    async def update(
        self, unit_id: uuid.UUID, request: UpdateUnitRequest
    ) -> UnitResponse:
        """Update unit fields i.e rent_amount"""
        unit = await self._units.get_by_id(unit_id)
        if not unit:
            raise NotFoundError(message=f"Unit {unit_id} not found")
        await self._assert_building_owned(unit.building_id)
        updates = request.model_dump(exclude_unset=True)
        if updates:
            await self._units.update(unit_id, **updates)
        return await self.get(unit_id)


# Tenant Service
class TenantService:
    """Business logic for tenants scoped to the authenticated landlord"""

    def __init__(
        self,
        tenant_repo: TenantRepository,
        landlord_repo: LandlordRepository,
        current_user: User,
    ) -> None:
        self._tenant_repo = tenant_repo
        self._landlord_repo = landlord_repo
        self._user = current_user

    async def _get_landlord(self) -> Landlord:
        landlord = await self._landlord_repo.get_by_user_id(self._user.id)
        if not landlord:
            raise NotFoundError(message="Landlord profile not found for this user")
        return landlord

    async def create(self, request: CreateTenantRequest) -> TenantResponse:
        """Register a new tenant under the landlord's scope"""
        landlord = await self._get_landlord()
        if await self._tenant_repo.national_id_exists(landlord.id, request.national_id):
            raise ConflictError(
                message=f"National ID {request.national_id} already registered"
            )
        tenant = await self._tenant_repo.create(
            landlord.id,
            request.full_name,
            request.phone,
            request.national_id,
            request.email,
        )
        return TenantResponse.model_validate(tenant)

    async def list(self, page: int, page_size: int) -> PaginatedResponse:
        landlord = await self._get_landlord()
        items, total = await self._tenant_repo.list_by_landlord(
            landlord.id, page, page_size
        )
        return PaginatedResponse(
            items=[TenantResponse.model_validate(tenant) for tenant in items],
            total=total,
            page=page,
            page_size=page_size,
        )

    async def get(self, tenant_id: uuid.UUID) -> TenantResponse:
        tenant = await self._tenant_repo.get_by_id(tenant_id)
        if not tenant:
            raise NotFoundError(message=f"Tenant {tenant_id} not found")
        landlord = await self._get_landlord()
        if tenant.landlord_id != landlord.id:
            raise ForbiddenError(message="Tenant does not belong to your account")
        return TenantResponse.model_validate(tenant)

    async def update(
        self, tenant_id: uuid.UUID, request: CreateTenantRequest
    ) -> TenantResponse:
        await self.get(tenant_id)
        updates = request.model_dump(exclude_unset=True)
        if updates:
            await self._tenant_repo.update(tenant_id, **updates)
        return await self.get(tenant_id)


# Lease Service
class LeaseService:
    """Core business logic for creating, terminating leases, and viewing ledger entries
    This service:
        - Generates the unique M‑Pesa account_reference (BUILDINGCODE-UNITNUMBER).
        - Seeds the first water reading (required initial meter value).
        - Creates the first rent ledger entry for the current period.
        - Updates unit status to OCCUPIED.
        - Provides ledger history.
    """

    def __init__(
        self,
        lease_repo: LeaseRepository,
        unit_repo: UnitRepository,
        tenant_repo: TenantRepository,
        building_repo: BuildingRepository,
        ledger_repo: LedgerRepository,
        water_readings_repo: WaterReadingRepository,
        current_user: User,
    ):
        self._leases = lease_repo
        self._units = unit_repo
        self._tenants = tenant_repo
        self._buildings = building_repo
        self._water_readings = water_readings_repo
        self._ledger = ledger_repo
        self._user = current_user

    def _current_period(self) -> str:
        """Return YYYY‑MM string for the current UTC month"""
        now = datetime.now(tz=timezone.utc)
        return f"{now.year}-{now.month:02d}"

    async def create(
        self, unit_id: uuid.UUID, request: CreateLeaseRequest
    ) -> LeaseResponse:
        """Start a new lease on a vacant unit"""
        unit = await self._units.get_by_id(unit_id)
        if not unit:
            raise NotFoundError(message=f"Unit {unit_id} not found")
        if unit.status == UnitStatus.OCCUPIED:
            raise BusinessRuleError(message="Unit is already occupied")
        if not await self._tenants.exists(request.tenant_id):
            raise NotFoundError(message=f"Tenant {request.tenant_id} not found")
        if await self._leases.get_active_by_unit(unit_id):
            raise BusinessRuleError(message="Unit already has an active lease")

        # build the account reference: BUILDINGCODE-UNITNUMBER
        building = await self._buildings.get_by_id(unit.building_id)
        if not building:
            raise NotFoundError(message=f"Building {unit.building} not found")
        base_ref = f"{building.code}-{unit.unit_number}".upper()

        lease = await self._create_lease_with_ref(
            unit_id=unit_id,
            request=request,
            base_ref=base_ref,
            rent_amount=unit.rent_amount,
        )
        # set status
        await self._units.set_status(unit_id, UnitStatus.OCCUPIED)

        # seed water reading with initial meter value(required on first lease)
        period = self._current_period()
        await self._water_readings.create(
            unit_id=unit_id,
            lease_id=lease.id,
            period=period,
            previous_reading=request.initial_water_reading,
            current_reading=request.initial_water_reading,
            rate_per_unit=Decimal("0"),
            entered_by=self._user.id,
            entered_at=datetime.now(tz=timezone.utc),
        )
        # create first ledger entry for current period
        cfg = await self._buildings.get_latest_charge_config(unit.building_id)
        if not cfg:
            raise BusinessRuleError(
                message=f"Building {unit.building_id} has no charge configuration"
            )
        await self._ledger.create(
            lease_id=lease.id,
            period=period,
            base_rent=unit.rent_amount,
            garbage_charge=cfg.garbage_charge,
            previous_balance=Decimal("0"),
        )
        logger.info(
            "lease_created",
            lease_id=str(lease.id),
            account_ref=lease.account_reference,
            unit_id=str(unit_id),
        )
        return LeaseResponse.model_validate(lease)

    async def _create_lease_with_ref(
        self,
        unit_id: uuid.UUID,
        request: CreateLeaseRequest,
        base_ref: str,
        rent_amount: Decimal,
    ) -> Lease:
        """Attempt lease creation ,retrying once with a suffixed reference if the base reference collides"""
        try:
            return await self._leases.create(
                unit_id=unit_id,
                tenant_id=request.tenant_id,
                rent_amount=rent_amount,
                deposit_amount=request.deposit_amount,
                start_date=request.start_date,
                end_date=request.end_date,
                account_reference=base_ref,
            )
        except ConflictError:
            # Base ref collided. Generate a suffixed fallback using a fresh UUID
            fallback_ref = f"{base_ref}-{uuid.uuid4().hex[:6].upper()}"
            logger.info(
                "account_reference_fallback",
                base_ref=base_ref,
                fallback_ref=fallback_ref,
                unit_id=str(unit_id),
            )
            return await self._leases.create(
                unit_id=unit_id,
                tenant_id=request.tenant_id,
                rent_amount=rent_amount,
                deposit_amount=request.deposit_amount,
                start_date=request.start_date,
                end_date=request.end_date,
                account_reference=fallback_ref,
            )

    async def terminate(self, lease_id: uuid.UUID) -> None:
        """Terminate an active lease, set unit back to VACANT"""
        lease = await self._leases.get_by_id(lease_id)
        if not lease:
            raise NotFoundError(message=f"Lease {lease_id} not found")
        if lease.status == LeaseStatus.TERMINATED:
            raise BusinessRuleError(message="Lease is already terminated")
        now = datetime.now(tz=timezone.utc)
        await self._leases.terminate(lease_id, now)
        await self._units.set_status(lease.unit_id, UnitStatus.VACANT)
        logger.info("lease_terminated", lease_id=str(lease_id))

    async def get_ledger(self, lease_id: uuid.UUID) -> list[LedgerEntryResponse]:
        """Return all rent ledger entries for a lease, ordered by period descending"""
        entries = await self._ledger.list_by_lease(lease_id)
        return [
            LedgerEntryResponse(
                id=e.id,
                lease_id=e.lease_id,
                period=e.period,
                base_rent=e.base_rent,
                water_charge=e.water_charge,
                garbage_charge=e.garbage_charge,
                previous_balance=e.previous_balance,
                total_amount_due=e.total_amount_due,
                amount_paid=e.amount_paid,
                balance=e.balance,
                status=e.status.value,
                water_reading_entered=e.water_reading_id is not None,
            )
            for e in entries
        ]


class WaterReadingService:
    """Process montly water meter readings
    Ensures:
        - No duplicate reading for same unit/period.
        - Current reading >= previous reading.
        - An active lease exists.
        - A building charge configuration (water rate) exists.
        - Updates the corresponding rent ledger with the calculated water charge.
    """

    def __init__(
        self,
        reading_repo: WaterReadingRepository,
        unit_repo: UnitRepository,
        building_repo: BuildingRepository,
        ledger_repo: LedgerRepository,
        current_user: User,
    ) -> None:
        self._readings = reading_repo
        self._units = unit_repo
        self._buildings = building_repo
        self._ledger = ledger_repo
        self._user = current_user

    async def enter_reading(
        self, unit_id: uuid.UUID, req: CreateWaterReadingsRequest
    ) -> WaterReadingsResponse:
        """Submit a water reading"""
        pass
