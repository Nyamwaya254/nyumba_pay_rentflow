"""All Repositories for NyumbaPay core"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Sequence
import uuid
from sqlalchemy import case, func, select, text, update
import structlog
from sqlalchemy.ext.asyncio import AsyncSession

from nyumbapay_core.app.models.enums import (
    LeaseStatus,
    LedgerStatus,
    NotificationStatus,
    UnitStatus,
)
from nyumbapay_core.app.models.models import (
    Building,
    BuildingChargeConfig,
    Landlord,
    Lease,
    MpesaPayment,
    NotificationLog,
    RentLedger,
    Tenant,
    Unit,
    WaterReading,
)

logger = structlog.get_logger(__name__)


# landlord
class LandlordRepository:
    """Handles operations on the landlord table"""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_id(self, landlord_id: uuid.UUID) -> Landlord | None:
        """Fetch a landlord by PK"""
        result = await self._session.execute(
            select(Landlord).where(Landlord.id == landlord_id)
        )
        return result.scalar_one_or_none()

    async def get_by_user_id(self, user_id: uuid.UUID) -> Landlord | None:
        """Fetch the landlord profile linked to a specific user account"""
        result = await self._session.execute(
            select(Landlord).where(Landlord.user_id == user_id)
        )
        return result.scalar_one_or_none()

    async def get_by_paybill(self, paybill: str) -> Landlord | None:
        """Retrieve active landlord by M-Pesa paybill Number.
        Used by Mpesa webhook to identify which landlord owns the paybill
        """
        result = await self._session.execute(
            select(Landlord).where(
                Landlord.paybill_number == paybill,
                Landlord.is_active == True,  # noqa
            )
        )
        return result.scalar_one_or_none()

    async def paybill_exists(self, paybill: str) -> bool:
        """Checks if a paybill number is already registered (any status)"""
        result = await self._session.execute(
            select(Landlord.id).where(Landlord.paybill_number == paybill)
        )
        return result.scalar_one_or_none() is not None

    async def create(
        self,
        user_id: uuid.UUID,
        full_name: str,
        phone: str,
        business_name: str,
        paybill_number: str,
    ) -> Landlord:
        """Insert a new landlord ,flush and return the full object"""
        landlord = Landlord(
            user_id=user_id,
            full_name=full_name,
            phone=phone,
            business_name=business_name,
            paybill_number=paybill_number,
        )

        self._session.add(landlord)
        await self._session.flush()
        await self._session.refresh(landlord)
        return landlord

    async def list_all(
        self, page: int, page_size: int
    ) -> tuple[Sequence[Landlord], int]:
        """Paginated list of all landlords,ordered newest first"""
        total = (
            await self._session.execute(select(func.count()).select_from(Landlord))
        ).scalar_one()
        items = (
            (
                await self._session.execute(
                    select(Landlord)
                    .order_by(Landlord.created_at.desc())
                    .offset((page - 1) * page_size)
                    .limit(page_size)
                )
            )
            .scalars()
            .all()
        )
        return items, total

    async def update(self, landlord_id: uuid.UUID, **fields) -> None:
        """Partial update of a landlord (e.g deactivate,change business name)"""
        await self._session.execute(
            update(Landlord).where(Landlord.id == landlord_id).values(**fields)
        )


# Building repo


class BuildingRepository:
    """Handles buildings,their charge configurations and unit counts"""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_id(self, building_id: uuid.UUID) -> Building | None:
        """Fetch building by ID"""
        result = await self._session.execute(
            select(Building).where(Building.id == building_id)
        )
        return result.scalar_one_or_none()

    async def list_by_landlord(
        self,
        landlord_id: uuid.UUID,
        page: int,
        page_size: int,
    ) -> tuple[Sequence[Building], int]:
        """Paginated list of buildings owned by a landlord,ordered by name"""
        base = select(Building).where(Building.landlord_id == landlord_id)
        total = (
            await self._session.execute(
                select(func.count()).select_from(base.subquery())
            )
        ).scalar_one()
        items = (
            (
                await self._session.execute(
                    base.order_by(Building.name)
                    .offset((page - 1) * page_size)
                    .limit(page_size)
                )
            )
            .scalars()
            .all()
        )
        return items, total

    async def code_exists(self, landlord_id: uuid.UUID, code: str) -> bool:
        """Check if a building code (e.g., 'PALM') is already used by this landlord."""
        result = await self._session.execute(
            select(Building.id).where(
                Building.landlord_id == landlord_id,
                Building.code == code,
            )
        )
        return result.scalar_one_or_none() is not None

    async def create(
        self,
        landlord_id: uuid.UUID,
        name: str,
        address: str,
        city: str,
        code: str,
    ) -> Building:
        """Create a new building, return the ORM object."""
        building = Building(
            landlord_id=landlord_id,
            name=name,
            address=address,
            city=city,
            code=code,
        )

        self._session.add(building)
        await self._session.flush()
        await self._session.refresh(building)
        return building

    async def get_latest_charge_config(
        self, building_id: uuid.UUID
    ) -> BuildingChargeConfig | None:
        """Return the most recent charge configuration (highest effective_from)."""
        result = await self._session.execute(
            select(BuildingChargeConfig)
            .where(BuildingChargeConfig.building_id == building_id)
            .order_by(BuildingChargeConfig.effective_from.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def create_charge_config(
        self,
        building_id: uuid.UUID,
        garbage_charge: Decimal,
        water_rate_per_unit: Decimal,
        effective_from: date,
    ) -> BuildingChargeConfig:
        """Insert a new charge configuration (append‑only)"""
        charge_config = BuildingChargeConfig(
            building_id=building_id,
            garbage_charge=garbage_charge,
            water_rate_per_unit=water_rate_per_unit,
            effective_from=effective_from,
        )
        self._session.add(charge_config)
        await self._session.flush()
        await self._session.refresh(charge_config)
        return charge_config

    async def count_units(self, building_id: uuid.UUID) -> tuple[int, int]:
        """Returns (total_units, occupied_units) for dashboard stats."""
        total, occupied = (
            await self._session.execute(
                select(
                    func.count().label("total"),
                    func.count(case((Unit.status == UnitStatus.OCCUPIED, 1))).label(
                        "occupied"
                    ),
                ).where(Unit.building_id == building_id)
            )
        ).one()
        return total, occupied


# unit repo


class UnitRepository:
    """Handles units in a building"""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_id(self, unit_id: uuid.UUID) -> Unit | None:
        """Fetch a unit by its PK"""
        result = await self._session.execute(select(Unit).where(Unit.id == unit_id))
        return result.scalar_one_or_none()

    async def list_by_building(
        self,
        building_id: uuid.UUID,
        status: UnitStatus | None,
        page: int,
        page_size: int,
    ) -> tuple[Sequence[Unit], int]:
        """List units in a building, optionally filtered by status (vacant/occupied)
        Pass status to filter, pass None to get all units
        """
        conds = [Unit.building_id == building_id]
        if status:
            conds.append(Unit.status == status)
        base = select(Unit).where(*conds)
        total = (
            await self._session.execute(
                select(func.count()).select_from(base.subquery())
            )
        ).scalar_one()
        items = (
            (
                await self._session.execute(
                    base.order_by(Unit.unit_number)
                    .offset((page - 1) * page_size)
                    .limit(page_size)
                )
            )
            .scalars()
            .all()
        )
        return items, total

    async def unit_number_exists(
        self, building_id: uuid.UUID, unit_number: str
    ) -> bool:
        """Check if a unit number already exists in the building"""
        result = await self._session.execute(
            select(Unit).where(
                Unit.building_id == building_id,
                Unit.unit_number == unit_number,
            )
        )
        return result.scalar_one_or_none() is not None

    async def create(
        self,
        building_id: uuid.UUID,
        unit_number: str,
        rent_amount: Decimal,
        floor: int | None,
    ) -> Unit:
        """Create a new unit  in building"""
        unit = Unit(
            building_id=building_id,
            unit_number=unit_number,
            rent_amount=rent_amount,
            floor=floor,
        )
        self._session.add(unit)
        await self._session.flush()
        await self._session.refresh(unit)
        return unit

    async def set_status(self, unit_id: uuid.UUID, status: UnitStatus) -> None:
        await self._session.execute(
            update(Unit).where(Unit.id == unit_id).values(status=status)
        )

    async def update(self, unit_id: uuid.UUID, **fields) -> None:
        await self._session.execute(
            update(Unit).where(Unit.id == unit_id).values(**fields)
        )


# tenant
class TenantRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_id(self, tenant_id: uuid.UUID) -> Tenant | None:
        """Fetch tenant by their id"""
        result = await self._session.execute(
            select(Tenant).where(Tenant.id == tenant_id)
        )
        return result.scalar_one_or_none()

    async def list_by_landlord(
        self, landlord_id: uuid.UUID, page: int, page_size: int
    ) -> tuple[Sequence[Tenant], int]:
        """Paginated list of all tenants of a landlord, sorted by name"""
        base = select(Tenant).where(Tenant.landlord_id == landlord_id)
        total = (
            await self._session.execute(
                select(func.count()).select_from(base.subquery())
            )
        ).scalar_one()
        items = (
            (
                await self._session.execute(
                    base.order_by(Tenant.full_name)
                    .offset((page - 1) * page_size)
                    .limit(page_size)
                )
            )
            .scalars()
            .all()
        )
        return items, total

    async def national_id_exists(
        self, landlord_id: uuid.UUID, national_id: str
    ) -> bool:
        """Prevent duplicate national IDs under the same landlord."""
        result = await self._session.execute(
            select(Tenant.id).where(
                Tenant.landlord_id == landlord_id, Tenant.national_id == national_id
            )
        )
        return result.scalar_one_or_none() is not None

    async def create(
        self,
        landlord_id: uuid.UUID,
        full_name: str,
        phone: str,
        national_id: str,
        email: str | None,
    ) -> Tenant:
        """Register a new tenant"""
        tenant = Tenant(
            landlord_id=landlord_id,
            full_name=full_name,
            phone=phone,
            national_id=national_id,
            email=email,
        )
        self._session.add(tenant)
        await self._session.flush()
        await self._session.refresh(tenant)
        return tenant

    async def update(self, tenant_id: uuid.UUID, **fields) -> None:
        """Update tenant information"""
        await self._session.execute(
            update(Tenant).where(Tenant.id == tenant_id).values(**fields)
        )


# lease
class LeaseRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_id(self, lease_id: uuid.UUID) -> Lease | None:
        """Fetch lease by their id"""
        result = await self._session.execute(select(Lease).where(Lease.id == lease_id))
        return result.scalar_one_or_none()

    async def get_active_by_unit(self, unit_id: uuid.UUID) -> Lease | None:
        """Return the currently active lease for a unit"""
        result = await self._session.execute(
            select(Lease).where(
                Lease.unit_id == unit_id,
                Lease.status == LeaseStatus.ACTIVE,
            )
        )
        return result.scalar_one_or_none()

    async def get_by_account_reference(self, ref: str) -> Lease | None:
        """Find active lease by M‑Pesa account reference (e.g., 'PALM-A3')"""
        result = await self._session.execute(
            select(Lease).where(
                Lease.account_reference == ref.upper().strip(),
                Lease.status == LeaseStatus.ACTIVE,
            )
        )
        return result.scalar_one_or_none()

    async def create(
        self,
        unit_id: uuid.UUID,
        tenant_id: uuid.UUID,
        rent_amount: Decimal,
        deposit_amount: Decimal,
        start_date: date,
        end_date: date | None,
        account_reference: str,
    ) -> Lease:
        """Start a new lease. Also updates unit status to OCCUPIED (caller’s responsibility)"""
        lease = Lease(
            unit_id=unit_id,
            tenant_id=tenant_id,
            rent_amount=rent_amount,
            deposit_amount=deposit_amount,
            start_date=start_date,
            end_date=end_date,
            account_reference=account_reference,
        )
        self._session.add(lease)
        await self._session.flush()
        await self._session.refresh(lease)
        return lease

    async def terminate(self, lease_id: uuid.UUID, terminated_at) -> None:
        """Mark a lease as terminated"""
        await self._session.execute(
            update(Lease)
            .where(Lease.id == lease_id)
            .values(
                status=LeaseStatus.TERMINATED,
                terminated_at=terminated_at,
            )
        )

    async def get_all_active(self) -> Sequence[Lease]:
        """Used by Celery background tasks (e.g., monthly billing) to iterate all active leases"""
        result = await self._session.execute(
            select(Lease).where(Lease.status == LeaseStatus.ACTIVE)
        )
        return result.scalars().all()


# water reading
class WaterReadingRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_latest_by_unit(self, unit_id: uuid.UUID) -> WaterReading | None:
        """Last reading for a unit (used to compute previous_reading for next period)."""
        result = await self._session.execute(
            select(WaterReading)
            .where(WaterReading.unit_id == unit_id)
            .order_by(WaterReading.period.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def get_by_unit_by_specific_period(
        self, unit_id: uuid.UUID, period: str
    ) -> WaterReading | None:
        """Check if a reading already exists for that month (idempotency)"""
        result = await self._session.execute(
            select(WaterReading).where(
                WaterReading.unit_id == unit_id,
                WaterReading.period == period,
            )
        )
        return result.scalar_one_or_none()

    async def create(
        self,
        unit_id: uuid.UUID,
        lease_id: uuid.UUID,
        period: str,
        previous_reading: Decimal,
        current_reading: Decimal,
        rate_per_unit: Decimal,
        entered_by: uuid.UUID,
        entered_at: datetime,
    ) -> WaterReading:
        """Store a meter reading, compute consumption and water charge."""
        consumed = current_reading - previous_reading
        charge = consumed * rate_per_unit
        water_reading = WaterReading(
            unit_id=unit_id,
            lease_id=lease_id,
            period=period,
            previous_reading=previous_reading,
            current_reading=current_reading,
            units_consumed=consumed,
            rate_per_unit=rate_per_unit,
            water_charge=charge,
            entered_by=entered_by,
            entered_at=entered_at,
        )
        self._session.add(water_reading)
        await self._session.flush()
        await self._session.refresh(water_reading)
        return water_reading


# notification
class NotificationRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        tenant_id: uuid.UUID,
        lease_id: uuid.UUID,
        channel: str,
        notif_type: str,
        period: str,
    ) -> NotificationLog:
        """Create a log entry before sending (status = PENDING)"""
        entry = NotificationLog(
            tenant_id=tenant_id,
            lease_id=lease_id,
            channel=channel,
            type=notif_type,
            period=period,
        )
        self._session.add(entry)
        await self._session.flush()
        await self._session.refresh(entry)
        return entry

    async def mark_sent(
        self,
        notif_id: uuid.UUID,
        provider_message_id: str,
        sent_at: datetime,
    ) -> None:
        """Update log after successful delivery."""
        await self._session.execute(
            update(NotificationLog)
            .where(NotificationLog.id == notif_id)
            .values(
                status=NotificationStatus.SENT,
                provider_message_id=provider_message_id,
                sent_at=sent_at,
            )
        )

    async def mark_failed(self, notif_id: uuid.UUID, error: str) -> None:
        """Record failure reason"""
        await self._session.execute(
            update(NotificationLog)
            .where(NotificationLog.id == notif_id)
            .values(status=NotificationStatus.FAILED, error_detail=error)
        )

    async def already_sent(
        self, lease_id: uuid.UUID, period: str, notif_type: str
    ) -> bool:
        """Idempotency check – prevent sending the same notification twice."""
        result = await self._session.execute(
            select(NotificationLog.id).where(
                NotificationLog.lease_id == lease_id,
                NotificationLog.period == period,
                NotificationLog.type == notif_type,
                NotificationLog.status == "sent",
            )
        )
        return result.scalar_one_or_none() is not None


# report repo
class ReportRepository:
    """This repo uses raw SQL(text) for complex aggregations"""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def gef_defaulters(self, landlord_id: uuid.UUID, period: str) -> list[dict]:
        """Return all tenants with  upaid/partial ledger entries for
        a given period under a specific landlord,with contanct details
        Used for automated reminders
        """

        stmt = text("""
            SELECT
                rl.id            AS lease_id,
                u.unit_number,
                b.name           AS building_name,
                t.full_name      AS tenant_name,
                t.phone          AS tenant_phone,
                l.account_reference,
                rl.period,
                rl.total_amount_due,
                rl.amount_paid,
                rl.balance,
                rl.status
            FROM rent_ledger rl
            JOIN leases l ON l.id = rl.lease_id
            JOIN units u ON u.id = l.unit_id
            JOIN buildings b ON b.id = u.building_id
            JOIN tenants t ON t.id = l.tenant_id
            WHERE b.landlord_id = :landlord_id
              AND rl.period = :period
              AND rl.status IN ('unpaid', 'partial')
            ORDER BY rl.balance DESC

        """)
        result = await self._session.execute(
            stmt, {"landlord_id": landlord_id, "period": period}
        )
        return [dict(row._mapping) for row in result]

    async def get_occupancy(self, landlord_id: uuid.UUID) -> list[dict]:
        """Returns per-building occupancy stats for a landlord"""

        stmt = text("""
            SELECT
                b.id             AS building_id,
                b.name           AS building_name,
                COUNT(u.id)      AS total_units,
                COUNT(u.id) FILTER (WHERE u.status = 'occupied') AS occupied,
                COUNT(u.id) FILTER (WHERE u.status = 'vacant')   AS vacant
            FROM buildings b
            JOIN units u ON u.building_id = b.id
            WHERE b.landlord_id = :landlord_id
            GROUP BY b.id, b.name
            ORDER BY b.name
        """)
        result = await self._session.execute(stmt, {"landlord_id": landlord_id})
        return [dict(row._mapping) for row in result]

    async def get_revenue(self, landlord_id: uuid.UUID, period: str) -> dict:
        """Expected revenue, collected revenue, and outstanding for a period."""

        stmt = text("""
            SELECT
                COALESCE(SUM(rl.total_amount_due), 0) AS expected_revenue,
                COALESCE(SUM(rl.amount_paid), 0)      AS collected_revenue,
                COALESCE(SUM(rl.balance), 0)          AS outstanding
            FROM rent_ledger rl
            JOIN leases l ON l.id = rl.lease_id
            JOIN units u ON u.id = l.unit_id
            JOIN buildings b ON b.id = u.building_id
            WHERE b.landlord_id = :landlord_id
              AND rl.period = :period
        """)
        result = await self._session.execute(
            stmt, {"landlord_id": landlord_id, "period": period}
        )
        row = result.fetchone()
        return dict(row._mapping) if row else {}


# ledger
class LedgerRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_lease_period(
        self, lease_id: uuid.UUID, period: str
    ) -> RentLedger | None:
        """Fetch a specific month’s ledger entry for a lease"""
        result = await self._session.execute(
            select(RentLedger).where(
                RentLedger.lease_id == lease_id,
                RentLedger.period == period,
            )
        )
        return result.scalar_one_or_none()

    async def get_latest_by_lease(self, lease_id: uuid.UUID) -> RentLedger | None:
        """Return the most recent (highest period) ledger entry."""
        result = await self._session.execute(
            select(RentLedger)
            .where(RentLedger.lease_id == lease_id)
            .order_by(RentLedger.period.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def list_by_lease(self, lease_id: uuid.UUID) -> Sequence[RentLedger]:
        """Returns the full payment history for a lease,newest first"""
        result = await self._session.execute(
            select(RentLedger)
            .where(RentLedger.lease_id == lease_id)
            .order_by(RentLedger.period.desc())
        )
        return result.scalars().all()

    async def create(
        self,
        lease_id: uuid.UUID,
        period: str,
        base_rent: Decimal,
        garbage_charge: Decimal,
        previous_balance: Decimal,
        water_charge: Decimal = Decimal("0"),
    ) -> RentLedger:
        """Creates a new monthly ledger entry.
        The ledger is created at month start, water readings come later in the month
        """
        total = base_rent + garbage_charge + previous_balance
        total_charge = RentLedger(
            lease_id=lease_id,
            period=period,
            base_rent=base_rent,
            garbage_charge=garbage_charge,
            previous_balance=previous_balance,
            water_charge=Decimal("0"),
            total_amount_due=total,
            amount_paid=Decimal("0"),
            balance=total,
        )
        self._session.add(total_charge)
        await self._session.flush()
        await self._session.refresh(total_charge)
        return total_charge

    async def apply_water_charge(
        self,
        ledger_id: uuid.UUID,
        water_charge: Decimal,
        water_reading_id: uuid.UUID,
    ) -> None:
        """Update a ledger with water charges after landlord submits meter reading for that period"""
        entry = (
            await self._session.execute(
                select(RentLedger)
                .where(RentLedger.id == ledger_id)
                .with_for_update()  # row-level lock
            )
        ).scalar_one()
        new_total = (
            entry.base_rent
            + water_charge
            + entry.garbage_charge
            + entry.previous_balance
        )
        new_balance = new_total - entry.amount_paid
        status = self._compute_status(new_total, entry.amount_paid)
        await self._session.execute(
            update(RentLedger)
            .where(RentLedger.id == ledger_id)
            .values(
                water_charge=water_charge,
                total_amount_due=new_total,
                balance=new_balance,
                water_reading_id=water_reading_id,
                status=status,
            )
        )

    async def apply_payment(
        self, lease_id: uuid.UUID, period: str, amount: Decimal
    ) -> RentLedger:
        """Apply a payment to a specific period’s ledger, update status & balance
        Uses SELECT FOR UPDATE to prevent race conditions from
        concurrent M-Pesa callbacks for the same lease/period.
        """
        # lock the row for the duration of this transaction
        # prevents two concurrent M-Pesa callbacks doubling the payment
        entry = (
            await self._session.execute(
                select(RentLedger)
                .where(
                    RentLedger.lease_id == lease_id,
                    RentLedger.period == period,
                )
                .with_for_update()
            )
        ).scalar_one_or_none()
        if not entry:
            raise ValueError(
                f"No ledger entry for lease {lease_id} for period {period}"
            )
        new_paid = entry.amount_paid + amount
        new_balance = entry.total_amount_due - new_paid
        status = self._compute_status(entry.total_amount_due, new_paid)
        await self._session.execute(
            update(RentLedger)
            .where(RentLedger.id == entry.id)
            .values(amount_paid=new_paid, balance=new_balance, status=status)
        )
        await self._session.refresh(entry)
        return entry

    @staticmethod
    def _compute_status(total_due: Decimal, amount_paid: Decimal) -> LedgerStatus:
        """Internal helper to determine UNPAID, PARTIAL, PAID, or OVERPAID"""
        if amount_paid <= 0:
            return LedgerStatus.UNPAID
        if amount_paid >= total_due:
            return (
                LedgerStatus.OVERPAID if amount_paid > total_due else LedgerStatus.PAID
            )
        return LedgerStatus.PARTIAL

    async def get_unpaid_for_period(
        self, period: str
    ) -> AsyncGenerator[Sequence[RentLedger]]:
        """Return all ledger entries that are not fully paid for a given month (used for reminders)
        Streams in partitions of 100 to avoid loading thousands of rowa into memory
        """
        result = await self._session.stream(
            select(RentLedger).where(
                RentLedger.period == period,
                RentLedger.status.in_([LedgerStatus.UNPAID, LedgerStatus.PARTIAL]),
            )
        )
        async for partition in result.scalars().partitions(100):
            yield partition


# payment
class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def create(
        self,
        landlord_id: uuid.UUID,
        receipt_number: str,
        business_short_code: str,
        account_reference: str,
        msisdn: str,
        amount: Decimal,
        transaction_date: datetime,
        raw_payload: dict[str, Any],
    ) -> MpesaPayment:
        """Store an incoming M‑Pesa webhook payload"""
        record = MpesaPayment(
            landlord_id=landlord_id,
            mpesa_receipt_number=receipt_number,
            business_short_code=business_short_code,
            account_reference=account_reference,
            msisdn=msisdn,
            amount=amount,
            transaction_date=transaction_date,
            raw_payload=raw_payload,
        )
        self._session.add(record)
        await self._session.flush()
        await self._session.refresh(record)
        return record

    async def receipt_exists(self, receipt: str) -> bool:
        """Prevent duplicate processing of the same M‑Pesa transaction"""
        result = await self._session.execute(
            select(MpesaPayment.id).where(MpesaPayment.mpesa_receipt_number == receipt)
        )
        return result.scalar_one_or_none() is not None

    async def reconcile(
        self, payment_id: uuid.UUID, lease_id: uuid.UUID, period: str
    ) -> None:
        """Match a payment to a lease and a specific period (after ledger is paid)."""
        await self._session.execute(
            update(MpesaPayment)
            .where(MpesaPayment.id == payment_id)
            .values(
                matched_lease_id=lease_id, matched_period=period, is_reconciled=True
            )
        )

    async def list_by_landlord(
        self,
        landlord_id: uuid.UUID,
        reconciled: bool | None,
        page: int,
        page_size: int,
    ) -> tuple[Sequence[MpesaPayment], int]:
        """Paginated list of payments for a landlord with an optional reconciliation filter"""

        conds = [MpesaPayment.landlord_id == landlord_id]
        if reconciled is not None:
            conds.append(MpesaPayment.is_reconciled == reconciled)
        base = select(MpesaPayment).where(*conds)
        total = (
            await self._session.execute(
                select(func.count()).select_from(base.subquery())
            )
        ).scalar_one()
        items = (
            (
                await self._session.execute(
                    base.order_by(MpesaPayment.created_at.desc())
                    .offset((page - 1) * page_size)
                    .limit(page_size)
                )
            )
            .scalars()
            .all()
        )

        return items, total

    async def get_by_id(self, payment_id: uuid.UUID) -> MpesaPayment | None:
        """Fetch a payment by ID"""
        result = await self._session.execute(
            select(MpesaPayment).where(MpesaPayment.id == payment_id)
        )
        return result.scalar_one_or_none()
