"""ORM models - NyumbaPAy Core
All tables with indexes as per the locked schema.
Relationships are defined with lazy="raise" to prevent
accidental N+1 — all joins must be explicit via selectinload/joinedload.
"""

from datetime import date, datetime
from decimal import Decimal
import uuid
from sqlalchemy import (
    Boolean,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship, validates
from sqlalchemy.dialects.postgresql import UUID, JSONB

from nyumbapay_core.app.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin
from nyumbapay_core.app.models.enums import (
    DepositStatus,
    LeaseStatus,
    LedgerStatus,
    NotificationChannel,
    NotificationStatus,
    NotificationType,
    UnitStatus,
    UserRole,
)


class User(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    """Authentication principal.Landlord can have one user account"""

    __tablename__ = "users"

    clerk_user_id: Mapped[str] = mapped_column(
        String(255),
        unique=True,
        nullable=False,
        comment="Clerk user ID e.g. user_2abc... — the auth identity",
    )
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)

    @validates("email")
    def _normalize_email(self, key: str, value: str) -> str:
        return value.lower().strip()

    password_hash: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )
    role: Mapped[UserRole] = mapped_column(
        String(20), nullable=False, default=UserRole.LANDLORD
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
    )
    landlord: Mapped["Landlord | None"] = relationship(
        "Landlord",
        back_populates="user",
        lazy="raise",
        uselist=False,
    )

    __table_args__ = (
        Index("ix_users_email", "email", unique=True),
        Index("ix_users_clerk_user_id", "clerk_user_id", unique=True),
    )


class Landlord(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    """Landlord profile - one per user"""

    __tablename__ = "landlords"

    user_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="RESTRICT"),
        unique=True,
        nullable=False,
    )
    full_name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )
    phone: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
    )
    business_name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )
    paybill_number: Mapped[str] = mapped_column(
        String(20),
        unique=True,
        nullable=False,
        comment="Safaricom Paybill - unique per landlord",
    )
    is_active: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
    )
    user: Mapped["User"] = relationship(
        "User",
        back_populates="landlord",
        lazy="raise",
    )
    buildings: Mapped[list["Building"]] = relationship(
        "Building", back_populates="landlord", lazy="raise"
    )

    __table_args__ = (
        Index("ix_landlords_user_id", "user_id"),
        Index("ix_landlords_paybill", "paybill_number", unique=True),
        Index("ix_landlords_phone", "phone"),
    )


class Building(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    """A building owned by a landlord"""

    __tablename__ = "buildings"

    landlord_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("landlords.id", ondelete="RESTRICT"),
        nullable=False,
    )
    name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )
    address: Mapped[str] = mapped_column(
        String(500),
        nullable=False,
    )
    city: Mapped[str] = mapped_column(String(100), nullable=False)
    code: Mapped[str] = mapped_column(
        String(10),
        nullable=False,
        comment="Short code used in MPESA account reference e.g PALM",
    )
    landlord: Mapped["Landlord"] = relationship(
        "Landlord", back_populates="buildings", lazy="raise"
    )
    units: Mapped[list["Unit"]] = relationship(
        "Unit", back_populates="building", lazy="raise"
    )
    charge_configs: Mapped[list["BuildingChargeConfig"]] = relationship(
        "BuildingChargeConfig",
        back_populates="building",
        order_by="BuildingChargeConfig.effective_from.desc()",
        lazy="raise",
    )

    __table_args__ = (
        Index("ix_buildings_landlord_id", "landlord_id"),
        UniqueConstraint("landlord_id", "code", name="uq_buildings_landlord_code"),
    )


class BuildingChargeConfig(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    """Per Building charge configuration - append only for audit history"""

    __tablename__ = "building_charge_configs"

    building_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("buildings.id", ondelete="RESTRICT"),
        nullable=False,
    )
    garbage_charge: Mapped[Decimal] = mapped_column(
        Numeric(19, 4),
        nullable=False,
        default=Decimal("500.00"),
    )
    water_rate_per_unit: Mapped[Decimal] = mapped_column(
        Numeric(19, 4),
        nullable=False,
        comment="KES per cubic metre consumed",
    )
    effective_from: Mapped[date] = mapped_column(
        Date,
        nullable=False,
    )
    building: Mapped["Building"] = relationship(
        "Building", back_populates="charge_configs", lazy="raise"
    )

    __table_args__ = (
        Index("ix_building_charge_configs_building_id", "building_id"),
        Index("ix_building_charge_configs_effective", "building_id", "effective_from"),
    )


class Unit(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    """A rentable unit within a building"""

    __tablename__ = "units"

    building_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("buildings.id", ondelete="RESTRICT"),
        nullable=False,
    )
    unit_number: Mapped[str] = mapped_column(
        String(20), nullable=False, comment="e.g A1,B1 - unique within building"
    )
    floor: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rent_amount: Mapped[Decimal] = mapped_column(
        Numeric(19, 4),
        nullable=False,
        comment="Current listed rent - may differ from lease.rent_amount (snapshot)",
    )
    status: Mapped[UnitStatus] = mapped_column(
        String(20), nullable=False, default=UnitStatus.VACANT
    )
    building: Mapped["Building"] = relationship(
        "Building", back_populates="units", lazy="raise"
    )
    leases: Mapped[list["Lease"]] = relationship(
        "Lease", back_populates="unit", lazy="raise"
    )

    __table_args__ = (
        Index("ix_units_building_id", "building_id"),
        Index("ix_units_building_status", "building_id", "status"),
        UniqueConstraint("building_id", "unit_number", name="uq_units_building_number"),
    )


class Tenant(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    """A tenant - scoped to a landlord"""

    __tablename__ = "tenants"

    landlord_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("landlords.id", ondelete="RESTRICT"),
        nullable=False,
    )
    full_name: Mapped[str] = mapped_column(
        String(255),
        nullable=False,
    )
    phone: Mapped[str] = mapped_column(String(20), nullable=False)
    national_id: Mapped[str] = mapped_column(String(30), nullable=False)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    leases: Mapped[list["Lease"]] = relationship(
        "Lease", back_populates="tenant", lazy="raise"
    )

    __table_args__ = (
        Index("ix_tenants_landlord_id", "landlord_id"),
        Index("ix_tenants_phone", "phone"),
        Index("ix_tenants_national_id", "national_id"),
        UniqueConstraint(
            "landlord_id", "national_id", name="uq_tenants_landlord_national_id"
        ),
    )


class Lease(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    """Active assignment of a tenant to a unit
    account_reference is the M-Pesa payment key — format: {BUILDING_CODE}-{UNIT_NUMBER}
    e.g. PALM-A3. Set on creation, never changed.
    """

    __tablename__ = "leases"

    unit_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("units.id", ondelete="RESTRICT"), nullable=False
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
    )
    rent_amount: Mapped[Decimal] = mapped_column(
        Numeric(19, 4),
        nullable=False,
        comment="Snapshot of unit.rent_amount at lease signing",
    )
    deposit_amount: Mapped[Decimal] = mapped_column(
        Numeric(19, 4),
        nullable=False,
    )
    deposit_status: Mapped[DepositStatus] = mapped_column(
        String(20),
        nullable=False,
        default=DepositStatus.HELD,
    )
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    end_date: Mapped[date | None] = mapped_column(
        Date,
        nullable=True,
        comment="Null = month-to-month",
    )
    account_reference: Mapped[str] = mapped_column(
        String(30),
        unique=True,
        nullable=False,
        comment="M-PESA account reference e.g PALM-A3 ",
    )

    @validates("account_reference")
    def _normalize_account_reference(self, key: str, value: str) -> str:
        return value.upper().strip()

    status: Mapped[LeaseStatus] = mapped_column(
        String(20),
        nullable=False,
        default=LeaseStatus.ACTIVE,
    )
    terminated_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    unit: Mapped["Unit"] = relationship("Unit", back_populates="leases", lazy="raise")
    tenant: Mapped["Tenant"] = relationship(
        "Tenant", back_populates="leases", lazy="raise"
    )
    ledger_entries: Mapped[list["RentLedger"]] = relationship(
        "RentLedger", back_populates="lease", lazy="raise"
    )
    water_readings: Mapped[list["WaterReading"]] = relationship(
        "WaterReading", back_populates="lease", lazy="raise"
    )

    __table_args__ = (
        Index("ix_leases_unit_id", "unit_id"),
        Index("ix_leases_tenant_id", "tenant_id"),
        Index("ix_leases_unit_status", "unit_id", "status"),
        Index("ix_leases_account_reference", "account_reference", unique=True),
        # Only one active lease per unit at a time
        Index(
            "uq_leases_one_active_per_unit",
            "unit_id",
            unique=True,
            postgresql_where="status = 'active'",
        ),
    )


class RentLedger(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    """Montly billing record per lease
    One row per lease per period. Generated on the 1st by Celery.
    Water charge starts at 0 and is updated when landlord enters readings.
    balance = previous_balance + total_amount_due - amount_paid
    """

    __tablename__ = "rent_ledger"

    lease_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("leases.id", ondelete="RESTRICT"),
        nullable=False,
    )
    period: Mapped[str] = mapped_column(
        String(7),
        nullable=False,
        comment="YYYY_MM e.g 2025-01",
    )
    base_rent: Mapped[Decimal] = mapped_column(
        Numeric(19, 4), nullable=False, comment="Snapshot from lease.rent_amount"
    )
    water_charge: Mapped[Decimal] = mapped_column(
        Numeric(19, 4),
        nullable=False,
        default=Decimal("0.0"),
        comment="0 until landlord enters the water reading",
    )
    garbage_charge: Mapped[Decimal] = mapped_column(
        Numeric(19, 4), nullable=False, comment="Snapshot from building_charge_configs"
    )
    previous_balance: Mapped[Decimal] = mapped_column(
        Numeric(19, 4),
        nullable=False,
        default=Decimal("0.00"),
        comment="Carry forward from prior period (positive = owes)",
    )
    total_amount_due: Mapped[Decimal] = mapped_column(
        Numeric(19, 4),
        nullable=False,
        comment="base_rent + water_charge + garbage_charge + previous_balance",
    )
    amount_paid: Mapped[Decimal] = mapped_column(
        Numeric(19, 4),
        nullable=False,
        default=Decimal("0.00"),
    )
    balance: Mapped[Decimal] = mapped_column(
        Numeric(19, 4),
        nullable=False,
        comment="total_amount_due - amount_paid (positive = still owes)",
    )
    water_reading_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("water_readings.id", ondelete="SET NULL"),
        nullable=True,
    )
    status: Mapped[LedgerStatus] = mapped_column(
        String(20), nullable=False, default=LedgerStatus.UNPAID
    )
    lease: Mapped["Lease"] = relationship(
        "Lease", back_populates="ledger_entries", lazy="raise"
    )
    water_reading: Mapped["WaterReading | None"] = relationship(
        "WaterReading", foreign_keys=[water_reading_id], lazy="raise"
    )

    __table_args__ = (
        Index("ix_rent_ledger_lease_id", "lease_id"),
        Index("ix_rent_ledger_period_status", "period", "status"),
        UniqueConstraint("lease_id", "period", name="uq_rent_ledger_lease_period"),
    )


class WaterReading(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    """Montly water meter reading per unit
    previous_reading auto-populated from last period's current_reading.
    First reading for a new tenant requires manual entry of both values.
    """

    __tablename__ = "water_readings"

    unit_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("units.id", ondelete="RESTRICT"),
        nullable=False,
    )
    lease_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("leases.id", ondelete="RESTRICT"),
        nullable=False,
    )
    period: Mapped[str] = mapped_column(String(7), nullable=False)
    previous_reading: Mapped[Decimal] = mapped_column(
        Numeric(19, 4),
        nullable=False,
        comment="Auto-populated from last period current_reading",
    )
    current_reading: Mapped[Decimal] = mapped_column(
        Numeric(19, 4),
        nullable=False,
        comment="Entered by landlord",
    )
    units_consumed: Mapped[Decimal] = mapped_column(
        Numeric(19, 4),
        nullable=False,
        comment="current_reading - previous_reading",
    )
    rate_per_unit: Mapped[Decimal] = mapped_column(
        Numeric(19, 4),
        nullable=False,
        comment="Snapshot of building_charge_configs.water_rate_per_unit",
    )
    water_charge: Mapped[Decimal] = mapped_column(
        Numeric(19, 4), nullable=False, comment="units_consumed * rate_per_unit"
    )
    entered_by: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("users.id", ondelete="RESTRICT"),
        nullable=False,
    )
    entered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    lease: Mapped["Lease"] = relationship(
        "Lease", back_populates="water_readings", lazy="raise"
    )

    __table_args__ = (
        Index("ix_water_readings_unit_id", "unit_id"),
        Index("ix_water_readings_lease_id", "lease_id"),
        UniqueConstraint("unit_id", "period", name="uq_water_readings_unit_period"),
    )


class MpesaPayment(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    """Reconciled M-Pesa payment record in the core DB.

    Populated by the Celery consumer that reads from Redis Pub/Sub.
    Unmatched payments (unknown account_reference) are stored with
    matched_lease_id=None for manual resolution by landlord.
    """

    __tablename__ = "mpesa_payments"

    landlord_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("landlords.id", ondelete="RESTRICT"),
        nullable=False,
    )
    mpesa_receipt_number: Mapped[str] = mapped_column(
        String(30), unique=True, nullable=False
    )
    business_short_code: Mapped[str] = mapped_column(String(20), nullable=False)
    account_reference: Mapped[str] = mapped_column(String(30), nullable=False)
    msisdn: Mapped[str] = mapped_column(String(15), nullable=False)
    amount: Mapped[Decimal] = mapped_column(Numeric(19, 4), nullable=False)
    transaction_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    matched_lease_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("leases.id", ondelete="SET NULL"), nullable=True
    )
    matched_period: Mapped[str | None] = mapped_column(String(7), nullable=True)
    is_reconciled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    raw_payload: Mapped[dict] = mapped_column(JSONB, nullable=False)

    __table_args__ = (
        Index("ix_mpesa_payments_landlord_id", "landlord_id"),
        Index("ix_mpesa_payments_landlord_reconciled", "landlord_id", "is_reconciled"),
        Index("ix_mpesa_payments_landlord_created", "landlord_id", "created_at"),
        UniqueConstraint("mpesa_receipt_number", name="uq_mpesa_receipt"),
    )


class NotificationLog(Base, UUIDPrimaryKeyMixin, TimestampMixin):
    """Audit log of every notification sent or attempted."""

    __tablename__ = "notification_logs"

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="RESTRICT"),
        nullable=False,
    )
    lease_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("leases.id", ondelete="RESTRICT"), nullable=False
    )
    channel: Mapped[NotificationChannel] = mapped_column(String(20), nullable=False)
    type: Mapped[NotificationType] = mapped_column(String(30), nullable=False)
    period: Mapped[str] = mapped_column(String(7), nullable=False)
    status: Mapped[NotificationStatus] = mapped_column(
        String(20), nullable=False, default=NotificationStatus.PENDING
    )
    provider_message_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    error_detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    sent_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    __table_args__ = (
        Index("ix_notification_logs_tenant_id", "tenant_id"),
        Index("ix_notification_logs_lease_period", "lease_id", "period", "type"),
        Index("ix_notification_logs_sent_at", "sent_at"),
    )
