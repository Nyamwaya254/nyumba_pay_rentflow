"""
Models package.

This module ensures all ORM models are imported so that
SQLAlchemy metadata is fully populated for Alembic autogeneration.
"""

from app.models.base import Base

# Import ALL models so they register with Base.metadata
from app.models.models import (
    User,
    Landlord,
    Building,
    BuildingChargeConfig,
    Unit,
    Tenant,
    Lease,
    RentLedger,
    WaterReading,
    MpesaPayment,
    NotificationLog,
)

__all__ = [
    "Base",
    "User",
    "Landlord",
    "Building",
    "BuildingChargeConfig",
    "Unit",
    "Tenant",
    "Lease",
    "RentLedger",
    "WaterReading",
    "MpesaPayment",
    "NotificationLog",
]
