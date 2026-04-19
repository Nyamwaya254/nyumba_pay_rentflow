"""Shared enums for ORM models and pydantic schemas"""

import enum


class UserRole(str, enum.Enum):
    SUPER_ADMIN = "super_admin"
    LANDLORD = "landlord"


class UnitStatus(str, enum.Enum):
    VACANT = "vacant"
    OCCUPIED = "occupied"


class LeaseStatus(str, enum.Enum):
    ACTIVE = "active"
    TERMINATED = "terminated"


class DepositStatus(str, enum.Enum):
    HELD = "held"
    REFUNDED = "refunded"


class LedgerStatus(str, enum.Enum):
    UNPAID = "unpaid"
    PARTIAL = "partial"
    PAID = "paid"
    OVERPAID = "overpaid"


class NotificationChannel(str, enum.Enum):
    SMS = "sms"
    WHATSAPP = "whatsapp"


class NotificationType(str, enum.Enum):
    REMINDER_28 = "reminder_28"
    REMINDER_1ST = "reminder_1st"
    REMINDER_5TH = "reminder_5th"
    PAYMENT_CONFIRMATION = "payment_confirmation"
    WELCOME = "welcome"


class NotificationStatus(str, enum.Enum):
    SENT = "sent"
    FAILED = "failed"
    PENDING = "pending"
