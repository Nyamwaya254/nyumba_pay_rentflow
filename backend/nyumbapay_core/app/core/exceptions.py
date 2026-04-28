"""Custom exception hierachy- Nyumbapay Core
Exception (Python built-in)
└── AppError                    ← every error in the system
    ├── DomainError             ← business/validation errors (4xx)
    │   ├── AuthError           ← 401
    │   ├── ForbiddenError      ← 403
    │   ├── NotFoundError       ← 404
    │   ├── ConflictError       ← 409
    │   └── BusinessRuleError   ← 422
    └── InfraError              ← external service failures (5xx)
        ├── PaymentServiceError ← 502 M-Pesa/Daraja
        ├── NotificationError   ← 502 SMS/WhatsApp
        └── ClerkError          ← 502 Clerk API
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class AppError(Exception):
    message: str
    status_code: int = 500
    error_code: str = "INTERNAL_ERROR"
    detail: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        return self.message

    def to_problem_detail(self) -> dict[str, Any]:
        return {
            "type": f"https://nyumbapay.co.ke/errors/{self.error_code.lower()}",
            "title": self.error_code.replace("_", " ").title(),
            "status": self.status_code,
            "detail": self.message,
            **self.detail,
        }


# Domain errors


@dataclass
class DomainError(AppError):
    """Base for all business logic errors"""

    status_code: int = 422


@dataclass
class AuthError(DomainError):
    """Raised when JWT validation fails, token is expired, or clerk_user_id not found in your DB"""

    message: str = "Authentication failed"
    status_code: int = 401
    error_code: str = "AUTH_FAILED"


@dataclass
class ForbiddenError(DomainError):
    """Raised when the user is authenticated but doesn't have permission"""

    message: str = "Access denied"
    status_code: int = 403
    error_code: str = "FORBIDDEN"


@dataclass
class NotFoundError(DomainError):
    """Raised when a requested resource doesn't exist"""

    message: str = "Resource not found"
    status_code: int = 404
    error_code: str = "NOT_FOUND"


@dataclass
class ConflictError(DomainError):
    """Raised on uniqueness violations caught before hitting the DB"""

    message: str = "Resource already exists"
    status_code: int = 409
    error_code: str = "CONFLICT"


@dataclass
class BusinessRuleError(DomainError):
    """Raised when input is technically valid but violates a domain rule"""

    message: str = "Business rule violation"
    status_code: int = 422
    error_code: str = "BUSINESS_RULE_VIOLATION"


# infrastructure errors


@dataclass
class InfraError(AppError):
    """Base for external service failures"""

    status_code: int = 502


@dataclass
class PaymentServiceError(InfraError):
    message: str = "Payment service request failed"
    error_code: str = "PAYMENT_SERVICE_ERROR"


@dataclass
class NotificationError(InfraError):
    message: str = "Notification delivery failed"
    error_code: str = "NOTIFICATION_ERROR"


@dataclass
class ClerkError(InfraError):
    message: str = "Clerk API request failed"
    error_code: str = "CLERK_ERROR"
