"""Payments Router"""

import uuid
from fastapi import APIRouter, Depends, Header, Query, Request

from app.core.dependencies import LandlordUserDep, get_reconciliation_service
from app.core.middleware import limiter
from app.schemas.validation import (
    ManualMatchRequest,
    PaymentListResponse,
    PaymentResponse,
)
from app.services.services import ReconciliationService

payments_router = APIRouter(prefix="/payments", tags=["payments"])


@payments_router.get(
    "", response_model=PaymentListResponse, summary="All M-Pesa payments(paginated)"
)
@limiter.limit("30/minute")
async def list_payments(
    request: Request,
    _: LandlordUserDep,
    service: ReconciliationService = Depends(get_reconciliation_service),
    reconciled: bool | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> PaymentListResponse:
    """Filter by reconciled=true (matched) or reconciled=false (unmatched)"""
    return await service.list_payments(reconciled, page, page_size)


@payments_router.post(
    "/{payment_id}/match",
    summary="Manually match unmacthed payment to a lease",
    response_model=PaymentResponse,
)
@limiter.limit("20/minute")
async def manual_match(
    request: Request,
    payment_id: uuid.UUID,
    payload: ManualMatchRequest,
    _: LandlordUserDep,
    service: ReconciliationService = Depends(get_reconciliation_service),
    idempotency_key: str | None = Header(None, alias="idempotency_key"),
):
    "20/minute per landlord user"
    return await service.manual_match(payment_id, payload, idempotency_key)
