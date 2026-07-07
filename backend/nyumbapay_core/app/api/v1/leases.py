"""Leases Router"""

import uuid
from fastapi import APIRouter, Depends, Header, Query, status

from app.core.dependencies import LandlordUserDep, get_lease_service
from app.core.middleware import limiter
from app.schemas.validation import (
    CreateLeaseRequest,
    LeaseResponse,
    LedgerEntryResponse,
)
from app.services.services import LeaseService


leases_router = APIRouter(prefix="/units/{unit_id}/leases", tags=["leases"])


@leases_router.post(
    "", status_code=status.HTTP_201_CREATED, response_model=LeaseResponse
)
@limiter.limit("10/minute")
async def create_lease(
    unit_id: uuid.UUID,
    request: CreateLeaseRequest,
    _: LandlordUserDep,
    service: LeaseService = Depends(get_lease_service),
    idempotency_key: str | None = Header(None, alias="Idempotency-key"),
) -> LeaseResponse:
    """Create the lease,marks unit as occupied, sends initial water reading"""
    return await service.create(unit_id, request, idempotency_key=idempotency_key)


standalone_leases_router = APIRouter(prefix="/leases", tags=["leases"])


@standalone_leases_router.delete(
    "/{lease_id}", status_code=status.HTTP_204_NO_CONTENT, summary="Terminate a lease"
)
@limiter.limit("5/minute")
async def terminate_lease(
    lease_id: uuid.UUID,
    _: LandlordUserDep,
    service: LeaseService = Depends(get_lease_service),
) -> None:
    """Terminate an active lease, set unit back to VACANT endpoint"""
    await service.terminate(lease_id)


@standalone_leases_router.get(
    "/{lease_id}/ledger",
    response_model=list[LedgerEntryResponse],
    summary="Full ledger history for a lease",
)
@limiter.limit("60/minute")
async def get_ledger(
    lease_id: uuid.UUID,
    _: LandlordUserDep,
    service: LeaseService = Depends(get_lease_service),
    page: int = Query(1, ge=1),
    page_size: int = Query(12, ge=1, le=60),
) -> list[LedgerEntryResponse]:
    """60/minute per landlord user"""
    return await service.get_ledger(
        lease_id,
        page,
        page_size,
    )
