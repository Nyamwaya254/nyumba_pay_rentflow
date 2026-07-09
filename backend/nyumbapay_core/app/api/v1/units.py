"""Units Router"""

import uuid
from fastapi import APIRouter, Depends, Query, Request, status

from app.core.dependencies import LandlordUserDep, get_unit_service
from app.core.middleware import limiter
from app.models.enums import UnitStatus
from app.schemas.validation import (
    CreateUnitRequest,
    PaginatedResponse,
    UpdateUnitRequest,
)
from app.services.services import UnitService

units_router = APIRouter(prefix="/buildings/{building_id}/units", tags=["units"])
"""Used for building-scoped operations"""


@units_router.post("", status_code=status.HTTP_201_CREATED)
@limiter.limit("30/minute")
async def create_unit(
    request: Request,
    building_id: uuid.UUID,
    payload: CreateUnitRequest,
    _: LandlordUserDep,
    service: UnitService = Depends(get_unit_service),
):
    """Each unit has its own rent_amount.
    A1 can be 45000,b2 can be 20000 - set individually
    """
    return await service.create(building_id, payload)


@units_router.get("")
@limiter.limit("100/minute")
async def list_units(
    request: Request,
    building_id: uuid.UUID,
    _: LandlordUserDep,
    service: UnitService = Depends(get_unit_service),
    unit_status: UnitStatus | None = Query(None, alias="status"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
) -> PaginatedResponse:
    """List of all units under a building with optional filter using status"""
    return await service.list(building_id, page, page_size, unit_status)


standalone_units_router = APIRouter(prefix="/units", tags=["units"])
"""Standalone  units router for direct unit access by ID"""


@standalone_units_router.get("/{unit_id}", summary="Get unit detil")
@limiter.limit("120/minute")
async def get_unit(
    request: Request,
    unit_id: uuid.UUID,
    _: LandlordUserDep,
    service: UnitService = Depends(get_unit_service),
):
    """Get unit by its ID endpoint"""
    return await service.get(unit_id)


@standalone_units_router.patch("/{unit_id}", summary="Update unit rent amount")
@limiter.limit("30/minute")
async def update_unit(
    request: Request,
    unit_id: uuid.UUID,
    payload: UpdateUnitRequest,
    _: LandlordUserDep,
    service: UnitService = Depends(get_unit_service),
):
    """Update unit rent amount endpoint"""
    return await service.update(unit_id, payload)
