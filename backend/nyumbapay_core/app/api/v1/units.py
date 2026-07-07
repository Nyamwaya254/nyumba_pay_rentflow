"""Units Router"""

import uuid
from fastapi import APIRouter, Depends, Query, status

from app.core.dependencies import LandlordUserDep, get_unit_service
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
async def create_unit(
    building_id: uuid.UUID,
    request: CreateUnitRequest,
    _: LandlordUserDep,
    service: UnitService = Depends(get_unit_service),
):
    """Each unit has its own rent_amount.
    A1 can be 45000,b2 can be 20000 - set individually
    """
    return await service.create(building_id, request)


@units_router.get("")
async def list_units(
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
async def get_unit(
    unit_id: uuid.UUID,
    _: LandlordUserDep,
    service: UnitService = Depends(get_unit_service),
):
    """Get unit by its ID endpoint"""
    return await service.get(unit_id)


@standalone_units_router.patch("/{unit_id}", summary="Update unit rent amount")
async def update_unit(
    unit_id: uuid.UUID,
    request: UpdateUnitRequest,
    _: LandlordUserDep,
    service: UnitService = Depends(get_unit_service),
):
    """Update unit rent amount endpoint"""
    return await service.update(unit_id, request)
