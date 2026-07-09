"""Buildings router"""

import uuid
from fastapi import APIRouter, Depends, Query, Request, status

from app.core.middleware import limiter
from app.schemas.validation import (
    BuildingDetailResponse,
    CreateBuildingRequest,
    PaginatedResponse,
)
from app.core.dependencies import LandlordUserDep, get_building_service
from app.services.services import BuildingService


buildings_router = APIRouter(prefix="/buildings", tags=["buildings"])


@buildings_router.post(
    "",
    response_model=BuildingDetailResponse,
    status_code=status.HTTP_201_CREATED,
)
@limiter.limit("20/minute")
async def create_building(
    request: Request,
    payload: CreateBuildingRequest,
    _: LandlordUserDep,
    service: BuildingService = Depends(get_building_service),
) -> BuildingDetailResponse:
    """Create building with charge config(water rate + garbage)
    20/minute per landlord user
    """
    return await service.create(payload)


@buildings_router.get(
    "",
    response_model=PaginatedResponse,
    summary="List landlord's buildings (paginated)",
)
@limiter.limit("100/minute")
async def list_buildings(
    request: Request,
    _: LandlordUserDep,
    service: BuildingService = Depends(get_building_service),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> PaginatedResponse:
    """100/minute per landlord user"""
    return await service.list(page, page_size)


@buildings_router.get(
    "/{building_id}",
    response_model=BuildingDetailResponse,
    summary="Get building detail",
)
@limiter.limit("120/minute")
async def get_building(
    request: Request,
    building_id: uuid.UUID,
    _: LandlordUserDep,
    service: BuildingService = Depends(get_building_service),
) -> BuildingDetailResponse:
    return await service.get(building_id)
