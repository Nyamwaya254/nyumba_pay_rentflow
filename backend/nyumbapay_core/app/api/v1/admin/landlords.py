"""Admin Router"""

import uuid
from fastapi import APIRouter, Depends, Query, Response, status
import structlog

from app.core.dependencies import SuperAdminDep, get_landlord_service
from app.schemas.validation import (
    CreateLandlordRequest,
    LandlordListResponse,
    LandlordResponse,
    UpdateLandlordRequest,
)
from app.services.services import LandlordService
from app.core.middleware import limiter


admin_router = APIRouter(prefix="/admin", tags=["admin"])
logger = structlog.get_logger(__name__)


@admin_router.post(
    "/landlords", response_model=LandlordResponse, status_code=status.HTTP_201_CREATED
)
@limiter.limit("10/minute")
async def create_landlord(
    request: CreateLandlordRequest,
    _: SuperAdminDep,
    http_response: Response,
    service: LandlordService = Depends(get_landlord_service),
) -> LandlordResponse:
    """10/minute per super-admin user"""
    result, c2b_registered = await service.create_landlord(request)
    if not c2b_registered:
        http_response.headers["warning"] = (
            "299 nyumbapay"
            "Daraja C2B registration pending — retry via /retry-registration"
        )
        logger.warning("landlord_created_c2b_pending", landlord_id=str(result.id))
    return result


@admin_router.get(
    "/landlords", response_model=LandlordListResponse, summary="List all landlords"
)
@limiter.limit("60/minute")
async def list_landlords(
    _: SuperAdminDep,
    service: LandlordService = Depends(get_landlord_service),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> LandlordListResponse:
    """60/minute per super-admin user"""
    return await service.list_landlords(page, page_size)


@admin_router.get(
    "/landlords/{landlord_id}",
    response_model=LandlordResponse,
    summary="Get landlord detail",
)
@limiter.limit("120/minute")
async def get_landlord(
    landlord_id: uuid.UUID,
    _: SuperAdminDep,
    service: LandlordService = Depends(get_landlord_service),
) -> LandlordResponse:
    """120/minute per super-admin user"""
    return await service.get_landlord(landlord_id)


@admin_router.patch(
    "/landlords/{landlord_id}",
    response_model=LandlordResponse,
    summary="Update landlord",
)
@limiter.limit("20/minute")
async def update_landlord(
    landlord_id: uuid.UUID,
    request: UpdateLandlordRequest,
    _: SuperAdminDep,
    service: LandlordService = Depends(get_landlord_service),
) -> LandlordResponse:
    """20/minute per super-admin user"""
    return await service.update_landlord(landlord_id, request)


@admin_router.delete(
    "/landlords/{landlord_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Deactivate landlord",
)
@limiter.limit("5/minute")
async def deactivate_landlord(
    landlord_id: uuid.UUID,
    _: SuperAdminDep,
    service: LandlordService = Depends(get_landlord_service),
) -> None:
    await service.deactivate_landlord(landlord_id)


@admin_router.patch(
    "/landlords/{landlord_id}/retry-registration",
    summary="Retry Daraja C2B registration",
)
@limiter.limit("5/minute")
async def retry_registration(
    landlord_id: uuid.UUID,
    _: SuperAdminDep,
    service: LandlordService = Depends(get_landlord_service),
) -> dict:
    return await service.retry_registration(landlord_id)
