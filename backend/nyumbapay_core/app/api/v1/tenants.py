"""Tenants Router"""

import uuid
from fastapi import APIRouter, Depends, Query, status

from app.core.dependencies import LandlordUserDep, get_tenant_service
from app.core.middleware import limiter
from app.schemas.validation import (
    CreateTenantRequest,
    PaginatedResponse,
    TenantResponse,
)
from app.services.services import TenantService


tenants_router = APIRouter(prefix="/tenants", tags=["tenants"])


@tenants_router.post(
    "",
    response_model=TenantResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new tenant",
)
@limiter.limit("20/minute")
async def create_tenant(
    request: CreateTenantRequest,
    _: LandlordUserDep,
    service: TenantService = Depends(get_tenant_service),
) -> TenantResponse:
    """20/minute per landlord user"""
    return await service.create(request)


@tenants_router.get("", summary="List all tenants(paginated)")
@limiter.limit("100/minute")
async def list_tenants(
    _: LandlordUserDep,
    service: TenantService = Depends(get_tenant_service),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
) -> PaginatedResponse:
    """100/minute per landlord user. Paginated read"""
    return await service.list(page, page_size)


@tenants_router.get("/{tenant_id}", summary="Get tenant detail")
@limiter.limit("120/minute")
async def get_tenant(
    tenant_id: uuid.UUID,
    _: LandlordUserDep,
    service: TenantService = Depends(get_tenant_service),
):
    """120/minute per landlord user. Primary key lookup"""
    return await service.get(tenant_id)
