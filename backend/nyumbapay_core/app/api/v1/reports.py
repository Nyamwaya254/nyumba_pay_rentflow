"""Reports Router"""

from fastapi import APIRouter, Depends, Query

from app.core.dependencies import LandlordUserDep, get_report_service
from app.core.middleware import limiter
from app.schemas.validation import DefaulterResponse, OccupancyResponse, RevenueResponse
from app.services.services import ReportService

reports_router = APIRouter(prefix="/reports", tags=["reports"])


@reports_router.get("/defaulters", response_model=DefaulterResponse)
@limiter.limit("20/minute")
async def defaulters(
    _: LandlordUserDep,
    service: ReportService = Depends(get_report_service),
    period: str = Query(
        ..., pattern=r"^\d{4}-\d{2}$", description="YYYY-MM e.g. 2025-01"
    ),
) -> list[DefaulterResponse]:
    """20/minute per landlord user"""
    return await service.defaulters(period)


@reports_router.get(
    "/occupancy",
    summary="Occupancy review per building",
    response_model=list[OccupancyResponse],
)
@limiter.limit("20/minute")
async def occupancy(
    _: LandlordUserDep,
    service: ReportService = Depends(get_report_service),
) -> list[OccupancyResponse]:
    """20/minute per landlord user"""
    return await service.occupancy()


@reports_router.get("/revenue", response_model=RevenueResponse)
@limiter.limit("20/minute")
async def revenue(
    _: LandlordUserDep,
    service: ReportService = Depends(get_report_service),
    period: str = Query(..., pattern=r"^\d{4}-\d{2}$"),
) -> RevenueResponse:
    """20/minute per landlord user"""
    return await service.revenue(period)
