"""Water Readings Router"""

import uuid
from fastapi import APIRouter, Depends, Request, status

from app.core.dependencies import LandlordUserDep, get_water_reading_service
from app.core.middleware import limiter
from app.schemas.validation import CreateWaterReadingsRequest, WaterReadingsResponse
from app.services.services import WaterReadingService


water_readings_router = APIRouter(
    prefix="units/{unit_id}/water_readings", tags=["water-readings"]
)


@water_readings_router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=WaterReadingsResponse,
    summary="Enter montly water reading for a unit",
)
@limiter.limit("30/minute")
async def enter_water_reading(
    request: Request,
    unit_id: uuid.UUID,
    payload: CreateWaterReadingsRequest,
    _: LandlordUserDep,
    service: WaterReadingService = Depends(get_water_reading_service),
) -> WaterReadingsResponse:
    """Landlord enters current_reading. Previous reading is auto-populated
    from last period. System computes units_consumed × rate and updates
    the rent_ledger entry for this period."""

    return await service.enter_reading(unit_id, payload)
