# auth

from fastapi import APIRouter, Request

from app.core.dependencies import CurrentUserDep
from app.schemas.validation import UserResponse
from app.core.middleware import limiter


auth_router = APIRouter(prefix="/auth", tags=["auth"])


@auth_router.get(
    "/me",
    response_model=UserResponse,
    summary="Get Current user",
)
@limiter.limit("120/minute")
async def get_me(request: Request, current_user: CurrentUserDep) -> UserResponse:
    """120/minute per user"""
    return UserResponse.model_validate(current_user)
