"""Clerk Webhook Router"""

from fastapi import APIRouter, HTTPException, Header, Request, status
import structlog
from svix import Webhook

from app.core.dependencies import DbSessionDep
from app.core.config import get_settings
from app.repositories.user_repo import UserRepository
from app.core.middleware import limiter

logger = structlog.get_logger(__name__)
clerk_router = APIRouter(prefix="/webhooks/clerk", tags=["webhooks"])


@clerk_router.post(
    "",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Clerk webhook events",
    include_in_schema=False,
)
@limiter.limit(
    "100/minute",
    key_func=lambda req: f"ip:{req.client.host}" if req.client else "ip:unknown",
)
async def clerk_webhook(
    req: Request,
    db: DbSessionDep,
    svix_id: str = Header(..., alias="svix_id"),
    svix_timestamp: str = Header(..., alias="svix_timestamp"),
    svix_signature: str = Header(..., alias="svix_signature"),
):
    """100/minute per IP, keyed by IP not user."""
    cfg = get_settings()
    body = await req.body()

    try:
        wh = Webhook(cfg.clerk_webhook_secret)
        event = wh.verify(
            body,
            {
                "svix_id": svix_id,
                "svix_timestamp": svix_timestamp,
                "svix_signature": svix_signature,
            },
        )
    except Exception as exc:
        logger.warning("clerk_webhook_verification_failed", error=str(exc))
        raise HTTPException(status_code=400)

    event_type = event.get("type")
    logger.info("clerk_webhook_received", event_type=event_type)

    if event_type == "user.deleted":
        clerk_user_id = event.get("data", {}).get("id")
        if clerk_user_id:
            repo = UserRepository(db)
            user = await repo.get_by_clerk_id(clerk_user_id)
            if user:
                await repo.deactivate(user.id)
                logger.info(
                    "user_deactivated_via_clerk_webhook", clerk_user_id=clerk_user_id
                )
    else:
        logger.info("clerk_webhook_unhandled_event", event_type=event_type)
