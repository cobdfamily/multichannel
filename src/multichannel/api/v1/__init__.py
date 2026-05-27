"""v1 routers."""

from fastapi import APIRouter

from multichannel.api.v1 import delivery_status, messages, outbound, webhooks

router = APIRouter()
router.include_router(outbound.router)
router.include_router(webhooks.router)
router.include_router(delivery_status.router)
router.include_router(messages.router)

__all__ = ["router"]
