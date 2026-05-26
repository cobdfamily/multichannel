"""API routers."""

from fastapi import APIRouter

from multichannel.api.v1 import router as v1_router

router = APIRouter()
router.include_router(v1_router, prefix="/api/v1")

__all__ = ["router"]
