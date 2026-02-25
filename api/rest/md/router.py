from fastapi import APIRouter

from .identity import router as identity_router
from .portfolio import router as portfolio_router
from .orders import router as orders_router
from .trades import router as trades_router
from .securities import router as securities_router

router = APIRouter()
router.include_router(identity_router)
router.include_router(portfolio_router)
router.include_router(orders_router)
router.include_router(trades_router)
router.include_router(securities_router)
