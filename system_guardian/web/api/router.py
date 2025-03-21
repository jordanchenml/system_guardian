from fastapi.routing import APIRouter

from system_guardian.web.api import echo, ingest, kafka, monitoring, rabbit

api_router = APIRouter()
api_router.include_router(monitoring.router)
api_router.include_router(echo.router, prefix="/echo", tags=["echo"])
api_router.include_router(ingest.router, prefix="/ingest", tags=["ingest"])
api_router.include_router(rabbit.router, prefix="/rabbit", tags=["rabbit"])
api_router.include_router(kafka.router, prefix="/kafka", tags=["kafka"])
