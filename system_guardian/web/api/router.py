from fastapi.routing import APIRouter

from system_guardian.web.api import echo, ingest, kafka, monitoring, rabbit, vector_db, incidents

api_router = APIRouter()
api_router.include_router(monitoring.router)
api_router.include_router(echo.router, prefix="/echo", tags=["echo"])
api_router.include_router(rabbit.router, prefix="/rabbit", tags=["rabbit"])
api_router.include_router(kafka.router, prefix="/kafka", tags=["kafka"])
api_router.include_router(ingest.github.router, prefix="/ingest/github", tags=["github"])
api_router.include_router(ingest.jira.router, prefix="/ingest/jira", tags=["jira"])
api_router.include_router(vector_db.router, prefix="/vector-db", tags=["vector-db"])
api_router.include_router(incidents.router, prefix="/incidents", tags=["incidents"])