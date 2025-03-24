"""Qdrant dependencies module."""
from typing import AsyncGenerator

from fastapi import Depends

from system_guardian.services.vector_db.qdrant_client import QdrantClient, get_qdrant_client


async def get_qdrant_dependency(
    qdrant_client: QdrantClient = Depends(get_qdrant_client),
) -> AsyncGenerator[QdrantClient, None]:
    """
    Get Qdrant client for FastAPI dependency injection.

    :param qdrant_client: Qdrant client instance
    :yields: QdrantClient instance
    """
    try:
        yield qdrant_client
    finally:
        # No cleanup needed, as client is a singleton managed by the lru_cache
        pass 