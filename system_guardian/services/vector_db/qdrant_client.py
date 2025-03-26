"""Qdrant vector database client implementation."""
import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional, Union, Tuple
from functools import lru_cache

import numpy as np
from loguru import logger
from pydantic import BaseModel, Field
from qdrant_client import QdrantClient as QdrantBaseClient
from qdrant_client.http import models as qdrant_models
from qdrant_client.http.exceptions import UnexpectedResponse
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from system_guardian.settings import settings


class VectorRecord(BaseModel):
    """Vector record with metadata."""

    id: str
    vector: List[float]
    metadata: Dict[str, Any]
    score: Optional[float] = None
    created_at: datetime = Field(default_factory=datetime.utcnow)


class QdrantClient:
    """Client for Qdrant vector database."""

    def __init__(
        self,
        host: str = settings.qdrant_host,
        port: int = settings.qdrant_port,
        api_key: Optional[str] = settings.qdrant_api_key,
        timeout: int = settings.qdrant_timeout,
    ):
        """
        Initialize Qdrant client.

        :param host: Qdrant server host
        :param port: Qdrant server port
        :param api_key: API key for authentication
        :param timeout: Request timeout in seconds
        """
        self.client = QdrantBaseClient(
            host=host,
            port=port,
            api_key=api_key,
            timeout=timeout,
            check_compatibility=False
        )
        logger.info(f"Initialized Qdrant client: {host}:{port}")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(
            (ConnectionError, TimeoutError, UnexpectedResponse)
        ),
    )
    async def create_collection(
        self,
        collection_name: str,
        vector_size: int,
        distance: str = "Cosine",
        recreate: bool = False,
    ) -> bool:
        """
        Create a new collection or recreate an existing one.

        :param collection_name: Name of the collection
        :param vector_size: Dimension of vectors to store
        :param distance: Distance function to use (Cosine, Euclid, Dot)
        :param recreate: Whether to recreate the collection if it exists
        :returns: True if the operation was successful
        """
        # Run in a separate thread to avoid blocking the event loop
        loop = asyncio.get_event_loop()
        
        try:
            if recreate:
                # Delete collection if it exists
                try:
                    await loop.run_in_executor(
                        None, lambda: self.client.delete_collection(collection_name)
                    )
                    logger.info(f"Deleted existing collection: {collection_name}")
                except Exception as e:
                    logger.debug(f"Collection might not exist: {e}")
                
            # Create collection
            await loop.run_in_executor(
                None,
                lambda: self.client.create_collection(
                    collection_name=collection_name,
                    vectors_config=qdrant_models.VectorParams(
                        size=vector_size,
                        distance=distance,
                    ),
                ),
            )
            logger.info(f"Created collection: {collection_name}, vector_size: {vector_size}")
            return True
        except Exception as e:
            logger.error(f"Failed to create collection {collection_name}: {e}")
            return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(
            (ConnectionError, TimeoutError, UnexpectedResponse)
        ),
    )
    async def upsert_vectors(
        self,
        collection_name: str,
        vectors: List[VectorRecord],
    ) -> bool:
        """
        Insert or update vectors in the collection.

        :param collection_name: Name of the collection
        :param vectors: List of vector records to insert/update
        :returns: True if the operation was successful
        """
        loop = asyncio.get_event_loop()
        
        logger.debug(f"[VECTOR_DB] Starting vector insertion to {collection_name} with {len(vectors)} records")
        try:
            # Convert to Qdrant points
            points = [
                qdrant_models.PointStruct(
                    id=v.id,
                    vector=v.vector,
                    payload=v.metadata,
                )
                for v in vectors
            ]
            
            # Upsert points
            logger.debug(f"[VECTOR_DB] Executing vector insertion: collection={collection_name}, points={len(points)}")
            await loop.run_in_executor(
                None,
                lambda: self.client.upsert(
                    collection_name=collection_name,
                    points=points,
                ),
            )
            logger.info(f"Upserted {len(vectors)} vectors to {collection_name}")
            logger.debug(f"[VECTOR_DB] Vector insertion completed: collection={collection_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to upsert vectors to {collection_name}: {e}")
            return False

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(
            (ConnectionError, TimeoutError, UnexpectedResponse)
        ),
    )
    async def search_vectors(
        self,
        collection_name: str,
        query_vector: List[float],
        limit: int = 10,
        filter_condition: Optional[Dict[str, Any]] = None,
    ) -> List[VectorRecord]:
        """
        Search for similar vectors in the collection.

        :param collection_name: Name of the collection
        :param query_vector: Vector to search for
        :param limit: Maximum number of results to return
        :param filter_condition: Additional filter conditions
        :returns: List of vector records sorted by similarity
        """
        loop = asyncio.get_event_loop()
        
        logger.debug(f"[VECTOR_DB] Starting vector search: collection={collection_name}, limit={limit}")
        if filter_condition:
            logger.debug(f"[VECTOR_DB] Using filter conditions: {filter_condition}")
        
        try:
            # Search
            search_results = await loop.run_in_executor(
                None,
                lambda: self.client.search(
                    collection_name=collection_name,
                    query_vector=query_vector,
                    limit=limit,
                    query_filter=filter_condition,
                ),
            )
            
            # Convert to vector records
            results = [
                VectorRecord(
                    id=str(r.id),
                    vector=query_vector,  # Note: Qdrant doesn't return vectors by default
                    metadata=r.payload,
                    score=r.score,
                )
                for r in search_results
            ]
            
            logger.debug(f"[VECTOR_DB] Search completed, found {len(results)} results")
            return results
        except Exception as e:
            logger.error(f"Failed to search vectors in {collection_name}: {e}")
            return []

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=retry_if_exception_type(
            (ConnectionError, TimeoutError, UnexpectedResponse)
        ),
    )
    async def delete_vectors(
        self,
        collection_name: str,
        vector_ids: List[str],
    ) -> bool:
        """
        Delete vectors from the collection.

        :param collection_name: Name of the collection
        :param vector_ids: List of vector IDs to delete
        :returns: True if the operation was successful
        """
        loop = asyncio.get_event_loop()
        
        try:
            # Delete points
            await loop.run_in_executor(
                None,
                lambda: self.client.delete(
                    collection_name=collection_name,
                    points_selector=qdrant_models.PointIdsList(
                        points=vector_ids,
                    ),
                ),
            )
            logger.info(f"Deleted {len(vector_ids)} vectors from {collection_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete vectors from {collection_name}: {e}")
            return False

    async def collection_exists(self, collection_name: str) -> bool:
        """
        Check if a collection exists.

        :param collection_name: Name of the collection
        :returns: True if the collection exists
        """
        loop = asyncio.get_event_loop()
        
        try:
            collections = await loop.run_in_executor(
                None, lambda: self.client.get_collections().collections
            )
            return any(c.name == collection_name for c in collections)
        except Exception as e:
            logger.error(f"Failed to check if collection {collection_name} exists: {e}")
            return False

    async def get_collection_info(
        self, collection_name: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get information about a collection.

        :param collection_name: Name of the collection
        :returns: Collection information or None if collection doesn't exist
        """
        loop = asyncio.get_event_loop()
        
        try:
            collection_info = await loop.run_in_executor(
                None, lambda: self.client.get_collection(collection_name)
            )
            return {
                "name": collection_info.name,
                "vector_size": collection_info.config.params.vectors.size,
                "distance": collection_info.config.params.vectors.distance,
                "points_count": collection_info.points_count,
            }
        except Exception as e:
            logger.error(f"Failed to get collection info for {collection_name}: {e}")
            return None


@lru_cache()
def get_qdrant_client() -> QdrantClient:
    """
    Get a singleton instance of the Qdrant client.
    
    This function uses lru_cache to ensure only one client is created.
    It can be called directly without dependency injection.

    :returns: Singleton Qdrant client instance
    """
    return QdrantClient(
        host=settings.qdrant_host,
        port=settings.qdrant_port,
        api_key=settings.qdrant_api_key,
        timeout=settings.qdrant_timeout,
    ) 