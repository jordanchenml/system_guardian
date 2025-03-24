"""Vector database API views."""
from typing import Dict, List, Any

from fastapi import APIRouter, Depends, HTTPException, status, Query

from system_guardian.services.vector_db.qdrant_client import QdrantClient
from system_guardian.services.vector_db.dependencies import get_qdrant_dependency
from system_guardian.web.api.vector_db.schema import (
    CollectionResponse,
    CollectionsListResponse,
    CollectionInfoResponse,
)


router = APIRouter()


@router.post("/collections/{collection_name}", response_model=CollectionResponse, status_code=status.HTTP_201_CREATED)
async def create_collection(
    collection_name: str,
    vector_size: int = Query(..., description="Dimension of the vector space"),
    distance: str = Query("Cosine", description="Distance metric to use (Cosine, Euclid, Dot)"),
    recreate: bool = Query(False, description="Whether to recreate the collection if it exists"),
    qdrant_client: QdrantClient = Depends(get_qdrant_dependency),
) -> CollectionResponse:
    """
    Create a new vector collection.
    
    :param collection_name: Name of the collection
    :param vector_size: Dimension of the vector space
    :param distance: Distance metric to use (Cosine, Euclid, Dot)
    :param recreate: Whether to recreate the collection if it exists
    :param qdrant_client: Qdrant client instance
    :returns: Collection response indicating success or failure
    :raises HTTPException: If collection creation fails
    """
    result = await qdrant_client.create_collection(
        collection_name=collection_name,
        vector_size=vector_size,
        distance=distance,
        recreate=recreate,
    )
    
    if not result:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create collection {collection_name}",
        )
    
    return CollectionResponse(success=True)


@router.get("/collections", response_model=CollectionsListResponse, status_code=status.HTTP_200_OK)
async def list_collections(
    qdrant_client: QdrantClient = Depends(get_qdrant_dependency),
) -> CollectionsListResponse:
    """
    List all vector collections.
    
    :param qdrant_client: Qdrant client instance
    :returns: List of collection names
    :raises HTTPException: If listing collections fails
    """
    loop = __import__("asyncio").get_event_loop()
    
    try:
        collections_info = await loop.run_in_executor(
            None, lambda: qdrant_client.client.get_collections().collections
        )
        return CollectionsListResponse(collections=[c.name for c in collections_info])
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to list collections: {str(e)}",
        )


@router.get("/collections/{collection_name}", response_model=CollectionInfoResponse, status_code=status.HTTP_200_OK)
async def get_collection_info(
    collection_name: str,
    qdrant_client: QdrantClient = Depends(get_qdrant_dependency),
) -> CollectionInfoResponse:
    """
    Get information about a vector collection.
    
    :param collection_name: Name of the collection
    :param qdrant_client: Qdrant client instance
    :returns: Collection information
    :raises HTTPException: If collection does not exist
    """
    collection_info = await qdrant_client.get_collection_info(collection_name)
    
    if not collection_info:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Collection {collection_name} not found",
        )
    
    return CollectionInfoResponse(**collection_info) 