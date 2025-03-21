from typing import Optional

from fastapi import APIRouter, Header, Request

from system_guardian.web.api.ingest.schema import Message

router = APIRouter()


@router.post("/github", response_model=Message)
async def process_github_webhook(
    request: Request,
    x_github_event: Optional[str] = Header(None, alias="X-GitHub-Event"),
) -> Message:
    """
    Process GitHub webhook events.

    This endpoint accepts GitHub webhook payloads for various events (PRs, Issues, Deployments, etc.)
    and processes them according to the event type specified in the X-GitHub-Event header.

    :param request: The incoming request object
    :param x_github_event: GitHub event type from X-GitHub-Event header
    :returns: message indicating successful processing
    """
    # Here you would typically validate the webhook signature
    # and process the event according to its type
    
    # Get the raw JSON data from the request
    body = await request.json()
    
    # Log event type for debugging
    print(f"GitHub Event: {x_github_event}")
    
    # Return the body as a Message object
    return Message(message=body)
