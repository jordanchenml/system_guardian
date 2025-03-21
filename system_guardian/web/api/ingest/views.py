from typing import Optional

from fastapi import APIRouter, Header

from system_guardian.web.api.ingest.schema import Message

router = APIRouter()


@router.post("/github", response_model=Message)
async def process_github_webhook(
    # request: Request,
    # incoming_message: Message,
    x_github_event: Optional[str] = Header(None, alias="X-GitHub-Event"),
) -> Message:
    """
    Process GitHub webhook events.

    This endpoint accepts GitHub webhook payloads for various events (PRs, Issues, Deployments, etc.)
    and processes them according to the event type specified in the X-GitHub-Event header.

    :param request: The incoming request object
    :param incoming_message: The webhook payload in the Message format
    :param x_github_event: GitHub event type from X-GitHub-Event header
    :returns: message indicating successful processing
    """
    # Here you would typically validate the webhook signature
    # and process the event according to its type

    # For now, we'll just echo back the incoming message
    print(x_github_event)
    return str(x_github_event)
