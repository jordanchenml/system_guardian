from typing import Optional, Dict, Any
import json
from urllib.parse import parse_qs

from fastapi import APIRouter, Header, Request
from loguru import logger

from system_guardian.web.api.ingest.jira.schema import Message

router = APIRouter()


@router.post("/", response_model=Message)
async def process_jira_webhook(
    request: Request,
    x_jira_event: Optional[str] = Header(None, alias="X-Jira-Event"),
) -> Message:
    """
    Process Jira webhook events.

    This endpoint accepts Jira webhook payloads for various events (PRs, Issues, Deployments, etc.)
    and processes them according to the event type specified in the X-Jira-Event header.
    """
    logger.info(f"Received Jira event: {x_jira_event}")
    body = await request.json()
    logger.info(f"Received body: {body}")
    return Message(message=body)
