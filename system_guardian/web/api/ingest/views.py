from typing import Optional, Dict, Any
import json
from urllib.parse import parse_qs

from fastapi import APIRouter, Header, Request
from loguru import logger

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
    # Log the received event type for debugging purposes
    logger.info(f"Received GitHub event: {x_github_event}")
    
    try:
        # Determine the request content type
        content_type = request.headers.get("content-type", "")
        
        # Process the request based on content type
        if "application/json" in content_type:
            # Direct JSON parsing for JSON content type
            body = await request.json()
        else:
            # Handle form data (application/x-www-form-urlencoded)
            form_data = await request.body()
            form_data_str = form_data.decode('utf-8')
            
            # Parse form data
            parsed_data = parse_qs(form_data_str)
            
            # GitHub webhooks typically include JSON data in the 'payload' field
            if 'payload' in parsed_data:
                payload_json = parsed_data['payload'][0]
                body = json.loads(payload_json)
            else:
                # If no payload field, try parsing the entire body as JSON
                body = json.loads(form_data_str)
        
        # Log the parsed payload for debugging
        logger.debug(f"Successfully parsed GitHub payload: {body}")
        
        # Return the processed message
        return Message(message=body)
        
    except json.JSONDecodeError as e:
        # Handle JSON parsing errors
        error_msg = f"Invalid JSON in webhook payload: {str(e)}"
        logger.error(error_msg)
        return Message(message={"error": error_msg})
    except UnicodeDecodeError as e:
        # Handle encoding errors
        error_msg = f"Encoding error in webhook payload: {str(e)}"
        logger.error(error_msg)
        return Message(message={"error": error_msg})
    except Exception as e:
        # Handle any other unexpected errors
        error_msg = f"Error processing GitHub webhook: {str(e)}"
        logger.exception(error_msg)
        return Message(message={"error": error_msg})
