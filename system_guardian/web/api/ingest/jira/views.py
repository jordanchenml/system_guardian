from typing import Optional, Dict, Any
import json
from urllib.parse import parse_qs

from fastapi import APIRouter, Header, Request, Depends, BackgroundTasks
from loguru import logger
from aiokafka import AIOKafkaProducer
from aio_pika import Channel
from aio_pika.pool import Pool

from system_guardian.web.api.ingest.jira.schema import Message
from system_guardian.web.api.ingest.schema import StandardEventMessage
from system_guardian.services.ingest import MessagePublisher
from system_guardian.services.kafka.dependencies import get_kafka_producer
from system_guardian.services.rabbit.dependencies import get_rmq_channel_pool

router = APIRouter()


@router.post("/", response_model=Message)
async def process_jira_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_jira_event: Optional[str] = Header(None, alias="X-Jira-Event"),
    kafka_producer: Optional[AIOKafkaProducer] = Depends(get_kafka_producer),
    rmq_channel_pool: Optional[Pool[Channel]] = Depends(get_rmq_channel_pool),
) -> Message:
    """
    Process Jira webhook events.

    This endpoint accepts Jira webhook payloads for various events
    and processes them according to the event type specified in the X-Jira-Event header.
    It also forwards the event to configured message queues.

    :param request: The incoming request object
    :param background_tasks: FastAPI background tasks object for async processing
    :param x_jira_event: Jira event type from X-Jira-Event header
    :param kafka_producer: Kafka producer dependency
    :param rmq_channel_pool: RabbitMQ channel pool dependency
    :returns: message indicating successful processing
    """
    logger.info(f"Received Jira event: {x_jira_event}")
    
    try:
        # Parse the request body as JSON
        body = await request.json()
        logger.debug(f"Received Jira webhook body: {body}")
        
        # Create standardized event message
        event_message = StandardEventMessage(
            source="jira",
            event_type=x_jira_event or extract_event_type_from_body(body) or "unknown",
            raw_payload=body,
        )
        
        # Forward to message queues in the background
        # This allows us to respond to the webhook quickly without waiting for message queue processing
        background_tasks.add_task(
            MessagePublisher.publish_event,
            event_message=event_message,
            kafka_producer=kafka_producer,
            # rmq_channel_pool=rmq_channel_pool,
        )
        
        return Message(message=body)
    except json.JSONDecodeError as e:
        # Handle JSON parsing errors
        error_msg = f"Invalid JSON in Jira webhook payload: {str(e)}"
        logger.error(error_msg)
        return Message(message={"error": error_msg})
    except Exception as e:
        # Handle any other unexpected errors
        error_msg = f"Error processing Jira webhook: {str(e)}"
        logger.exception(error_msg)
        return Message(message={"error": error_msg})


def extract_event_type_from_body(body: Dict[str, Any]) -> Optional[str]:
    """
    Extract the event type from the Jira webhook body if not provided in the header.
    
    :param body: The parsed JSON body of the Jira webhook
    :returns: The extracted event type or None if not found
    """
    # Different ways to extract event type based on Jira webhook format
    if "webhookEvent" in body:
        return body["webhookEvent"]
    if "issue_event_type_name" in body:
        return body["issue_event_type_name"]
    
    # Add more extraction logic as needed for different Jira webhook formats
    
    return None
