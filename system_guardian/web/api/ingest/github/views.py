from typing import Optional
import json
from urllib.parse import parse_qs

from fastapi import APIRouter, Header, Request, Depends, BackgroundTasks
from loguru import logger
from aiokafka import AIOKafkaProducer
from aio_pika import Channel
from aio_pika.pool import Pool

from system_guardian.web.api.ingest.github.schema import Message
from system_guardian.web.api.ingest.schema import StandardEventMessage
from system_guardian.services.ingest import MessagePublisher
from system_guardian.services.kafka.dependencies import get_kafka_producer
from system_guardian.services.rabbit.dependencies import get_rmq_channel_pool

router = APIRouter()


@router.post("/", response_model=Message)
async def process_github_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    x_github_event: Optional[str] = Header(None, alias="X-GitHub-Event"),
    kafka_producer: Optional[AIOKafkaProducer] = Depends(get_kafka_producer),
    rmq_channel_pool: Optional[Pool[Channel]] = Depends(get_rmq_channel_pool),
) -> Message:
    """
    Process GitHub webhook events.

    This endpoint accepts GitHub webhook payloads for various events (PRs, Issues, Deployments, etc.)
    and processes them according to the event type specified in the X-GitHub-Event header.
    It also forwards the event to configured message queues.

    :param request: The incoming request object
    :param background_tasks: FastAPI background tasks object for async processing
    :param x_github_event: GitHub event type from X-GitHub-Event header
    :param kafka_producer: Kafka producer dependency
    :param rmq_channel_pool: RabbitMQ channel pool dependency
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
        
        # Create standardized event message
        event_message = StandardEventMessage(
            source="github",
            event_type=x_github_event or "unknown",
            raw_payload=body,
        )
        
        # Forward to message queues in the background
        # This allows us to respond to the webhook quickly without waiting for message queue processing
        background_tasks.add_task(
            MessagePublisher.publish_event,
            event_message=event_message,
            kafka_producer=kafka_producer,
            rmq_channel_pool=rmq_channel_pool,
        )
        
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
