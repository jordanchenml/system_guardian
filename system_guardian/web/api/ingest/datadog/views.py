"""Views for Datadog webhook events."""

from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from loguru import logger
from aiokafka import AIOKafkaProducer
from aio_pika import Channel
from aio_pika.pool import Pool
from datetime import datetime

from system_guardian.web.api.ingest.datadog.schema import (
    DatadogWebhookRequest,
    DatadogWebhookResponse,
)
from system_guardian.web.api.ingest.schema import StandardEventMessage
from system_guardian.services.ingest import MessagePublisher
from system_guardian.services.kafka.dependencies import get_kafka_producer
from system_guardian.services.rabbit.dependencies import get_rmq_channel_pool
from system_guardian.services.ai.severity_classifier import SeverityClassifier

router = APIRouter()


@router.post(
    "/",
    response_model=DatadogWebhookResponse,
    status_code=status.HTTP_200_OK,
)
async def handle_datadog_webhook(
    webhook_data: DatadogWebhookRequest,
    background_tasks: BackgroundTasks,
    kafka_producer: AIOKafkaProducer = Depends(get_kafka_producer),
    rmq_channel_pool: Pool[Channel] = Depends(get_rmq_channel_pool),
) -> DatadogWebhookResponse:
    """
    Handle incoming Datadog webhook alerts.

    :param webhook_data: The webhook data from Datadog
    :param background_tasks: FastAPI background tasks object for async processing
    :param kafka_producer: Kafka producer dependency (不再使用，但保留參數以維持相容性)
    :param rmq_channel_pool: RabbitMQ channel pool dependency
    :returns: Response indicating success or failure
    """
    logger.info(f"Received Datadog webhook data: {webhook_data}")
    try:
        # Create a SeverityClassifier instance
        severity_classifier = SeverityClassifier()
        processed_count = 0

        for alert in webhook_data.alerts:
            # Convert alerts to dict
            alert_dict = alert.model_dump()

            # Determine severity using SeverityClassifier
            severity = await severity_classifier.classify_severity(
                incident_title=alert.title,
                incident_description=alert.text,
                source="datadog",
                events_data=[alert_dict],
            )

            alert_dict["severity"] = severity

            # Create standardized event message
            event_message = StandardEventMessage(
                source="datadog",
                event_type="alert",
                event_id=alert.alert_id,
                timestamp=alert.alert_created_at,
                raw_payload=alert_dict,
            )

            # 只使用RabbitMQ處理事件，不再使用Kafka
            # Forward to RabbitMQ in the background
            background_tasks.add_task(
                MessagePublisher.publish_event,
                event_message=event_message,
                kafka_producer=None,  # 不再使用Kafka
                rmq_channel_pool=rmq_channel_pool,
            )
            processed_count += 1

        logger.info(f"Successfully processed {processed_count} Datadog alerts")
        return DatadogWebhookResponse(
            success=True,
            message=f"Successfully processed {processed_count} alerts",
            processed_alerts=processed_count,
        )

    except Exception as e:
        logger.error(f"Error processing Datadog webhook: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to process webhook: {str(e)}",
        )
