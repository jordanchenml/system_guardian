"""Services for processing and forwarding webhook events."""
import asyncio
import re
from typing import Optional, Dict

from loguru import logger
from aio_pika import Channel, Message
from aio_pika.pool import Pool
from aiokafka import AIOKafkaProducer

from system_guardian.web.api.ingest.schema import StandardEventMessage


class MessagePublisher:
    """Service for publishing messages to different message queues."""

    # Default topics mapping
    DEFAULT_TOPICS: Dict[str, str] = {
        "github": "github_events",
        "jira": "jira_events",
        # Add more sources here as needed
    }

    @staticmethod
    def sanitize_topic_name(topic: str) -> str:
        """
        Sanitize the topic name to make it valid for Kafka.
        
        Kafka topic names can only include letters, numbers, dots, underscores, and hyphens.
        This method replaces any invalid characters with underscores.
        
        :param topic: The raw topic name
        :returns: A sanitized topic name that is valid for Kafka
        """
        # Replace all non-alphanumeric characters except dots, underscores, and hyphens with underscores
        sanitized = re.sub(r'[^a-zA-Z0-9\._-]', '_', topic)
        
        # Ensure the topic name doesn't start with a dot or underscore (Kafka recommendation)
        if sanitized and sanitized[0] in ['.', '_']:
            sanitized = 'topic' + sanitized
            
        return sanitized

    @staticmethod
    async def send_to_kafka(
        producer: AIOKafkaProducer,
        event_message: StandardEventMessage,
        topic: Optional[str] = None,
    ) -> None:
        """
        Send a message to Kafka.
        
        :param producer: Kafka producer instance
        :param event_message: The standardized event message
        :param topic: Optional kafka topic, if not provided uses a simplified topic strategy
        """
        if not topic:
            # Use a simpler topic strategy - one topic per source
            # This avoids having to create many different topics in Kafka
            if event_message.source in MessagePublisher.DEFAULT_TOPICS:
                topic = MessagePublisher.DEFAULT_TOPICS[event_message.source]
            else:
                # Fallback to a generic topic
                topic = "webhook_events"
                
            logger.info(f"Using topic '{topic}' for event from source '{event_message.source}' of type '{event_message.event_type}'")
        else:
            # If a topic was provided, still ensure it's valid
            sanitized_topic = MessagePublisher.sanitize_topic_name(topic)
            if topic != sanitized_topic:
                logger.warning(f"Provided topic name '{topic}' was sanitized to '{sanitized_topic}'")
                topic = sanitized_topic
        
        try:
            logger.info(f"Sending message to Kafka topic: {topic}")
            await producer.send(
                topic=topic,
                value=event_message.to_json().encode("utf-8"),
            )
            logger.debug(f"Successfully sent message to Kafka topic: {topic}")
        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {str(e)}")
            # Consider retrying or storing for later processing

    @staticmethod
    async def send_to_rabbitmq(
        channel_pool: Pool[Channel],
        event_message: StandardEventMessage,
        exchange_name: str = "webhook_events",
        routing_key: Optional[str] = None,
    ) -> None:
        """
        Send a message to RabbitMQ.
        
        :param channel_pool: RabbitMQ channel pool
        :param event_message: The standardized event message
        :param exchange_name: The exchange to publish to
        :param routing_key: Optional routing key, if not provided uses source.eventtype format
        """
        if not routing_key:
            # Generate routing key based on source and event type
            routing_key = f"{event_message.source}.{event_message.event_type}"
        
        try:
            logger.info(f"Sending message to RabbitMQ: {exchange_name}/{routing_key}")
            async with channel_pool.acquire() as conn:
                exchange = await conn.declare_exchange(
                    name=exchange_name,
                    auto_delete=True,
                )
                await exchange.publish(
                    message=Message(
                        body=event_message.to_json().encode("utf-8"),
                        content_encoding="utf-8",
                        content_type="application/json",
                    ),
                    routing_key=routing_key,
                )
            logger.debug(f"Successfully sent message to RabbitMQ: {exchange_name}/{routing_key}")
        except Exception as e:
            logger.error(f"Failed to send message to RabbitMQ: {str(e)}")
            # Consider retrying or storing for later processing

    @staticmethod
    async def publish_event(
        event_message: StandardEventMessage,
        kafka_producer: Optional[AIOKafkaProducer] = None,
        rmq_channel_pool: Optional[Pool[Channel]] = None,
    ) -> None:
        """
        Publish an event to all configured message queues.
        
        :param event_message: The standardized event message
        :param kafka_producer: Optional Kafka producer
        :param rmq_channel_pool: Optional RabbitMQ channel pool
        """
        tasks = []

        if kafka_producer:
            tasks.append(
                MessagePublisher.send_to_kafka(kafka_producer, event_message)
            )
        
        if rmq_channel_pool:
            tasks.append(
                MessagePublisher.send_to_rabbitmq(rmq_channel_pool, event_message)
            )
        
        if tasks:
            # Run all publishing tasks concurrently
            await asyncio.gather(*tasks, return_exceptions=True)
        else:
            logger.warning("No message queue configured for event publishing") 