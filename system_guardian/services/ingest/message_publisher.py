"""Services for processing and forwarding webhook events."""

import asyncio
import re
from typing import Optional, Dict, List, Any
import json
from datetime import datetime

from loguru import logger
from aio_pika import Channel, Message
from aio_pika.pool import Pool
from aiokafka import AIOKafkaProducer

# Import Slack notification components
from system_guardian.services.slack.client import SlackClient
from system_guardian.services.slack.templates import SlackMessageTemplate, AlertSeverity
from system_guardian.services.jira.client import JiraClient


class MessagePublisher:
    """Service for publishing messages to different message queues."""

    # Default topics mapping
    DEFAULT_TOPICS: Dict[str, str] = {
        "github": "github_events",
        "jira": "jira_events",
        "datadog": "datadog_events",
        # Add more sources here as needed
    }

    # Critical event types that should be routed to RabbitMQ for real-time processing
    CRITICAL_EVENT_TYPES: List[str] = [
        "error",
        "failure",
        "alert",
        "security",
        "outage",
        "incident",
        # Add more critical event types here
    ]

    # Incident-related topics and routing
    INCIDENT_TOPIC = "system_incidents"
    INCIDENT_ROUTING_KEY = "incidents.detected"
    INCIDENT_EXCHANGE = "system_incidents"

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
        sanitized = re.sub(r"[^a-zA-Z0-9\._-]", "_", topic)

        # Ensure the topic name doesn't start with a dot or underscore (Kafka recommendation)
        if sanitized and sanitized[0] in [".", "_"]:
            sanitized = "topic" + sanitized

        return sanitized

    @staticmethod
    def is_critical_event(event_message) -> bool:
        """
        Determine if an event should be considered critical and processed in real-time.

        :param event_message: The standardized event message
        :returns: True if the event is critical, False otherwise
        """
        # Check if event type contains any critical keywords
        event_type_lower = event_message.event_type.lower()
        if any(
            critical_type in event_type_lower
            for critical_type in MessagePublisher.CRITICAL_EVENT_TYPES
        ):
            return True

        # Check if the payload contains urgent/critical flags
        payload = event_message.raw_payload
        if isinstance(payload, dict):
            # Check for priority or severity indicators in common fields
            priority = payload.get("priority", "").lower()
            severity = payload.get("severity", "").lower()

            if any(word in priority for word in ["high", "urgent", "critical"]):
                return True

            if any(
                word in severity for word in ["high", "urgent", "critical", "fatal"]
            ):
                return True

        return False

    @staticmethod
    async def send_to_kafka(
        producer: AIOKafkaProducer,
        event_message,
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

            logger.debug(
                f"Using topic '{topic}' for event from source '{event_message.source}'"
            )
        else:
            # If a topic was provided, still ensure it's valid
            sanitized_topic = MessagePublisher.sanitize_topic_name(topic)
            if topic != sanitized_topic:
                logger.warning(
                    f"Provided topic name '{topic}' was sanitized to '{sanitized_topic}'"
                )
                topic = sanitized_topic

        try:
            # Simplified logging, only show details at DEBUG level
            logger.debug(f"Sending message to Kafka topic: {topic}")
            await producer.send(
                topic=topic,
                value=event_message.to_json().encode("utf-8"),
            )
            # No longer output success message to reduce log volume
        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {str(e)}")
            # Consider retrying or storing for later processing

    @staticmethod
    async def send_to_rabbitmq(
        channel_pool: Pool[Channel],
        event_message,
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
            logger.info(
                f"Successfully published message to RabbitMQ with routing key: {routing_key}"
            )
        except Exception as e:
            logger.error(f"Failed to send message to RabbitMQ: {str(e)}")
            # Consider retrying or storing for later processing

    @staticmethod
    async def publish_incident_detection(
        kafka_producer: Optional[AIOKafkaProducer],
        rmq_channel_pool: Optional[Pool[Channel]],
        event_message,
        incident_id: int,
        incident_info: Dict[str, Any],
    ) -> None:
        """
        Publish a notification that an incident was detected from an event.

        :param kafka_producer: Kafka producer for long-term storage
        :param rmq_channel_pool: RabbitMQ channel pool for real-time notifications
        :param event_message: The original event message that triggered the incident
        :param incident_id: The ID of the newly created incident
        :param incident_info: Additional information about the incident
        """
        # Create an enhanced message with incident information
        # Convert timestamp to ISO format string if it's a datetime object
        timestamp = event_message.timestamp
        if isinstance(timestamp, datetime):
            timestamp = timestamp.isoformat()

        incident_payload = {
            "original_event": {
                "source": event_message.source,
                "event_type": event_message.event_type,
                "timestamp": timestamp,
            },
            "incident_id": incident_id,
            "detection_time": incident_info.get("created_at", ""),
            "severity": incident_info.get("severity", "unknown"),
            "title": incident_info.get("title", ""),
            "description": incident_info.get("description", ""),
        }

        # Always send to RabbitMQ for real-time notifications
        if rmq_channel_pool:
            try:
                # Simplified logging
                logger.info(
                    f"Publishing incident #{incident_id} notification to RabbitMQ"
                )
                async with rmq_channel_pool.acquire() as conn:
                    exchange = await conn.declare_exchange(
                        name=MessagePublisher.INCIDENT_EXCHANGE,
                        auto_delete=False,  # Keep exchange even without bindings
                    )
                    await exchange.publish(
                        message=Message(
                            body=json.dumps(incident_payload).encode("utf-8"),
                            content_encoding="utf-8",
                            content_type="application/json",
                        ),
                        routing_key=MessagePublisher.INCIDENT_ROUTING_KEY,
                    )
            except Exception as e:
                logger.error(
                    f"Failed to publish incident notification to RabbitMQ: {str(e)}"
                )

        # 完全停止使用Kafka，即使提供了kafka_producer參數
        # if kafka_producer:
        #     try:
        #         # Simplified logging
        #         logger.info(f"Storing incident #{incident_id} record in Kafka")
        #         await kafka_producer.send(
        #             topic=MessagePublisher.INCIDENT_TOPIC,
        #             value=json.dumps(incident_payload).encode("utf-8"),
        #         )
        #     except Exception as e:
        #         logger.error(f"Failed to store incident record in Kafka: {str(e)}")

        # Send Slack notification directly
        try:
            # Create SlackClient
            slack_client = SlackClient()

            # Skip if Slack is not configured
            if not slack_client.is_configured:
                logger.warning(
                    "Slack notifications disabled, skipping incident notification"
                )
                return

            # Map severity from incident to AlertSeverity
            severity_map = {
                "low": AlertSeverity.INFO,
                "medium": AlertSeverity.WARNING,
                "high": AlertSeverity.ERROR,
                "critical": AlertSeverity.CRITICAL,
                # Default mappings if the severity doesn't match exactly
                "info": AlertSeverity.INFO,
                "warning": AlertSeverity.WARNING,
                "error": AlertSeverity.ERROR,
            }

            # Get incident details
            severity_str = incident_info.get("severity", "medium").lower()
            detection_time = incident_info.get(
                "created_at", datetime.utcnow().isoformat()
            )

            # Map severity to AlertSeverity enum
            severity = severity_map.get(severity_str, AlertSeverity.WARNING)

            # Send notification using incident template
            logger.info(
                f"Sending direct Slack notification for incident #{incident_id}"
            )
            template = SlackMessageTemplate.create_incident_notification(
                incident_id=str(incident_id),
                title=incident_info.get("title", "Untitled Incident"),
                severity=severity,
                description=incident_info.get(
                    "description", "No description available"
                ),
                timestamp=detection_time,
            )

            await slack_client.send_template(template)
            logger.info(f"Direct Slack notification sent for incident #{incident_id}")

        except Exception as e:
            logger.error(
                f"Error sending direct Slack notification for incident: {str(e)}"
            )

        # Create JIRA ticket directly
        try:
            # Create JIRA client
            jira_client = JiraClient()

            # Skip if JIRA is not configured
            if not jira_client.is_configured:
                logger.warning(
                    "JIRA is not properly configured, incident ticket will not be created"
                )
                return

            # Get incident details
            title = incident_info.get("title", "Untitled Incident")
            description = incident_info.get("description", "No description available")
            severity_str = incident_info.get("severity", "medium")

            # Get source information if available
            source = event_message.source
            event_type = event_message.event_type

            # Log JIRA configuration
            logger.info(
                f"JIRA configuration: project_key={jira_client.project_key}, issue_type={jira_client.issue_type}"
            )

            # Create JIRA ticket
            logger.info(f"Creating direct JIRA ticket for incident #{incident_id}")
            result = await jira_client.create_incident_ticket(
                incident_id=str(incident_id),
                title=title,
                description=description,
                severity=severity_str,
                source=source,
                event_type=event_type,
            )

            if "key" in result:
                logger.info(
                    f"Direct JIRA ticket created successfully: {result['key']} for incident #{incident_id}"
                )
            else:
                error_code = result.get("error", "unknown error")
                error_details = result.get("details", "no details available")
                logger.error(
                    f"Failed to create direct JIRA ticket for incident #{incident_id}: {error_code}"
                )
                logger.error(f"Error details: {error_details}")

        except Exception as e:
            logger.error(f"Error creating direct JIRA ticket for incident: {str(e)}")

    @staticmethod
    async def publish_event(
        event_message,
        kafka_producer: Optional[AIOKafkaProducer] = None,
        rmq_channel_pool: Optional[Pool[Channel]] = None,
    ) -> None:
        """
        Publish an event to message queues based on event characteristics.

        :param event_message: The standardized event message
        :param kafka_producer: Optional Kafka producer
        :param rmq_channel_pool: Optional RabbitMQ channel pool
        """
        tasks = []

        # 僅使用RabbitMQ來處理事件表插入
        # 不再使用Kafka作為事件處理的主要通道
        if rmq_channel_pool:
            # 所有事件都使用標準路由鍵發送到RabbitMQ
            tasks.append(
                MessagePublisher.send_to_rabbitmq(rmq_channel_pool, event_message)
            )

        # 完全停止使用Kafka，即使提供了kafka_producer也不使用
        # if kafka_producer:
        #     tasks.append(MessagePublisher.send_to_kafka(kafka_producer, event_message))

        if tasks:
            # Run all publishing tasks concurrently
            await asyncio.gather(*tasks, return_exceptions=True)
        else:
            logger.warning("No message queue configured for event publishing")
