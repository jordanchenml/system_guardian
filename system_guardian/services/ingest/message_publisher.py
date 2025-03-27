"""Services for processing and forwarding webhook events."""

import asyncio
import re
from typing import Optional, Dict, List, Any
import json
from datetime import datetime

from loguru import logger
from aio_pika import Channel, Message
from aio_pika.pool import Pool

# Import Slack notification components
from system_guardian.services.slack.client import SlackClient
from system_guardian.services.slack.templates import SlackMessageTemplate, AlertSeverity
from system_guardian.services.jira.client import JiraClient
from system_guardian.settings import settings


class MessagePublisher:
    """Service for publishing messages to different message queues."""

    # Default exchange mapping
    DEFAULT_EXCHANGES: Dict[str, str] = {
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
    INCIDENT_EXCHANGE = "system_incidents"
    INCIDENT_ROUTING_KEY = "incidents.detected"

    # HistoRIC data exchange for analytics and storage
    HISTORIC_EXCHANGE = "system_history"

    @staticmethod
    def sanitize_routing_key(routing_key: str) -> str:
        """
        Sanitize the routing key to make it valid for RabbitMQ.

        RabbitMQ routing keys can only include letters, numbers, dots, underscores, and hyphens.
        This method replaces any invalid characters with underscores.

        :param routing_key: The raw routing key
        :returns: A sanitized routing key that is valid for RabbitMQ
        """
        # Replace all non-alphanumeric characters except dots, underscores, and hyphens with underscores
        sanitized = re.sub(r"[^a-zA-Z0-9\._-]", "_", routing_key)
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

        # Ensure routing key is valid
        routing_key = MessagePublisher.sanitize_routing_key(routing_key)

        try:
            logger.info(f"Sending message to RabbitMQ: {exchange_name}/{routing_key}")
            async with channel_pool.acquire() as conn:
                # 為 webhook_events 交換機配置特殊參數，以匹配現有配置
                # webhook_events 使用 durable=false 和 auto_delete=true
                if exchange_name == "webhook_events":
                    durable_setting = False
                    auto_delete_setting = True
                else:
                    durable_setting = True
                    auto_delete_setting = False

                exchange = await conn.declare_exchange(
                    name=exchange_name,
                    auto_delete=auto_delete_setting,
                    durable=durable_setting,
                )
                await exchange.publish(
                    message=Message(
                        body=event_message.to_json().encode("utf-8"),
                        content_encoding="utf-8",
                        content_type="application/json",
                        delivery_mode=2,  # Make message persistent
                    ),
                    routing_key=routing_key,
                )
            logger.info(
                f"Successfully published message to RabbitMQ with routing key: {routing_key}",
            )
        except Exception as e:
            logger.error(f"Failed to send message to RabbitMQ: {str(e)}")
            # Consider retrying or storing for later processing

    @staticmethod
    async def send_to_historic_storage(
        channel_pool: Pool[Channel],
        event_message,
        routing_key: Optional[str] = None,
    ) -> None:
        """
        Send a message to historic storage using RabbitMQ.

        :param channel_pool: RabbitMQ channel pool
        :param event_message: The standardized event message
        :param routing_key: Optional routing key, if not provided uses source.eventtype.history format
        """
        if not routing_key:
            # Generate routing key based on source and event type for historical storage
            # Handle both object and dict types for event_message
            source = (
                event_message.source
                if hasattr(event_message, "source")
                else event_message.get("source", "unknown")
            )
            event_type = (
                event_message.event_type
                if hasattr(event_message, "event_type")
                else event_message.get("event_type", "unknown")
            )
            routing_key = f"{source}.{event_type}.history"

        # Use the historical exchange
        exchange_name = MessagePublisher.HISTORIC_EXCHANGE

        try:
            logger.debug(
                f"Storing historical data in RabbitMQ: {exchange_name}/{routing_key}",
            )
            async with channel_pool.acquire() as conn:
                exchange = await conn.declare_exchange(
                    name=exchange_name,
                    auto_delete=False,
                    durable=True,  # Make exchange persist across broker restarts
                )

                # Convert message to JSON string
                if hasattr(event_message, "to_json"):
                    # Object with to_json method
                    message_body = event_message.to_json().encode("utf-8")
                elif isinstance(event_message, dict):
                    # Dictionary object
                    message_body = json.dumps(event_message).encode("utf-8")
                else:
                    # Try to convert to dict if it's another type of object
                    try:
                        message_body = json.dumps(event_message.__dict__).encode(
                            "utf-8",
                        )
                    except (AttributeError, TypeError):
                        message_body = json.dumps(
                            {"error": "Could not serialize message"},
                        ).encode("utf-8")

                await exchange.publish(
                    message=Message(
                        body=message_body,
                        content_encoding="utf-8",
                        content_type="application/json",
                        delivery_mode=2,  # Make message persistent
                    ),
                    routing_key=routing_key,
                )
            # No logging for success to reduce log volume
        except Exception as e:
            logger.error(f"Failed to store historical data in RabbitMQ: {str(e)}")

    @staticmethod
    async def publish_incident_detection(
        rmq_channel_pool: Optional[Pool[Channel]],
        event_message,
        incident_id: int,
        incident_info: Dict[str, Any],
    ) -> None:
        """
        Publish a notification that an incident was detected from an event.

        :param rmq_channel_pool: RabbitMQ channel pool for real-time notifications
        :param event_message: The original event message that triggered the incident
        :param incident_id: The ID of the newly created incident
        :param incident_info: Additional information about the incident
        """
        # Validate incident ID - ensure it's a proper value
        if incident_id is None or incident_id == 0:
            logger.error(
                f"Attempted to publish notification with invalid incident_id: {incident_id}",
            )
            # Check if incident_info contains a valid ID we can use instead
            if (
                incident_info
                and "id" in incident_info
                and incident_info["id"] not in [None, 0]
            ):
                logger.info(
                    f"Using ID from incident_info instead: {incident_info['id']}",
                )
                incident_id = incident_info["id"]
            else:
                logger.error(
                    "Cannot find valid incident ID, cannot publish notification",
                )
                return

        # Ensure incident_id is valid and not zero
        if incident_id == 0:
            logger.error(
                "Incident ID is zero, skipping notification to prevent invalid alerts",
            )
            return

        logger.info(
            f"Publishing incident notification, ID: {incident_id}, Title: {incident_info.get('title', 'Unknown')}",
        )

        # Create an enhanced message with incident information
        # Get the timestamp, handling different possible formats
        timestamp = None
        if hasattr(event_message, "timestamp"):
            # Object with timestamp attribute
            timestamp = event_message.timestamp
        elif isinstance(event_message, dict) and "timestamp" in event_message:
            # Dictionary with timestamp key
            timestamp = event_message["timestamp"]

        # Convert timestamp to ISO format string if it's a datetime object
        if isinstance(timestamp, datetime):
            timestamp = timestamp.isoformat()
        elif timestamp is None:
            # Default to current time if no timestamp is available
            timestamp = datetime.now().isoformat()

        # Get source and event_type from event_message
        source = (
            event_message.source
            if hasattr(event_message, "source")
            else event_message.get("source", "unknown")
        )
        event_type = (
            event_message.event_type
            if hasattr(event_message, "event_type")
            else event_message.get("event_type", "unknown")
        )

        # Ensure incident_info has consistent ID
        if "id" in incident_info and incident_info["id"] != incident_id:
            logger.warning(
                f"Fixing inconsistent incident ID in payload: changing from {incident_info['id']} to {incident_id}",
            )
            incident_info["id"] = incident_id

        # Add message ID to ensure idempotent processing
        message_id = f"incident_{incident_id}_{int(datetime.now().timestamp())}"

        incident_payload = {
            "original_event": {
                "source": source,
                "event_type": event_type,
                "timestamp": timestamp,
            },
            "incident_id": incident_id,
            "id": incident_id,  # Ensure both ID fields exist and are consistent
            "message_id": message_id,  # Add unique message ID for deduplication
            "detection_time": incident_info.get("created_at", ""),
            "severity": incident_info.get("severity", "unknown"),
            "title": incident_info.get("title", ""),
            "description": incident_info.get("description", ""),
        }

        # Send to RabbitMQ for real-time notifications
        if rmq_channel_pool:
            try:
                # Simplified logging
                logger.info(
                    f"Publishing incident #{incident_id} notification to RabbitMQ",
                )
                async with rmq_channel_pool.acquire() as conn:
                    exchange = await conn.declare_exchange(
                        name=MessagePublisher.INCIDENT_EXCHANGE,
                        auto_delete=False,  # Keep exchange even without bindings
                        durable=False,  # 更改為 False 以匹配現有的 system_incidents 交換機配置
                    )
                    await exchange.publish(
                        message=Message(
                            body=json.dumps(incident_payload).encode("utf-8"),
                            content_encoding="utf-8",
                            content_type="application/json",
                            delivery_mode=2,  # Make message persistent
                        ),
                        routing_key=MessagePublisher.INCIDENT_ROUTING_KEY,
                    )

                # Also send to historic storage for analytics and record keeping
                await MessagePublisher.send_to_historic_storage(
                    rmq_channel_pool,
                    event_message,
                    routing_key=f"incidents.{incident_id}.history",
                )
            except Exception as e:
                logger.error(
                    f"Failed to publish incident notification to RabbitMQ: {str(e)}",
                )

        # Always send Slack notification for incidents, if enabled
        # This is independent of RabbitMQ status
        if settings.slack_enabled:
            try:
                source = (
                    event_message.source
                    if hasattr(event_message, "source")
                    else incident_payload.get("original_event", {}).get(
                        "source", "unknown",
                    )
                )

                logger.info(
                    f"Attempting to send incident #{incident_id} notification to Slack",
                )

                # Check if Slack settings are properly configured
                if not settings.slack_bot_token:
                    logger.error(
                        "Cannot send Slack notification: SYSTEM_GUARDIAN_SLACK_BOT_TOKEN is not set",
                    )
                    return

                if not settings.slack_channel_id:
                    logger.error(
                        "Cannot send Slack notification: SYSTEM_GUARDIAN_SLACK_CHANNEL_ID is not set",
                    )
                    return

                await MessagePublisher._send_incident_slack_notification(
                    incident_info, source,
                )
                logger.info(
                    f"Successfully sent incident #{incident_id} notification to Slack",
                )
            except Exception as e:
                logger.error(
                    f"Failed to send Slack notification: {str(e)}", exc_info=True,
                )
                # Log the full stack trace for easier debugging

    @staticmethod
    async def _send_incident_slack_notification(
        incident_info: Dict[str, Any], source: str,
    ) -> None:
        """
        Send a notification to Slack about a detected incident.

        :param incident_info: Information about the incident
        :param source: The source of the original event
        """
        try:
            # Log incident info for debugging
            logger.debug(
                f"Preparing Slack notification with incident info: {incident_info}",
            )

            # Track in-memory which notifications have been sent to prevent duplicates
            # Use a static class variable to keep track across all instances
            if not hasattr(MessagePublisher, "_sent_notifications"):
                MessagePublisher._sent_notifications = {}

            # Extract incident ID for deduplication
            incident_id = None
            for id_field in ["id", "incident_id"]:
                if id_field in incident_info and incident_info[id_field] not in [
                    None,
                    0,
                    "0",
                    "",
                ]:
                    incident_id = str(incident_info[id_field])
                    break

            # Avoid sending notifications with ID=0
            if not incident_id or incident_id in ["0", 0]:
                logger.error(
                    "Cannot send notification with invalid incident ID (missing or zero)",
                )
                return

            # Generate notification fingerprint using available fields
            notification_fingerprint = (
                f"{incident_id}:{incident_info.get('title', '')}:{source}"
            )

            # Check for duplicate notifications sent in the last 5 minutes
            current_time = datetime.utcnow()
            if notification_fingerprint in MessagePublisher._sent_notifications:
                last_sent = MessagePublisher._sent_notifications[
                    notification_fingerprint
                ]
                if (current_time - last_sent).total_seconds() < 300:  # 5 minutes
                    logger.info(
                        f"Skipping duplicate Slack notification for incident #{incident_id} - sent recently",
                    )
                    return

            # Validate required settings
            if not all([settings.slack_bot_token, settings.slack_channel_id]):
                logger.error(
                    "Cannot send Slack notification: missing required configuration. "
                    f"Bot token: {'configured' if settings.slack_bot_token else 'missing'}, "
                    f"Channel ID: {'configured' if settings.slack_channel_id else 'missing'}",
                )
                return

            logger.info(f"Preparing Slack notification for incident ID: {incident_id}")

            # Initialize Slack client
            slack_client = SlackClient()

            logger.debug(
                f"Slack client initialized with channel: {settings.slack_channel_id}",
            )

            # Validate Slack client configuration
            if not slack_client.is_configured:
                logger.error("Slack client is not properly configured")
                return

            # Determine severity for alert
            severity_str = str(incident_info.get("severity", "unknown")).lower()
            if severity_str in ["critical", "fatal"]:
                severity = AlertSeverity.CRITICAL
            elif severity_str in ["high", "major"]:
                severity = AlertSeverity.ERROR
            elif severity_str in ["medium", "moderate"]:
                severity = AlertSeverity.WARNING
            else:
                severity = AlertSeverity.INFO

            # Ensure title has a valid value
            title = incident_info.get("title")
            if not title or not isinstance(title, str):
                title = f"Incident from {source}"
                logger.warning(
                    f"Using default title for incident #{incident_id}: {title}",
                )

            # Ensure description has a valid value
            description = incident_info.get("description")
            if not description or not isinstance(description, str):
                description = f"An incident was detected from {source}"
                logger.warning(f"Using default description for incident #{incident_id}")

            # Include source in description if not already mentioned
            if (
                source
                and source != "unknown"
                and source.lower() not in description.lower()
            ):
                description = f"[Source: {source}] {description}"

            # Get timestamp from created_at or use current time
            timestamp = incident_info.get("created_at")
            if not timestamp:
                timestamp = datetime.now().isoformat()
                logger.warning(
                    f"Using current time for incident #{incident_id} as created_at was missing",
                )

            # Create message using create_incident_notification template
            logger.debug(f"Creating Slack message template for incident #{incident_id}")
            template = SlackMessageTemplate.create_incident_notification(
                incident_id=incident_id,
                title=title,
                severity=severity,
                description=description,
                timestamp=timestamp,
            )

            # Send message using send_template directly
            logger.debug(f"Sending Slack notification for incident #{incident_id}")
            response = await slack_client.send_template(template)

            if response and response.get("ok"):
                # Mark this notification as sent to prevent duplicates
                MessagePublisher._sent_notifications[notification_fingerprint] = (
                    current_time
                )

                # Cleanup notifications older than 1 hour to prevent memory growth
                cleanup_keys = [
                    k
                    for k, v in MessagePublisher._sent_notifications.items()
                    if (current_time - v).total_seconds() > 3600  # 1 hour
                ]
                for k in cleanup_keys:
                    del MessagePublisher._sent_notifications[k]

                logger.info(
                    f"Successfully sent incident #{incident_id} notification to Slack channel: {settings.slack_channel_id}",
                )
            else:
                logger.error(
                    f"Slack API returned error when sending notification: {response.get('error', 'unknown')}",
                )
        except Exception as e:
            logger.error(f"Failed to send Slack notification: {str(e)}", exc_info=True)
            # No need to re-raise, just log the error

    @staticmethod
    async def publish_event(
        event_message,
        rmq_channel_pool: Optional[Pool[Channel]] = None,
    ) -> None:
        """
        Publish an event message to appropriate channels based on event type.

        :param event_message: The standardized event message object
        :param rmq_channel_pool: Optional RabbitMQ channel pool
        """
        tasks = []

        # 僅使用RabbitMQ來處理事件表插入
        if rmq_channel_pool:
            # 所有事件都使用標準路由鍵發送到RabbitMQ
            tasks.append(
                MessagePublisher.send_to_rabbitmq(rmq_channel_pool, event_message),
            )

            # 將關鍵事件同時發送到歷史儲存中
            if MessagePublisher.is_critical_event(event_message):
                tasks.append(
                    MessagePublisher.send_to_historic_storage(
                        rmq_channel_pool, event_message,
                    ),
                )

        # Execute all messaging tasks concurrently
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
