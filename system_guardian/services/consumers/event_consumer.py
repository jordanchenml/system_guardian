"""Event consumer for processing events from message queues."""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple

from loguru import logger
from aio_pika import connect_robust, IncomingMessage
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from aio_pika.pool import Pool

from system_guardian.db.models.incidents import Event, Incident

# Remove direct import of StandardEventMessage, use type annotation instead
# from system_guardian.web.api.ingest.schema import StandardEventMessage
from system_guardian.settings import settings
from system_guardian.services.ai.incident_detector import IncidentDetector
from system_guardian.services.config import ConfigManager
from system_guardian.services.ai.severity_classifier import SeverityClassifier
from system_guardian.services.ingest.message_publisher import MessagePublisher
from system_guardian.services.slack.client import SlackClient
from system_guardian.services.slack.templates import SlackMessageTemplate, AlertSeverity
from system_guardian.services.jira.client import JiraClient


class EventConsumer:
    """Consumer for processing events from RabbitMQ."""

    def __init__(
        self,
        db_session_factory,
        rmq_exchange: str = "webhook_events",
        rmq_queue: str = "webhook_events_queue",
        rmq_routing_keys: Optional[List[str]] = None,
        auto_incident_creation: bool = True,
        ai_engine=None,
        rmq_channel_pool=None,
    ):
        """
        Initialize the event consumer.

        :param db_session_factory: Factory for creating database sessions
        :param rmq_exchange: RabbitMQ exchange name
        :param rmq_queue: RabbitMQ queue name
        :param rmq_routing_keys: List of RabbitMQ routing keys to bind
        :param auto_incident_creation: Whether to automatically create incidents from events
        :param ai_engine: Optional AIEngine instance for enhanced event processing
        :param rmq_channel_pool: Optional pre-initialized RabbitMQ channel pool
        """
        self.db_session_factory = db_session_factory
        self.rmq_exchange = rmq_exchange
        self.rmq_queue = rmq_queue
        self.rmq_routing_keys = rmq_routing_keys or [
            "github.*",
            "jira.*",
            "datadog.*",
            "incidents.*",
        ]
        self.should_exit = False
        self.auto_incident_creation = auto_incident_creation
        self.ai_engine = ai_engine

        # Initialize config manager and severity classifier
        self.config_manager = ConfigManager()
        self.severity_classifier = SeverityClassifier()

        # Initialize deduplication caches
        self._processed_messages = {}  # Used to track processed messages
        self._cache_cleanup_time = datetime.utcnow()

        # Initialize incident detector with new parameters
        self.incident_detector = IncidentDetector(
            config_manager=self.config_manager,
            llm_client=self.ai_engine.llm if self.ai_engine else None,
            llm_model=(
                settings.ai_incident_detection_model
                if settings.ai_allow_advanced_models
                else settings.openai_completion_model
            ),
            severity_classifier=self.severity_classifier,
        )

        # Store initialized RabbitMQ channel pool
        self.rmq_channel_pool = rmq_channel_pool

    async def start(self) -> None:
        """Start consuming messages from RabbitMQ."""
        await self.start_rabbitmq_consumer()

    async def stop(self) -> None:
        """Stop all consumers."""
        self.should_exit = True
        logger.info("Event consumer stopping")

    async def start_rabbitmq_consumer(self) -> None:
        """Start consuming messages from RabbitMQ."""
        logger.info(f"Starting RabbitMQ consumer for exchange: {self.rmq_exchange}")

        try:
            # Create connection
            connection = await connect_robust(
                host=settings.rabbit_host,
                port=settings.rabbit_port,
                login=settings.rabbit_user,
                password=settings.rabbit_pass,
                virtualhost=settings.rabbit_vhost,
            )
            logger.info("Successfully connected to RabbitMQ")

            # Create channel
            channel = await connection.channel()
            logger.info("Successfully created RabbitMQ channel")

            # If no RabbitMQ channel pool is provided, try to create one locally
            if self.rmq_channel_pool is None:
                try:
                    logger.warning(
                        "No RabbitMQ channel pool provided, attempting to create one locally",
                    )
                    # Using locally created RabbitMQ channel pool
                    from aio_pika.pool import Pool

                    async def get_local_connection():
                        """Get local RabbitMQ connection"""
                        return await connect_robust(
                            host=settings.rabbit_host,
                            port=settings.rabbit_port,
                            login=settings.rabbit_user,
                            password=settings.rabbit_pass,
                            virtualhost=settings.rabbit_vhost,
                        )

                    connection_pool = Pool(
                        get_local_connection, max_size=settings.rabbit_pool_size,
                    )
                    logger.info(
                        f"Created connection pool with size {settings.rabbit_pool_size}",
                    )

                    async def get_local_channel():
                        """Get local RabbitMQ channel"""
                        async with connection_pool.acquire() as connection:
                            return await connection.channel()

                    self.rmq_channel_pool = Pool(
                        get_local_channel, max_size=settings.rabbit_channel_pool_size,
                    )
                    logger.info(
                        f"Created channel pool with size {settings.rabbit_channel_pool_size}",
                    )
                except Exception as e:
                    logger.error(f"Failed to create RabbitMQ channel pool: {str(e)}")
                    logger.warning(
                        "Incident notifications to RabbitMQ will be disabled",
                    )

            # ===== Main RabbitMQ consumer setup =====

            # 1. Setup regular events exchange and queue
            webhook_exchange = await channel.declare_exchange(
                name=self.rmq_exchange,
                auto_delete=True,
                durable=False,
            )
            logger.info(f"Declared exchange: {self.rmq_exchange}")

            webhook_queue = await channel.declare_queue(
                name=self.rmq_queue,
                durable=True,
                auto_delete=False,
            )
            logger.info(f"Declared queue: {self.rmq_queue}")

            # Bind queue to exchange with routing keys
            for routing_key in self.rmq_routing_keys:
                await webhook_queue.bind(webhook_exchange, routing_key=routing_key)
                logger.info(f"Bound queue to exchange with routing key: {routing_key}")

            # 2. Setup incidents exchange and queue for incident notifications
            incident_exchange = await channel.declare_exchange(
                name=MessagePublisher.INCIDENT_EXCHANGE,
                auto_delete=False,
                durable=False,
            )
            logger.info(
                f"Declared incident exchange: {MessagePublisher.INCIDENT_EXCHANGE}",
            )

            incident_queue = await channel.declare_queue(
                name="incident_notifications",
                durable=True,
                auto_delete=False,
            )
            logger.info("Declared incident queue")

            # Bind incident queue to exchange with incident routing key
            await incident_queue.bind(
                incident_exchange,
                routing_key=MessagePublisher.INCIDENT_ROUTING_KEY,
            )
            logger.info(
                f"Bound incident queue to exchange with routing key: {MessagePublisher.INCIDENT_ROUTING_KEY}",
            )

            # 3. Setup historic data exchange and queue
            historic_exchange = await channel.declare_exchange(
                name=MessagePublisher.HISTORIC_EXCHANGE,
                auto_delete=False,
                durable=True,
            )
            logger.info(
                f"Declared historic exchange: {MessagePublisher.HISTORIC_EXCHANGE}",
            )

            historic_queue = await channel.declare_queue(
                name="historic_data",
                durable=True,
                auto_delete=False,
            )
            logger.info("Declared historic queue")

            # Bind historic queue to exchange with wildcard for all historic data
            await historic_queue.bind(
                historic_exchange,
                routing_key="*.*.history",
            )
            logger.info(
                "Bound historic queue to exchange with routing key: *.*.history",
            )

            # Set up consumers with callbacks
            logger.info("Setting up message consumers...")
            await webhook_queue.consume(self.on_rabbitmq_message)
            await incident_queue.consume(self.on_incident_notification)
            logger.info("Message consumers setup complete")

            # Set up prefetch count to prevent worker overload
            await channel.set_qos(prefetch_count=10)
            logger.info("Set QoS prefetch count to 10")

            logger.info("RabbitMQ consumer started successfully")

            # Keep consumer running until stopped
            while not self.should_exit:
                await asyncio.sleep(1)

            # Clean up
            await connection.close()
            logger.info("RabbitMQ consumer stopped")

        except Exception as e:
            logger.error(f"Error in RabbitMQ consumer setup: {str(e)}")
            logger.exception("Detailed exception information:")
            raise

    async def on_rabbitmq_message(self, message: IncomingMessage) -> None:
        """
        Handle incoming RabbitMQ messages.

        :param message: The RabbitMQ message
        """
        try:
            # Generate a unique message fingerprint for deduplication
            message_fingerprint = self._calculate_message_fingerprint(message)

            # Check if this message has already been processed
            if self._is_duplicate_message(message_fingerprint):
                logger.warning(
                    f"Received duplicate message, skipping processing: {message.routing_key}",
                )
                await message.ack()  # Acknowledge duplicates to prevent redelivery loop
                return

            logger.info(f"Received message from RabbitMQ: {message.routing_key}")

            # Store message fingerprint to prevent duplicate processing
            self._record_processed_message(message_fingerprint)

            # Process the message
            await self.process_message(message.body.decode("utf-8"))
            logger.info(f"Successfully processed message: {message.routing_key}")

            # Acknowledge the message after successful processing
            await message.ack()
            logger.info(f"Acknowledged message: {message.routing_key}")

        except Exception as e:
            logger.error(f"Error processing RabbitMQ message: {str(e)}")
            logger.exception("Detailed exception information:")
            # Acknowledge the message even if processing failed to prevent redelivery loop
            await message.ack()
            logger.info(f"Acknowledged failed message: {message.routing_key}")

    def _calculate_message_fingerprint(self, message: IncomingMessage) -> str:
        """
        Calculate a unique fingerprint for the message to detect duplicates.

        :param message: The RabbitMQ message
        :return: A unique fingerprint string
        """
        # Try to parse message body as JSON to extract unique identifiers
        try:
            message_body = message.body.decode("utf-8")
            message_data = json.loads(message_body)

            # If the message has an ID, use it as part of the fingerprint
            if "id" in message_data:
                return (
                    f"{message.routing_key}:{message_data['id']}:{hash(message_body)}"
                )

            # Message delivery tag can also help identify duplicates in some cases
            if hasattr(message, "delivery_tag"):
                return (
                    f"{message.routing_key}:{message.delivery_tag}:{hash(message_body)}"
                )
        except:
            # If we can't parse JSON or extract IDs, fall back to simpler fingerprinting
            pass

        # If all else fails, use a hash of the full message body
        return f"{message.routing_key}:{hash(message.body)}"

    def _is_duplicate_message(self, message_fingerprint: str) -> bool:
        """
        Check if a message is a duplicate based on its fingerprint.
        Also performs cleanup of the message cache periodically.

        :param message_fingerprint: The unique fingerprint of the message
        :return: True if the message has been processed recently
        """
        # Clean up cache if it's been more than 1 hour
        current_time = datetime.utcnow()
        if (current_time - self._cache_cleanup_time).total_seconds() > 3600:
            self._cleanup_message_cache()
            self._cache_cleanup_time = current_time

        # Check if message fingerprint exists in our processed messages cache
        if message_fingerprint in self._processed_messages:
            cache_time = self._processed_messages[message_fingerprint]
            # Consider messages as duplicates if processed in the last 30 minutes
            if (current_time - cache_time).total_seconds() < 1800:  # 30 minutes
                return True

        return False

    def _record_processed_message(self, message_fingerprint: str) -> None:
        """
        Record a message as processed to detect future duplicates.

        :param message_fingerprint: The unique fingerprint of the message
        """
        self._processed_messages[message_fingerprint] = datetime.utcnow()

    def _cleanup_message_cache(self) -> None:
        """
        Clean up the message deduplication cache to prevent memory growth.
        Removes entries older than 30 minutes.
        """
        current_time = datetime.utcnow()
        # Create a list of keys to remove (messages older than 30 minutes)
        keys_to_remove = [
            fp
            for fp, timestamp in self._processed_messages.items()
            if (current_time - timestamp).total_seconds() > 1800  # 30 minutes
        ]

        # Remove old entries
        for key in keys_to_remove:
            del self._processed_messages[key]

        logger.info(
            f"Cleaned message deduplication cache, removed {len(keys_to_remove)} entries",
        )

    async def on_incident_notification(self, message: IncomingMessage) -> None:
        """
        Handle incoming incident notification messages from RabbitMQ.

        :param message: The RabbitMQ message containing incident notification
        """
        try:
            # Generate fingerprint for deduplication
            message_fingerprint = self._calculate_message_fingerprint(message)

            # Check if this message has already been processed
            if self._is_duplicate_message(message_fingerprint):
                logger.warning(
                    f"Received duplicate incident notification, skipping: {message.routing_key}",
                )
                await message.ack()  # Acknowledge duplicates to prevent redelivery loop
                return

            # Store message fingerprint to prevent duplicate processing
            self._record_processed_message(message_fingerprint)

            async with message.process():
                logger.info(
                    f"Received incident notification from RabbitMQ: {message.routing_key}",
                )
                await self.process_incident_notification(message.body.decode("utf-8"))
        except Exception as e:
            logger.error(f"Error processing incident notification: {str(e)}")
            # Acknowledge the message even if processing failed to prevent redelivery loop
            await message.ack()

    async def process_incident_notification(self, message_body: str) -> None:
        """
        Process an incident notification message.

        :param message_body: JSON string containing incident notification
        """
        try:
            incident_data = json.loads(message_body)
            logger.info(f"Processing incident notification: {incident_data}")

            # Optionally perform additional processing for incidents
            # For example, notify external systems, update dashboards, etc.

            # Example: Send a notification to Slack
            await self._send_incident_slack_notification(incident_data)

            # Example: Create a Jira ticket
            await self._create_incident_jira_ticket(incident_data)

            logger.info(
                f"Incident notification processed: {incident_data.get('incident_id')}",
            )
        except Exception as e:
            logger.error(f"Error processing incident notification: {str(e)}")

    async def _send_incident_slack_notification(
        self, incident_data: Dict[str, Any],
    ) -> None:
        """
        Send a notification to Slack for an incident.

        :param incident_data: The incident data
        """
        if not settings.slack_enabled:
            logger.warning("Slack notifications are disabled in settings")
            return

        if not settings.slack_bot_token:
            logger.error("Slack bot token is not configured")
            return

        if not settings.slack_channel_id:
            logger.error("Slack channel ID is not configured")
            return

        try:
            logger.info(
                f"Preparing to send Slack notification for incident: {incident_data.get('id')}",
            )

            # Initialize Slack client
            slack_client = SlackClient(
                token=settings.slack_bot_token,
                default_channel=settings.slack_channel_id,
                username=settings.slack_username,
                icon_emoji=settings.slack_icon_emoji,
            )

            # Get incident details
            incident_id = str(incident_data.get("id", 0))
            title = incident_data.get("title", "Unknown incident")
            description = incident_data.get("description", "")
            severity = incident_data.get("severity", "unknown")
            source = incident_data.get("original_event", {}).get("source", "unknown")

            logger.info(f"Slack notification details:")
            logger.info(f"- Incident ID: {incident_id}")
            logger.info(f"- Title: {title}")
            logger.info(f"- Severity: {severity}")
            logger.info(f"- Source: {source}")

            # Include source in description if not already mentioned
            if (
                source
                and source != "unknown"
                and source.lower() not in description.lower()
            ):
                description = f"[Source: {source}] {description}"

            # Map severity to AlertSeverity
            severity_map = {
                "critical": AlertSeverity.CRITICAL,
                "high": AlertSeverity.ERROR,
                "medium": AlertSeverity.WARNING,
                "low": AlertSeverity.INFO,
                # Default mappings
                "info": AlertSeverity.INFO,
                "warning": AlertSeverity.WARNING,
                "error": AlertSeverity.ERROR,
                "fatal": AlertSeverity.CRITICAL,
            }
            alert_severity = severity_map.get(severity.lower(), AlertSeverity.WARNING)

            # Use create_incident_notification instead of incident_detected
            template = SlackMessageTemplate.create_incident_notification(
                incident_id=incident_id,
                title=title,
                severity=alert_severity,
                description=description,
                timestamp=incident_data.get("detection_time", None),
            )

            # Send template directly
            logger.info(f"Sending Slack notification for incident #{incident_id}")
            response = await slack_client.send_template(template)

            if response and response.get("ok", False):
                logger.info(
                    f"Successfully sent Slack notification for incident #{incident_id}",
                )
            else:
                error_msg = (
                    response.get("error", "unknown") if response else "no response"
                )
                logger.error(f"Slack API returned error: {error_msg}")

        except Exception as e:
            logger.error(f"Failed to send Slack notification: {str(e)}", exc_info=True)

    async def _create_incident_jira_ticket(self, incident_data: Dict[str, Any]) -> None:
        """
        Create a JIRA ticket for an incident.

        :param incident_data: The incident data
        """
        if not settings.jira_enabled:
            logger.debug("JIRA integration is disabled")
            return

        try:
            jira_client = JiraClient(
                url=settings.jira_url,
                username=settings.jira_username,
                api_token=settings.jira_api_token,
                project_key=settings.jira_project_key,
                issue_type=settings.jira_issue_type,
            )

            # Get incident details
            incident_id = incident_data.get("id", 0)
            title = incident_data.get("title", "Unknown incident")
            description = incident_data.get("description", "")
            severity = incident_data.get("severity", "unknown")
            source = incident_data.get("original_event", {}).get("source", "unknown")
            event_type = incident_data.get("original_event", {}).get(
                "event_type", "unknown",
            )

            # Create JIRA ticket
            result = await jira_client.create_incident_ticket(
                incident_id=str(incident_id),
                title=title,
                description=description,
                severity=severity,
                source=source,
                event_type=event_type,
            )

            if "key" in result:
                logger.info(
                    f"Created JIRA ticket {result['key']} for incident #{incident_id}",
                )
            else:
                logger.error(f"Failed to create JIRA ticket: {result}")
        except Exception as e:
            logger.error(f"Failed to create JIRA ticket: {str(e)}")

    async def process_message(
        self, message_body: str, is_priority: bool = False,
    ) -> None:
        """
        Process a message from RabbitMQ.

        :param message_body: The message body as a JSON string
        :param is_priority: Whether the message is a priority message
        """
        try:
            logger.info("Starting to process message...")

            # Parse the message
            data = json.loads(message_body)
            logger.info(f"Parsed message data: {data}")

            # Extract key fields
            source = data.get("source", "unknown")
            event_type = data.get("event_type", "unknown")
            raw_payload = data.get("raw_payload", {})

            # Log event processing info
            logger.info(f"Processing {source} event of type {event_type}")

            # Create a database session for this request
            async with self.db_session_factory() as session:
                logger.info("Created database session")

                # Store event in the database
                event = await self.store_event(session, source, event_type, raw_payload)
                logger.info(f"Event storage result: {'Success' if event else 'Failed'}")

                if not event:
                    logger.warning("Failed to store event in database")
                    return

                logger.info(f"Event stored in database with ID {event.id}")

                # Check if this event is related to an existing incident
                incident_id = await self.find_relevant_incident(
                    session, source, event_type, raw_payload,
                )
                logger.info(f"Found relevant incident: {incident_id}")

                if incident_id:
                    logger.info(
                        f"Event associated with existing incident #{incident_id}",
                    )
                    # Update event with incident association
                    event.incident_id = incident_id
                    await session.commit()
                    logger.info("Updated event with incident association")

                    # Publish incident update to RabbitMQ
                    if self.rmq_channel_pool:
                        try:
                            # Get incident details
                            incident_query = select(Incident).where(
                                Incident.id == incident_id,
                            )
                            incident_result = await session.execute(incident_query)
                            incident = incident_result.scalars().first()

                            if incident:
                                # Create incident update message
                                await MessagePublisher.publish_incident_detection(
                                    self.rmq_channel_pool,
                                    data,  # Use the original event data
                                    incident_id,
                                    {
                                        "id": incident.id,
                                        "title": incident.title,
                                        "description": incident.description,
                                        "severity": incident.severity,
                                        "created_at": incident.created_at.isoformat(),
                                    },
                                )
                                logger.info(
                                    f"Published incident update to RabbitMQ for incident #{incident_id}",
                                )
                        except Exception as e:
                            logger.error(f"Failed to publish incident update: {str(e)}")
                else:
                    # No existing incident, check if we should create a new one
                    if self.auto_incident_creation:
                        logger.info("Checking for auto incident creation...")
                        incident = await self.check_for_auto_incident_creation(
                            session, event.id, source, event_type, raw_payload,
                        )

                        if incident:
                            logger.info(f"Created new incident #{incident.id}")
                            # Update event with incident association
                            event.incident_id = incident.id
                            await session.commit()
                            logger.info("Updated event with new incident association")

                            # Find and associate related events
                            related_count = await self._associate_related_events(
                                session, incident, source, event_type,
                            )
                            logger.info(
                                f"Associated {related_count} related events with incident #{incident.id}",
                            )

                            # Publish new incident to RabbitMQ
                            if self.rmq_channel_pool:
                                try:
                                    # Validate incident ID
                                    if incident.id is None or incident.id == 0:
                                        logger.error(
                                            f"Cannot publish notification for invalid incident ID: {incident.id}",
                                        )
                                        return

                                    # Create incident info with consistent ID
                                    incident_info = {
                                        "id": incident.id,
                                        "title": incident.title,
                                        "description": incident.description,
                                        "severity": incident.severity,
                                        "created_at": incident.created_at.isoformat(),
                                    }

                                    logger.info(
                                        f"Publishing incident notification, ID: {incident.id}, Title: {incident.title}",
                                    )

                                    # Include unique message identifier to prevent duplicate processing
                                    message_id = f"incident_{incident.id}_{int(datetime.utcnow().timestamp())}"
                                    incident_info["message_id"] = message_id

                                    await MessagePublisher.publish_incident_detection(
                                        self.rmq_channel_pool,
                                        data,  # Use the original event data
                                        incident.id,
                                        incident_info,
                                    )
                                    logger.info(
                                        f"Published new incident to RabbitMQ: #{incident.id}",
                                    )
                                except Exception as e:
                                    logger.error(
                                        f"Failed to publish new incident: {str(e)}",
                                    )
                        else:
                            logger.info("No incident was created")
                    else:
                        logger.info("Auto incident creation is disabled")

        except json.JSONDecodeError:
            logger.error("Invalid JSON in message body")
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")
            logger.exception("Detailed exception information:")

    async def store_event(
        self,
        session: AsyncSession,
        source: str,
        event_type: str,
        raw_payload: Dict[str, Any],
        incident_id: Optional[int] = None,
    ) -> Optional[Event]:
        """
        Store an event in the database.

        :param session: The database session
        :param source: The event source
        :param event_type: The event type
        :param raw_payload: The raw event payload
        :param incident_id: Optional ID of an associated incident
        :return: The created Event object, or None if creation failed
        """
        # Use transaction to ensure atomicity of operations
        try:
            # First, construct an event fingerprint to help identify duplicates
            event_fingerprint = self._generate_event_fingerprint(
                source, event_type, raw_payload,
            )
            logger.debug(f"Event fingerprint: {event_fingerprint}")

            # Extract event ID from raw payload for deduplication
            event_id = None
            if source == "datadog" and "alert_id" in raw_payload:
                event_id = raw_payload["alert_id"]
            elif "id" in raw_payload:
                event_id = raw_payload["id"]

            # Set a more generous time window for duplicate detection (30 minutes)
            time_threshold = datetime.utcnow() - timedelta(minutes=30)

            # Add source-specific query conditions
            query_conditions = [
                (Event.source == source),
                (Event.event_type == event_type),
                (Event.created_at > time_threshold),
            ]

            # Add source-specific filtering
            if source == "datadog":
                # For DataDog alerts, specifically check for duplicates with the same alert ID
                if "alert_id" in raw_payload:
                    # Construct a SQL query part that checks if this alert_id exists in the JSONB content
                    # Note: This is a simplified approach; for actual JSON queries you'd need database-specific syntax
                    logger.debug(
                        f"Checking for DataDog event with alert_id: {raw_payload['alert_id']}",
                    )
            elif source == "github":
                # For GitHub events, deduplicate based on the issue/PR number
                if "issue" in raw_payload and "number" in raw_payload["issue"]:
                    logger.debug(
                        f"Checking for GitHub issue with number: {raw_payload['issue']['number']}",
                    )
            elif source == "jira":
                # For Jira events, check issue key
                if "issue" in raw_payload and "key" in raw_payload["issue"]:
                    logger.debug(
                        f"Checking for Jira issue with key: {raw_payload['issue']['key']}",
                    )

            # Execute the query with our conditions
            stmt = select(Event).where(*query_conditions)
            result = await session.execute(stmt)
            existing_events = result.scalars().all()

            # Check for source-specific duplicates
            for existing_event in existing_events:
                try:
                    # Calculate similarity for deeper comparison
                    if self._is_duplicate_event(
                        existing_event, source, event_type, raw_payload, event_id,
                    ):
                        logger.info(
                            f"[DB] Found duplicate event: {source}/{event_type}, skipping insertion",
                        )
                        return existing_event
                except Exception as e:
                    logger.warning(f"Error comparing event for duplication: {str(e)}")

            # No duplicate found, proceed with storing the new event
            logger.info(f"[DB] Storing new event to database: {source}/{event_type}")

            # Find relevant incident ID if not provided
            if incident_id is None:
                incident_id = await self.find_relevant_incident(
                    session, source, event_type, raw_payload,
                )
                if incident_id:
                    logger.info(f"[DB] Found related incident: #{incident_id}")

            # Create event object with additional metadata for better tracing
            event = Event(
                incident_id=incident_id,
                source=source,
                event_type=event_type,
                content=raw_payload,
                created_at=datetime.utcnow(),
            )

            # Add to session
            session.add(event)

            # Commit with transaction management and retry logic
            try:
                await session.commit()
                logger.debug(f"[DB] Event stored successfully: {source}/{event_type}")
            except Exception as commit_error:
                logger.error(f"[DB] Transaction commit failed: {str(commit_error)}")
                await session.rollback()
                logger.info("[DB] Transaction rolled back")
                raise

            # Refresh to get generated ID
            await session.refresh(event)

            # Verify and log success
            if event and event.id:
                logger.info(f"[DB] Event stored with ID: {event.id}")
                return event
            else:
                logger.error("[DB] Event stored but no ID assigned")
                return None

        except Exception as e:
            logger.error(f"Error storing event: {str(e)}")
            return None

    def _generate_event_fingerprint(
        self, source: str, event_type: str, payload: Dict[str, Any],
    ) -> str:
        """
        Generate a fingerprint for an event to identify duplicates.

        :param source: Event source
        :param event_type: Event type
        :param payload: Event payload
        :return: A string fingerprint
        """
        # Start with basic identifiers
        fingerprint = f"{source}:{event_type}"

        # Add source-specific identifiers
        if source == "datadog":
            if "alert_id" in payload:
                fingerprint += f":alert_{payload['alert_id']}"
            if "alert" in payload and "status" in payload["alert"]:
                fingerprint += f":status_{payload['alert']['status']}"

        elif source == "github":
            if "repository" in payload and "id" in payload["repository"]:
                fingerprint += f":repo_{payload['repository']['id']}"
            if "issue" in payload and "number" in payload["issue"]:
                fingerprint += f":issue_{payload['issue']['number']}"
            elif "pull_request" in payload and "number" in payload["pull_request"]:
                fingerprint += f":pr_{payload['pull_request']['number']}"

        elif source == "jira":
            if "issue" in payload:
                issue = payload["issue"]
                if "key" in issue:
                    fingerprint += f":key_{issue['key']}"
                elif "id" in issue:
                    fingerprint += f":id_{issue['id']}"

        # Add timestamp if available for more precise identification
        if "created_at" in payload:
            fingerprint += f":time_{payload['created_at']}"
        elif "timestamp" in payload:
            fingerprint += f":time_{payload['timestamp']}"

        return fingerprint

    def _is_duplicate_event(
        self,
        existing_event: Event,
        source: str,
        event_type: str,
        payload: Dict[str, Any],
        event_id: Optional[str],
    ) -> bool:
        """
        Check if an event is a duplicate of an existing event.

        :param existing_event: The existing event from database
        :param source: Source of new event
        :param event_type: Type of new event
        :param payload: Payload of new event
        :param event_id: ID of new event if available
        :return: True if duplicate, False otherwise
        """
        # Check if event has the same type and source
        if existing_event.source != source or existing_event.event_type != event_type:
            return False

        # Get content from existing event
        existing_content = existing_event.content
        if not isinstance(existing_content, dict):
            return False

        # Check for exact ID match first
        if event_id and source == "datadog":
            if (
                "alert_id" in existing_content
                and existing_content["alert_id"] == event_id
            ):
                logger.debug(f"Duplicate detected - same DataDog alert_id: {event_id}")
                return True

        elif (
            event_id and "id" in existing_content and existing_content["id"] == event_id
        ):
            logger.debug(f"Duplicate detected - same ID: {event_id}")
            return True

        # Source-specific duplicate detection
        if source == "github":
            # For GitHub, check repository + issue/PR number
            if "repository" in payload and "repository" in existing_content:
                if payload["repository"].get("id") == existing_content[
                    "repository"
                ].get("id"):
                    # Same repo, check issue/PR number
                    if "issue" in payload and "issue" in existing_content:
                        if payload["issue"].get("number") == existing_content[
                            "issue"
                        ].get("number"):
                            return True
                    elif (
                        "pull_request" in payload and "pull_request" in existing_content
                    ):
                        if payload["pull_request"].get("number") == existing_content[
                            "pull_request"
                        ].get("number"):
                            return True

        elif source == "jira":
            # For Jira, check issue key/ID
            if "issue" in payload and "issue" in existing_content:
                if payload["issue"].get("key") == existing_content["issue"].get("key"):
                    return True
                if payload["issue"].get("id") == existing_content["issue"].get("id"):
                    return True

        elif source == "datadog":
            # DataDog more complex matching
            if "alert_id" in payload and "alert_id" in existing_content:
                if payload["alert_id"] == existing_content["alert_id"]:
                    # Same alert, check if it's a status update
                    if "status" in payload and "status" in existing_content:
                        # Different status = not a duplicate but an update
                        if payload["status"] != existing_content["status"]:
                            return False
                    return True

        # Check exact timestamps - same timestamp likely means duplicate
        for ts_field in ["timestamp", "created_at", "occurred_at"]:
            if (
                ts_field in payload
                and ts_field in existing_content
                and payload[ts_field] == existing_content[ts_field]
            ):
                logger.debug(
                    f"Duplicate detected - same timestamp: {ts_field}={payload[ts_field]}",
                )
                return True

        return False

    async def find_relevant_incident(
        self,
        session: AsyncSession,
        source: str,
        event_type: str,
        raw_payload: Dict[str, Any],
    ) -> Optional[int]:
        """
        Find a relevant incident for the event.

        This function uses both rule-based association and semantic similarity
        to find the most relevant open incident for this event.

        :param session: Database session
        :param source: Event source
        :param event_type: Event type
        :param raw_payload: Event payload
        :returns: Incident ID or None
        """
        logger.debug(f"Finding relevant incident for {source}/{event_type} event")

        try:
            # 1. First attempt to find open incidents from the same source that are recent
            query = (
                select(Incident)
                .where(Incident.source == source)
                .where(Incident.status.in_(["open", "investigating"]))
                .order_by(Incident.created_at.desc())
            )

            # Execute the query and get the first five recent open incidents (to limit comparison)
            result = await session.execute(query)
            recent_incidents = result.scalars().fetchmany(5)

            if not recent_incidents:
                logger.debug(f"No open {source} incidents found")
                return None

            # 2. Extract meaningful content from the event
            event_title = ""
            event_description = ""

            # Handle different sources differently to extract the most relevant content
            if source == "github":
                if "issue" in event_type:
                    event_title = raw_payload.get("issue", {}).get("title", "")
                    event_description = raw_payload.get("issue", {}).get("body", "")
                elif "pull_request" in event_type:
                    event_title = raw_payload.get("pull_request", {}).get("title", "")
                    event_description = raw_payload.get("pull_request", {}).get(
                        "body", "",
                    )
            elif source == "jira":
                event_title = (
                    raw_payload.get("issue", {}).get("fields", {}).get("summary", "")
                )
                event_description = (
                    raw_payload.get("issue", {})
                    .get("fields", {})
                    .get("description", "")
                )
            elif source == "datadog":
                event_title = raw_payload.get("title", "")
                event_description = raw_payload.get("message", "") or raw_payload.get(
                    "text", "",
                )
            elif source == "slack":
                event_description = raw_payload.get("text", "") or raw_payload.get(
                    "message", {},
                ).get("text", "")

            # If we couldn't extract meaningful content, fallback to simpler methods
            if not event_title and not event_description:
                # Simple check - if this is the first event of this type in last 24 hours,
                # associate with most recent incident of the same source
                most_recent = recent_incidents[0] if recent_incidents else None
                if most_recent:
                    logger.debug(
                        f"No meaningful content extracted, associating with most recent incident #{most_recent.id}",
                    )
                    return most_recent.id
                return None

            # 3. Try using the incident similarity service if available
            try:
                # Lazy import to avoid circular imports
                from system_guardian.services.ai.incident_similarity import (
                    IncidentSimilarityService,
                )
                from system_guardian.services.vector_db.qdrant_client import (
                    QdrantClient,
                )
                from openai import AsyncOpenAI
                from system_guardian.settings import settings

                # Create a combined text for the event
                event_text = f"{event_title}\n{event_description}"

                # Create the similarity service
                qdrant_client = QdrantClient(
                    host=settings.qdrant_host,
                    port=settings.qdrant_port,
                    api_key=settings.qdrant_api_key,
                )

                openai_client = AsyncOpenAI(api_key=settings.openai_api_key)

                similarity_service = IncidentSimilarityService(
                    qdrant_client=qdrant_client,
                    openai_client=openai_client,
                )

                # Get incident IDs to filter (only consider open incidents)
                open_incident_ids = [str(incident.id) for incident in recent_incidents]

                if open_incident_ids:
                    # Create filter condition
                    filter_condition = {
                        "must": [
                            {"key": "incident_id", "match": {"any": open_incident_ids}},
                        ],
                    }

                    # Find similar incidents
                    similar_incidents = await similarity_service.find_similar_incidents(
                        query_text=event_text,
                        limit=3,
                        filter_condition=filter_condition,
                    )

                    # Check if any incident has high similarity (threshold: 0.75)
                    for similar in similar_incidents:
                        if similar.get("similarity_score", 0) > 0.75:
                            incident_id = similar.get("incident_id")
                            if incident_id:
                                logger.info(
                                    f"Found similar incident #{incident_id} with score {similar['similarity_score']:.2f}",
                                )
                                return int(incident_id)

                    logger.debug(
                        f"No sufficiently similar incidents found with similarity search",
                    )
            except Exception as e:
                logger.warning(f"Error using similarity service: {str(e)}")

            # 4. Fallback: Basic keyword matching between event and incident titles
            best_match = None
            best_score = 0

            # Simple keyword matching algorithm
            for incident in recent_incidents:
                score = 0

                # Split titles into tokens
                incident_tokens = set(incident.title.lower().split())
                event_tokens = set(
                    event_title.lower().split()
                    if event_title
                    else event_description.lower().split(),
                )

                # Calculate Jaccard similarity (intersection / union)
                if incident_tokens and event_tokens:
                    intersection = len(incident_tokens.intersection(event_tokens))
                    union = len(incident_tokens.union(event_tokens))
                    if union > 0:
                        score = intersection / union

                if score > best_score and score > 0.3:  # Threshold of 0.3
                    best_score = score
                    best_match = incident

            if best_match:
                logger.info(
                    f"Found related incident #{best_match.id} with keyword matching score {best_score:.2f}",
                )
                return best_match.id

            # 5. No good match found
            logger.debug(f"No relevant incident found for {source}/{event_type} event")
            return None

        except Exception as e:
            logger.error(f"Error finding relevant incident: {str(e)}")
            return None

    async def check_for_auto_incident_creation(
        self,
        session: AsyncSession,
        event_id: int,
        source: str,
        event_type: str,
        payload: Dict[str, Any],
    ) -> Optional[Incident]:
        """
        Check if an incident should be automatically created for this event.

        :param session: Database session
        :param event_id: ID of the event
        :param source: Event source
        :param event_type: Event type
        :param payload: Event payload
        :returns: The created incident if applicable, None otherwise
        """
        try:
            logger.info(
                f"Checking for auto incident creation for event {event_id} ({source}/{event_type})",
            )

            # Get the event object
            stmt = select(Event).where(Event.id == event_id)
            result = await session.execute(stmt)
            event = result.scalar_one_or_none()

            if not event:
                logger.warning(
                    f"Event with ID {event_id} not found when checking for auto incident creation",
                )
                return None

            # Run incident detection logic
            should_create, reason, severity, title, description = (
                await self._run_incident_detection(source, event_type, payload)
            )

            logger.info(f"Incident detection result:")
            logger.info(f"- Should create: {should_create}")
            logger.info(f"- Reason: {reason}")
            logger.info(f"- Severity: {severity}")
            logger.info(f"- Title: {title}")
            logger.info(f"- Description: {description}")

            if should_create:
                logger.info(
                    f"Auto-creating incident for {source} {event_type} event. Reason: {reason}",
                )

                # Create the incident
                incident = Incident(
                    title=title or f"Incident from {source} {event_type}",
                    description=description
                    or f"Automatically generated incident from {source} {event_type} event.",
                    status="open",
                    severity=severity or "medium",
                    source=source,
                    created_at=datetime.utcnow(),
                )

                # Add and commit the incident
                session.add(incident)
                await session.commit()
                await session.refresh(incident)

                # Associate the event with this incident
                event.incident_id = incident.id
                await session.commit()

                logger.info(
                    f"Created incident #{incident.id} from {source} {event_type} event",
                )

                # Direct Slack notification for new incidents - backup to ensure notification is sent
                # This is in addition to the RabbitMQ notification that will be published later
                if settings.slack_enabled:
                    try:
                        logger.info(
                            f"Sending direct Slack notification for new incident #{incident.id}",
                        )
                        await self._send_incident_slack_notification(
                            {
                                "id": incident.id,
                                "title": incident.title,
                                "description": incident.description,
                                "severity": incident.severity,
                                "created_at": incident.created_at.isoformat(),
                                "original_event": {"source": source},
                            },
                        )
                        logger.info(
                            f"Direct Slack notification for incident #{incident.id} sent successfully",
                        )
                    except Exception as e:
                        logger.error(
                            f"Failed to send direct Slack notification: {str(e)}",
                        )
                        # Continue processing even if Slack notification fails

                return incident
            else:
                logger.info(
                    f"No incident created for {source} {event_type} event. Reason: {reason}",
                )
                return None

        except Exception as e:
            logger.error(f"Error in auto incident detection: {str(e)}")
            return None

    async def _run_incident_detection(
        self, source: str, event_type: str, payload: Dict[str, Any],
    ) -> Tuple[bool, str, str, str, str]:
        """
        Run the actual incident detection logic.

        :param source: Event source
        :param event_type: Event type
        :param payload: Event payload
        :returns: Tuple of (should_create, reason, severity, title, description)
        """
        try:
            logger.info(f"Running incident detection for {source}/{event_type}")

            # 使用 incident_detector 進行檢測
            should_create = await self.incident_detector.check_event_conditions(
                payload, source, event_type,
            )

            if should_create:
                logger.info(f"Incident detected for {source}/{event_type}")

                # 使用 severity_classifier 判斷嚴重性
                severity = await self.severity_classifier.classify_severity(
                    source, event_type, payload,
                )
                logger.info(f"Classified severity as: {severity}")

                # 生成標題和描述
                title, description = self.incident_detector._extract_title_description(
                    source, event_type, payload,
                )
                logger.info(f"Generated title: {title}")
                logger.info(f"Generated description: {description}")

                return (
                    True,
                    "Incident detected by AI engine",
                    severity,
                    title,
                    description,
                )

            logger.info(f"No incident detected for {source}/{event_type}")
            return False, "No incident detection rules matched", None, None, None

        except Exception as e:
            logger.error(f"Error in incident detection: {str(e)}")
            return False, f"Error in detection: {str(e)}", None, None, None

    async def _associate_related_events(
        self,
        session: AsyncSession,
        incident: Incident,
        event_source: str,
        event_type: str,
    ) -> int:
        """
        Associate related events with an incident.

        This method finds events matching the source and type that are not
        already associated with an incident, and links them to this incident.

        :param session: Database session
        :param incident: The incident to associate events with
        :param event_source: Source of events to associate
        :param event_type: Type of events to associate
        :return: Number of events associated
        """
        # Find events of the same type that are not associated with an incident
        stmt = select(Event).where(
            (Event.source == event_source)
            & (Event.event_type == event_type)
            & (Event.incident_id.is_(None))
            & (
                Event.created_at >= datetime.utcnow() - timedelta(days=1)
            ),  # Limit to last 24 hours
        )

        result = await session.execute(stmt)
        events = result.scalars().all()

        logger.info(
            f"Associating {len(events)} {event_source}/{event_type} events with incident #{incident.id}",
        )

        # Associate events with this incident
        for event in events:
            event.incident_id = incident.id
            logger.debug(
                f"[DB] Associating event ID={event.id} to incident #{incident.id}",
            )

        # Commit the changes
        if events:
            await session.commit()
            logger.debug(
                f"[DB] Event association update committed successfully: {len(events)} events linked to incident #{incident.id}",
            )

        return len(events)
