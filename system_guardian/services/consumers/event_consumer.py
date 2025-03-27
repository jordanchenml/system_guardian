"""Event consumer for processing events from message queues."""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple

from loguru import logger
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
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
    """Consumer for processing events from Kafka and RabbitMQ."""

    # Critical event types that should be processed with higher priority
    CRITICAL_EVENT_TYPES = [
        "error",
        "failure",
        "alert",
        "security",
        "outage",
        "incident",
        "critical",
        "emergency",
        "urgent",
    ]

    def __init__(
        self,
        db_session_factory,
        kafka_topics: Optional[List[str]] = None,
        rmq_exchange: str = "webhook_events",
        rmq_queue: str = "webhook_events_queue",
        rmq_routing_keys: Optional[List[str]] = None,
        auto_incident_creation: bool = True,
        ai_engine=None,
        kafka_producer=None,
        rmq_channel_pool=None,
    ):
        """
        Initialize the event consumer.

        :param db_session_factory: Factory for creating database sessions
        :param kafka_topics: List of Kafka topics to consume from
        :param rmq_exchange: RabbitMQ exchange name
        :param rmq_queue: RabbitMQ queue name
        :param rmq_routing_keys: List of RabbitMQ routing keys to bind
        :param auto_incident_creation: Whether to automatically create incidents from events
        :param ai_engine: Optional AIEngine instance for enhanced event processing
        :param kafka_producer: Optional pre-initialized Kafka producer
        :param rmq_channel_pool: Optional pre-initialized RabbitMQ channel pool
        """
        self.db_session_factory = db_session_factory
        self.kafka_topics = kafka_topics or [
            "github_events",
            "jira_events",
            "datadog_events",
            "webhook_events",
            "system_incidents",
        ]
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

        # 存儲已初始化的消息隊列客戶端
        self.kafka_producer = kafka_producer
        self.rmq_channel_pool = rmq_channel_pool

    async def start(self) -> None:
        """Start consuming messages from RabbitMQ."""
        # 只啟動RabbitMQ消費者，不啟動Kafka消費者
        # 保留Kafka相關代碼，但不實際啟用
        consumers = [
            self.start_rabbitmq_consumer(),
            # 不使用Kafka作為事件處理通道
            # self.start_kafka_consumer(),
        ]

        # Run RabbitMQ consumer
        await asyncio.gather(*consumers)

    async def stop(self) -> None:
        """Stop all consumers."""
        self.should_exit = True
        logger.info("Event consumer stopping")

    async def start_kafka_consumer(self) -> None:
        """Start consuming messages from Kafka topics."""
        logger.info(f"Starting Kafka consumer for topics: {self.kafka_topics}")

        consumer = AIOKafkaConsumer(
            *self.kafka_topics,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id="system_guardian_event_consumer",
            auto_offset_reset="latest",  # Start consuming from the latest offset
            enable_auto_commit=False,  # Manual commit for better control
        )

        # If no Kafka producer is provided, try to create one locally
        if self.kafka_producer is None:
            try:
                # No longer using FastAPI dependencies
                logger.info(
                    "No Kafka producer provided, attempting to create one locally"
                )
                # Using locally created Kafka producer
                from aiokafka import AIOKafkaProducer

                self.kafka_producer = AIOKafkaProducer(
                    bootstrap_servers=settings.kafka_bootstrap_servers,
                    client_id="system_guardian_event_consumer_local",
                )
                await self.kafka_producer.start()
                logger.info("Created local Kafka producer successfully")
            except Exception as e:
                logger.error(f"Failed to create Kafka producer: {str(e)}")
                logger.warning("Incident notifications to Kafka will be disabled")

        await consumer.start()

        try:
            # Consume messages in an infinite loop
            while not self.should_exit:
                try:
                    # Fetch messages with a timeout
                    batch = await consumer.getmany(timeout_ms=1000)

                    for tp, messages in batch.items():
                        logger.info(
                            f"Received {len(messages)} messages from Kafka topic: {tp.topic}"
                        )

                        # Process regular event topics differently from system topics
                        if tp.topic == "system_incidents":
                            for message in messages:
                                await self.process_incident_notification(
                                    message.value.decode("utf-8")
                                )
                        else:
                            for message in messages:
                                # Process each regular event message
                                await self.process_message(
                                    message.value.decode("utf-8")
                                )

                    # Commit offsets for the batch
                    await consumer.commit()

                except Exception as e:
                    logger.error(f"Error processing Kafka message: {str(e)}")

        finally:
            # Clean up
            await consumer.stop()
            logger.info("Kafka consumer stopped")

    async def start_rabbitmq_consumer(self) -> None:
        """Start consuming messages from RabbitMQ."""
        logger.info(f"Starting RabbitMQ consumer for exchange: {self.rmq_exchange}")

        try:
            # Create connection
            logger.info(
                f"Attempting to connect to RabbitMQ: host={settings.rabbit_host}, port={settings.rabbit_port}"
            )
            connection = await connect_robust(
                host=settings.rabbit_host,
                port=settings.rabbit_port,
                login=settings.rabbit_user,
                password=settings.rabbit_pass,
                virtualhost=settings.rabbit_vhost,
            )
            logger.info("Successfully connected to RabbitMQ")

            # Create channel
            logger.info("Creating RabbitMQ channel...")
            channel = await connection.channel()
            logger.info("RabbitMQ channel created successfully")

            # If no RabbitMQ channel pool is provided, try to create one locally
            if self.rmq_channel_pool is None:
                try:
                    # No longer using FastAPI dependencies
                    logger.warning(
                        "No RabbitMQ channel pool provided, attempting to create one locally"
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
                        get_local_connection, max_size=settings.rabbit_pool_size
                    )

                    async def get_local_channel():
                        """Get local RabbitMQ channel"""
                        async with connection_pool.acquire() as connection:
                            return await connection.channel()

                    self.rmq_channel_pool = Pool(
                        get_local_channel, max_size=settings.rabbit_channel_pool_size
                    )
                    logger.info("Successfully created local RabbitMQ channel pool")
                except Exception as e:
                    logger.error(f"Failed to create RabbitMQ channel pool: {str(e)}")
                    logger.exception("Detailed error information:")
                    logger.warning("RabbitMQ incident notifications will be disabled")

            # ===== Main RabbitMQ consumer setup =====

            # 1. Setup regular events exchange and queue
            logger.info(f"Declaring webhook events exchange: {self.rmq_exchange}")
            webhook_exchange = await channel.declare_exchange(
                name=self.rmq_exchange,
                auto_delete=True,
            )

            logger.info(f"Declaring webhook events queue: {self.rmq_queue}")
            webhook_queue = await channel.declare_queue(
                name=self.rmq_queue,
                durable=True,
                auto_delete=False,
            )

            # 2. Bind queue to exchange with routing keys
            for routing_key in self.rmq_routing_keys:
                logger.info(f"Binding queue to routing key: {routing_key}")
                await webhook_queue.bind(
                    exchange=webhook_exchange,
                    routing_key=routing_key,
                )

            # 3. Set up message consumer
            logger.info("Setting up RabbitMQ message consumer...")

            async def process_webhook_message(message: IncomingMessage) -> None:
                """Process a webhook message."""
                async with message.process():
                    # Debug log of message information
                    logger.debug(
                        f"Received RabbitMQ message: routing_key={message.routing_key}, message_id={message.message_id}"
                    )

                    # Process message body
                    try:
                        body = message.body.decode("utf-8")
                        # Check if message is valid JSON
                        if len(body) < 5:  # Quick sanity check
                            logger.warning(
                                f"Received too short message content (length: {len(body)}), skipping processing"
                            )
                            return

                        # Determine if this is a priority message based on routing key or content
                        is_priority = any(
                            critical_type in message.routing_key.lower()
                            for critical_type in self.CRITICAL_EVENT_TYPES
                        )

                        # DEBUG: Add detailed message logs
                        logger.debug(f"Message body sample: {body[:200]}...")

                        # Process the message
                        await self.process_message(body, is_priority=is_priority)
                    except UnicodeDecodeError as ude:
                        logger.error(f"Error decoding message body: {str(ude)}")
                        logger.debug(f"Message body (raw bytes): {message.body[:100]}")
                    except Exception as e:
                        logger.error(f"Error processing RabbitMQ message: {str(e)}")
                        logger.exception("Detailed error information:")

            # Start consuming
            logger.info(f"Starting to consume messages from queue {self.rmq_queue}")
            await webhook_queue.consume(process_webhook_message)

            # 4. Setup incidents exchange and queue for notification
            logger.info("Setting up incident exchange and queue...")

            incidents_exchange = await channel.declare_exchange(
                name="system_incidents",
                auto_delete=False,
            )

            incidents_queue = await channel.declare_queue(
                name="system_incidents_queue",
                durable=True,
                auto_delete=False,
            )

            await incidents_queue.bind(
                exchange=incidents_exchange,
                routing_key="incidents.*",
            )

            # Incident notification handler
            async def process_incident_notification(message: IncomingMessage) -> None:
                """Process an incident notification message."""
                async with message.process():
                    try:
                        logger.info(
                            f"Received incident notification: routing_key={message.routing_key}"
                        )
                        body = message.body.decode("utf-8")
                        await self.process_incident_notification(body)
                    except Exception as e:
                        logger.error(
                            f"Error processing incident notification: {str(e)}"
                        )
                        logger.exception("Detailed error information:")

            # Start consuming incidents
            logger.info("Starting to consume messages from incident queue")
            await incidents_queue.consume(process_incident_notification)

            # Keep alive until should_exit flag is set
            logger.info("RabbitMQ consumer started and running")
            while not self.should_exit:
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"Error starting RabbitMQ consumer: {str(e)}")
            logger.exception("Detailed error information:")

            if not self.should_exit:
                # Try to reconnect after a delay if not explicitly stopped
                retry_delay = 5
                logger.info(f"Will attempt to reconnect in {retry_delay} seconds...")
                await asyncio.sleep(retry_delay)
                await self.start_rabbitmq_consumer()  # Recursive restart

    async def process_incident_notification(self, message_body: str) -> None:
        """
        Process an incident notification message.

        :param message_body: The notification body as a string
        """
        try:
            logger.info(
                f"Processing incident notification, content length: {len(message_body)} characters"
            )

            # Parse JSON message
            try:
                incident_data = json.loads(message_body)
            except json.JSONDecodeError as json_error:
                logger.error(
                    f"Error parsing incident notification JSON: {str(json_error)}"
                )
                logger.error(f"Invalid JSON data: {message_body[:500]}")
                return

            # Extract incident information
            incident_id = incident_data.get("incident_id")
            if not incident_id:
                logger.warning("Incident notification missing incident_id field")
                return

            # Process Slack notifications if enabled
            if settings.slack_enabled and settings.slack_bot_token:
                await self.send_slack_notification(incident_data)

            # Process Jira ticket creation if enabled
            if settings.jira_enabled and settings.jira_url:
                await self.create_jira_ticket(incident_data)

        except Exception as e:
            logger.error(f"Error processing incident notification: {str(e)}")
            logger.exception("Detailed error information:")

    async def process_message(
        self, message_body: str, is_priority: bool = False
    ) -> None:
        """
        Process a message from either Kafka or RabbitMQ.

        :param message_body: The message body as a string
        :param is_priority: Whether this is a priority message
        """
        # Add prefix for logs to identify the message type
        message_prefix = "[PRIORITY]" if is_priority else "[STD]"

        try:
            # Log basic message info - using more concise log format
            logger.info(
                f"{message_prefix} Processing message, content length: {len(message_body)} characters"
            )

            try:
                # Parse JSON message
                logger.debug(
                    f"{message_prefix} Original message content: {message_body[:200]}..."
                )
                try:
                    message_dict = json.loads(message_body)
                except json.JSONDecodeError as json_error:
                    logger.error(
                        f"{message_prefix} JSON parsing error: {str(json_error)}"
                    )
                    logger.error(
                        f"{message_prefix} Invalid JSON data: {message_body[:500]}"
                    )
                    return

                # Extract required fields
                source = message_dict.get("source", "unknown")
                event_type = message_dict.get("event_type", "unknown")
                event_id = message_dict.get("event_id", "unknown")
                raw_payload = message_dict.get("raw_payload", {})

                logger.info(
                    f"{message_prefix} Message content: source={source}, type={event_type}, id={event_id}"
                )

                # Add more detailed logging for Datadog events
                if source == "datadog":
                    logger.info(
                        f"{message_prefix} Processing Datadog event: source={source}, type={event_type}, id={event_id}"
                    )

                # Check for well-formed message (essential fields)
                if (
                    not source
                    or source == "unknown"
                    or not event_type
                    or event_type == "unknown"
                ):
                    logger.warning(
                        f"{message_prefix} Invalid message format, missing source or event_type"
                    )
                    return

                # Get session from the sessionmaker
                # Simplified database operation logging
                logger.debug(
                    f"{message_prefix} Creating database session for event {source}/{event_type}"
                )
                async with self.db_session_factory() as session:
                    # Store to database first (even if no incident)
                    logger.info(
                        f"{message_prefix} Storing event to database: {source}/{event_type}"
                    )

                    # Special handling for Datadog events
                    if source == "datadog":
                        logger.info(
                            f"{message_prefix} Datadog event original data sample: {str(raw_payload)[:200]}..."
                        )

                    # DEBUG: Print raw payload type
                    logger.debug(
                        f"{message_prefix} Raw payload type: {type(raw_payload)}"
                    )
                    logger.debug(
                        f"{message_prefix} Raw payload sample: {str(raw_payload)[:200]}..."
                    )

                    # 嘗試存儲事件
                    event = await self.store_event(
                        session, source, event_type, raw_payload
                    )

                    if not event:
                        logger.error(
                            f"{message_prefix} Unable to store event {source}/{event_type}"
                        )
                        return

                    logger.info(
                        f"{message_prefix} Stored event {source}/{event_type}, ID: {event.id}"
                    )

                    # Check if we need to create an incident
                    if not event.incident_id:  # Not already linked
                        logger.debug(
                            f"{message_prefix} Checking if incident should be created for event ID {event.id}"
                        )

                        # Use any available incident detector
                        detector = self.incident_detector

                        logger.debug(
                            f"{message_prefix} Using incident_detector to check event ID {event.id}"
                        )

                        # Check if this event meets basic conditions for an incident
                        conditions_met = False
                        try:
                            conditions_met = await detector.check_event_conditions(
                                event.content, event.source, event.event_type
                            )
                            logger.info(
                                f"{message_prefix} Event condition check result: {conditions_met}"
                            )
                        except Exception as conditions_error:
                            logger.error(
                                f"{message_prefix} Error checking event conditions: {str(conditions_error)}"
                            )
                            logger.exception("Detailed error information:")

                        if conditions_met:
                            logger.info(
                                f"{message_prefix} Event ID {event.id} meets conditions for incident creation"
                            )

                            # Use appropriate methods to determine whether to create an incident
                            # Check if thresholds are exceeded
                            threshold_breach = False
                            has_keywords = False

                            try:
                                threshold_breach = (
                                    await detector.check_event_thresholds(
                                        session, event.source, event.event_type
                                    )
                                )
                                logger.info(
                                    f"{message_prefix} Threshold check result: {threshold_breach}"
                                )
                            except Exception as threshold_error:
                                logger.error(
                                    f"{message_prefix} Error checking event thresholds: {str(threshold_error)}"
                                )
                                logger.exception("Detailed error information:")

                            # Check for keywords
                            try:
                                has_keywords = (
                                    await detector.analyze_content_for_keywords(
                                        event.content, event.source, event.event_type
                                    )
                                )
                                logger.info(
                                    f"{message_prefix} Keyword check result: {has_keywords}"
                                )
                            except Exception as keyword_error:
                                logger.error(
                                    f"{message_prefix} Error analyzing content for keywords: {str(keyword_error)}"
                                )
                                logger.exception("Detailed error information:")

                            # If any detection method is triggered, create an incident
                            should_create = threshold_breach or has_keywords

                            if should_create:
                                logger.info(
                                    f"{message_prefix} Creating incident for event {event.source}/{event.event_type}"
                                )

                                # Create incident
                                incident = None
                                try:
                                    incident = (
                                        await detector.create_incident_from_event(
                                            session,
                                            event.source,
                                            event.event_type,
                                            event.content,
                                            event.id,
                                        )
                                    )
                                except Exception as create_error:
                                    logger.error(
                                        f"{message_prefix} Error creating incident: {str(create_error)}"
                                    )
                                    logger.exception("Detailed error information:")

                                if not incident:
                                    logger.error(
                                        f"{message_prefix} Unable to create incident for event ID {event.id}"
                                    )
                                else:
                                    logger.info(
                                        f"{message_prefix} Created incident ID {incident.id}"
                                    )

                                    # 3. If incident was successfully created, publish notification
                                    if incident:
                                        try:
                                            logger.info(
                                                f"{message_prefix} Creating notification for incident ID {incident.id}"
                                            )

                                            # Dynamic import to avoid circular imports
                                            from system_guardian.web.api.ingest.schema import (
                                                StandardEventMessage,
                                            )

                                            event_message = StandardEventMessage(
                                                source=event.source,
                                                event_type=event.event_type,
                                                event_id=str(event.id),
                                                timestamp=event.created_at,
                                                raw_payload=event.content,
                                            )

                                            # Extract incident info for notification
                                            incident_info = {
                                                "created_at": (
                                                    incident.created_at.isoformat()
                                                    if incident.created_at
                                                    else datetime.utcnow().isoformat()
                                                ),
                                                "severity": incident.severity,
                                                "title": incident.title,
                                                "description": incident.description,
                                            }

                                            logger.info(
                                                f"{message_prefix} Publishing incident ID {incident.id} notification"
                                            )

                                            try:
                                                # 修改：僅使用RabbitMQ發送事件，不使用Kafka
                                                await MessagePublisher.publish_incident_detection(
                                                    kafka_producer=None,  # 設置為None，不使用Kafka發布
                                                    rmq_channel_pool=self.rmq_channel_pool,
                                                    event_message=event_message,
                                                    incident_id=incident.id,
                                                    incident_info=incident_info,
                                                )
                                                logger.info(
                                                    f"{message_prefix} Successfully published incident ID {incident.id} notification"
                                                )
                                            except Exception as publish_error:
                                                logger.error(
                                                    f"{message_prefix} Error publishing incident ID {incident.id} notification: {str(publish_error)}"
                                                )
                                                logger.exception(
                                                    "Detailed error information:"
                                                )

                                            logger.info(
                                                f"{message_prefix} Completed incident ID {incident.id} creation and notification"
                                            )
                                        except Exception as e:
                                            logger.error(
                                                f"{message_prefix} Error publishing incident ID {incident.id} notification: {str(e)}"
                                            )
                                            logger.exception(
                                                "Detailed error information:"
                                            )
            except Exception as session_error:
                logger.error(
                    f"{message_prefix} Database session error: {str(session_error)}"
                )
                logger.exception("Detailed error information:")

        except Exception as e:
            logger.error(f"Unhandled error processing message: {str(e)}")
            logger.exception("Detailed error information:")

    async def store_event(
        self,
        session: AsyncSession,
        source: str,
        event_type: str,
        raw_payload: Dict[str, Any],
        incident_id: Optional[int] = None,
    ) -> Optional[Event]:
        """
        Store an event to the database.

        :param session: Database session
        :param source: Event source
        :param event_type: Event type
        :param raw_payload: Event payload
        :param incident_id: Optional incident ID to associate with
        :returns: The created event
        """
        try:
            logger.info(
                f"[DB] Attempting to store event: {source}/{event_type}, payload size: {len(str(raw_payload))}"
            )

            # Check raw_payload type
            logger.debug(f"[DB] raw_payload type: {type(raw_payload)}")

            # Implement improved deduplication logic
            # Only consider an event duplicate if source, event_type, AND content are the same

            # Define a time window for deduplication check (5 minutes)
            time_threshold = datetime.utcnow() - timedelta(minutes=5)

            # Build query to find events with the same source and event_type within the time window
            stmt = select(Event).where(
                (Event.source == source)
                & (Event.event_type == event_type)
                & (Event.created_at > time_threshold)
            )

            # Execute the query
            result = await session.execute(stmt)
            existing_events = result.scalars().all()

            # Check if any existing event has identical content
            raw_payload_str = (
                json.dumps(raw_payload, sort_keys=True)
                if isinstance(raw_payload, dict)
                else str(raw_payload)
            )

            for existing_event in existing_events:
                try:
                    # Convert existing event content to string for comparison
                    existing_content = existing_event.content
                    existing_content_str = (
                        json.dumps(existing_content, sort_keys=True)
                        if isinstance(existing_content, dict)
                        else str(existing_content)
                    )

                    # Compare content strings
                    if existing_content_str == raw_payload_str:
                        logger.info(
                            f"[DB] Found duplicate event: {source}/{event_type}, ID: {existing_event.id}"
                        )
                        return existing_event
                except Exception as e:
                    # If content comparison fails, log and continue checking other events
                    logger.warning(
                        f"[DB] Content comparison failed for event ID {existing_event.id}: {str(e)}"
                    )
                    continue

            # If no duplicate found, proceed with storing the new event
            logger.info(
                f"[DB] No duplicate found. Storing event to database: {source}/{event_type}"
            )

            # If no explicit incident_id, try to find relevant incident
            if incident_id is None:
                incident_id = await self.find_relevant_incident(
                    session, source, event_type, raw_payload
                )
                if incident_id:
                    logger.info(f"[DB] Found relevant incident: #{incident_id}")

            # Create event object
            event = Event(
                incident_id=incident_id,
                source=source,
                event_type=event_type,
                content=raw_payload,
                created_at=datetime.utcnow(),
            )

            # Add to session
            session.add(event)

            # Add more detailed logging
            logger.debug(
                f"[DB] Executing insert operation: event[{source}/{event_type}], object ID: {id(event)}"
            )

            # Commit transaction
            try:
                await session.commit()
                logger.info(
                    f"[DB] Transaction committed successfully: event[{source}/{event_type}]"
                )
            except Exception as commit_error:
                logger.error(f"[DB] Transaction commit failed: {str(commit_error)}")
                logger.exception("[DB] Detailed error information:")
                # Try to rollback transaction
                try:
                    await session.rollback()
                    logger.info(f"[DB] Transaction rolled back")
                except Exception as rollback_error:
                    logger.error(
                        f"[DB] Transaction rollback failed: {str(rollback_error)}"
                    )
                raise commit_error

            # Reload event object to get latest data (like auto-generated ID)
            try:
                await session.refresh(event)
                logger.info(f"[DB] Event object successfully reloaded: ID={event.id}")
            except Exception as refresh_error:
                logger.error(
                    f"[DB] Unable to reload event object: {str(refresh_error)}"
                )
                logger.exception("[DB] Detailed error information:")
                # Try to get event by query
                try:
                    stmt = (
                        select(Event)
                        .where(
                            (Event.source == source)
                            & (Event.event_type == event_type)
                            & (
                                Event.created_at
                                > (datetime.utcnow() - timedelta(minutes=1))
                            )
                        )
                        .order_by(Event.created_at.desc())
                    )
                    result = await session.execute(stmt)
                    event = result.scalar_one_or_none()
                    if event:
                        logger.info(
                            f"[DB] Found recently created event via query, ID: {event.id}"
                        )
                    else:
                        logger.warning(
                            f"[DB] Unable to find recently created event via query"
                        )
                except Exception as query_error:
                    logger.error(
                        f"[DB] Unable to get event via query: {str(query_error)}"
                    )
                    raise refresh_error

            # Verify event was created and has ID
            if event and event.id:
                logger.info(
                    f"[DB] Successfully stored {source} event, type: {event_type}, ID: {event.id}"
                )
                return event
            else:
                logger.error(f"[DB] Event seems stored but no ID assigned")
                return None

        except Exception as e:
            logger.error(f"[DB] Error storing event {source}/{event_type}: {str(e)}")
            logger.exception("[DB] Detailed error information:")
            return None

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
                        "body", ""
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
                    "text", ""
                )
            elif source == "slack":
                event_description = raw_payload.get("text", "") or raw_payload.get(
                    "message", {}
                ).get("text", "")

            # If we couldn't extract meaningful content, fallback to simpler methods
            if not event_title and not event_description:
                # Simple check - if this is the first event of this type in last 24 hours,
                # associate with most recent incident of the same source
                most_recent = recent_incidents[0] if recent_incidents else None
                if most_recent:
                    logger.debug(
                        f"No meaningful content extracted, associating with most recent incident #{most_recent.id}"
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
                            {"key": "incident_id", "match": {"any": open_incident_ids}}
                        ]
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
                                    f"Found similar incident #{incident_id} with score {similar['similarity_score']:.2f}"
                                )
                                return int(incident_id)

                    logger.debug(
                        f"No sufficiently similar incidents found with similarity search"
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
                    else event_description.lower().split()
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
                    f"Found related incident #{best_match.id} with keyword matching score {best_score:.2f}"
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
            # Get the event object
            stmt = select(Event).where(Event.id == event_id)
            result = await session.execute(stmt)
            event = result.scalar_one_or_none()

            if not event:
                logger.warning(
                    f"Event with ID {event_id} not found when checking for auto incident creation"
                )
                return None

            # Run incident detection logic
            should_create, reason, severity, title, description = (
                await self._run_incident_detection(source, event_type, payload)
            )

            if should_create:
                logger.info(
                    f"Auto-creating incident for {source} {event_type} event. Reason: {reason}"
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
                    f"Created incident #{incident.id} from {source} {event_type} event"
                )
                return incident
            else:
                logger.debug(
                    f"No incident created for {source} {event_type} event. Reason: {reason}"
                )
                return None

        except Exception as e:
            logger.error(f"Error in auto incident detection: {str(e)}")
            return None

    async def _run_incident_detection(
        self, source: str, event_type: str, payload: Dict[str, Any]
    ) -> Tuple[bool, str, str, str, str]:
        """
        Run the actual incident detection logic.

        :param source: Event source
        :param event_type: Event type
        :param payload: Event payload
        :returns: Tuple of (should_create, reason, severity, title, description)
        """
        # Implement your incident detection logic here
        # This is a placeholder implementation
        # In a real implementation, you would use self.incident_detector

        # Check if this is a critical event type
        is_critical = any(
            critical_type in event_type.lower()
            for critical_type in [
                "error",
                "failure",
                "alert",
                "security",
                "outage",
                "incident",
            ]
        )

        if is_critical:
            return (
                True,
                "Critical event type detected",
                "high",
                f"Critical {event_type} from {source}",
                f"Automatic incident created from critical {source} {event_type} event",
            )

        # Use the incident detector for more advanced detection
        # This would be implemented in the IncidentDetector class

        return False, "No incident detection rules matched", None, None, None

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
            )  # Limit to last 24 hours
        )

        result = await session.execute(stmt)
        events = result.scalars().all()

        logger.info(
            f"Associating {len(events)} {event_source}/{event_type} events with incident #{incident.id}"
        )

        # Associate events with this incident
        for event in events:
            event.incident_id = incident.id
            logger.debug(
                f"[DB] Associating event ID={event.id} to incident #{incident.id}"
            )

        # Commit the changes
        if events:
            await session.commit()
            logger.debug(
                f"[DB] Event association update committed successfully: {len(events)} events linked to incident #{incident.id}"
            )

        return len(events)

    async def send_slack_notification(self, incident_data: Dict[str, Any]) -> None:
        """
        Send a Slack notification for an incident.

        :param incident_data: Incident data from notification
        """
        try:
            # Create SlackClient
            slack_client = SlackClient()

            # Skip if Slack is not configured
            if not slack_client.is_configured:
                logger.warning(
                    "Slack notification disabled, skipping event notification"
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
            incident_id = incident_data.get("incident_id", "unknown")
            title = incident_data.get("title", "Unnamed Event")
            description = incident_data.get("description", "No description provided")
            severity_str = incident_data.get("severity", "medium").lower()
            detection_time = incident_data.get(
                "detection_time", datetime.utcnow().isoformat()
            )

            # Map severity to AlertSeverity enum
            severity = severity_map.get(severity_str, AlertSeverity.WARNING)

            # Get source information if available
            original_event = incident_data.get("original_event", {})
            source = original_event.get("source", "unknown")
            event_type = original_event.get("event_type", "unknown")

            # Add source info to description if available
            if source != "unknown" and event_type != "unknown":
                source_info = f"\n\nSource: {source}\nEvent Type: {event_type}"
                description += source_info

            # Send notification using incident template
            logger.info(f"Sending incident ID {incident_id} Slack notification")
            template = SlackMessageTemplate.create_incident_notification(
                incident_id=str(incident_id),
                title=title,
                severity=severity,
                description=description,
                timestamp=detection_time,
            )

            await slack_client.send_template(template)
            logger.info(
                f"Successfully sent incident ID {incident_id} Slack notification"
            )

        except Exception as e:
            logger.error(
                f"Error sending incident ID {incident_id} Slack notification: {str(e)}"
            )
            logger.exception("Detailed error information:")

    async def create_jira_ticket(self, incident_data: Dict[str, Any]) -> None:
        """
        Create a JIRA ticket for an incident.

        :param incident_data: Incident data from notification
        """
        try:
            # Create JIRA client
            jira_client = JiraClient()

            # Skip if JIRA is not configured
            if not jira_client.is_configured:
                logger.warning(
                    "JIRA not correctly configured, unable to create incident ticket"
                )
                return

            # Get incident details
            incident_id = incident_data.get("incident_id", "unknown")
            title = incident_data.get("title", "Unnamed Event")
            description = incident_data.get("description", "No description provided")
            severity = incident_data.get("severity", "medium")

            # Get source information if available
            original_event = incident_data.get("original_event", {})
            source = original_event.get("source", "unknown")
            event_type = original_event.get("event_type", "unknown")

            # Create JIRA ticket
            logger.info(f"Creating incident ID {incident_id} JIRA ticket")
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
                    f"Successfully created JIRA ticket: {result['key']} corresponding to incident ID {incident_id}"
                )
            else:
                logger.error(
                    f"Unable to create JIRA ticket for incident ID {incident_id}: {result.get('error', 'Unknown error')}"
                )

        except Exception as e:
            logger.error(
                f"Error creating incident ID {incident_id} JIRA ticket: {str(e)}"
            )
            logger.exception("Detailed error information:")
