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
        consumers = [
            self.start_rabbitmq_consumer(),
            # 不再使用Kafka作為主要事件處理通道
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

        # Create connection
        connection = await connect_robust(
            host=settings.rabbit_host,
            port=settings.rabbit_port,
            login=settings.rabbit_user,
            password=settings.rabbit_pass,
            virtualhost=settings.rabbit_vhost,
        )

        # Create channel
        channel = await connection.channel()

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
                logger.info("Created local RabbitMQ channel pool successfully")
            except Exception as e:
                logger.error(f"Failed to create RabbitMQ channel pool: {str(e)}")
                logger.warning("Incident notifications to RabbitMQ will be disabled")

        # ===== Main RabbitMQ consumer setup =====

        # 1. Setup regular events exchange and queue
        webhook_exchange = await channel.declare_exchange(
            name=self.rmq_exchange,
            auto_delete=True,
        )

        webhook_queue = await channel.declare_queue(
            name=self.rmq_queue,
            durable=True,
            auto_delete=False,
        )

        specific_routing_keys = [
            "github.push",
            "github.issue",
            "github.pull_request",
            "jira.issue_deleted",
            "jira.issue_updated",
            "jira.issue_created",
            "jira.project",
            "datadog.alert",
        ]

        # 綁定特定的路由鍵
        for routing_key in specific_routing_keys:
            await webhook_queue.bind(webhook_exchange, routing_key)
        logger.info(
            f"Binding queue {self.rmq_queue} to routing keys: {specific_routing_keys}"
        )

        # 2. Setup incident notification exchange and queue
        incidents_exchange = await channel.declare_exchange(
            name=MessagePublisher.INCIDENT_EXCHANGE,
            auto_delete=False,
        )

        incidents_queue = await channel.declare_queue(
            name="incident_notifications_queue",
            durable=True,
            auto_delete=False,
        )

        # Bind incident notification routing key
        await incidents_queue.bind(
            incidents_exchange, MessagePublisher.INCIDENT_ROUTING_KEY
        )

        # Consume regular events queue
        await webhook_queue.consume(self.on_rabbitmq_message)

        # Consume incident notification queue
        await incidents_queue.consume(self.on_incident_notification)

        logger.info("RabbitMQ consumer started")

        # Keep connection alive until should_exit flag is set
        while not self.should_exit:
            await asyncio.sleep(1)

        # Close connection when should_exit is set
        await connection.close()
        logger.info("RabbitMQ consumer stopped")

    async def on_rabbitmq_message(self, message: IncomingMessage) -> None:
        """
        Handle incoming RabbitMQ messages.

        :param message: The incoming message
        """
        async with message.process():
            try:
                # Parse and process the message
                message_body = message.body.decode("utf-8")
                logger.info(f"Received message from RabbitMQ: {message.routing_key}")
                await self.process_message(message_body)
            except Exception as e:
                logger.error(f"Error processing RabbitMQ message: {str(e)}")

    async def on_incident_notification(self, message: IncomingMessage) -> None:
        """
        Handle incoming incident notification messages from RabbitMQ.

        :param message: The incoming message
        """
        async with message.process():
            try:
                # Parse and process the incident notification
                message_body = message.body.decode("utf-8")
                logger.info(
                    f"Received incident notification from RabbitMQ: {message.routing_key}"
                )
                await self.process_incident_notification(message_body)
            except Exception as e:
                logger.error(f"Error processing incident notification: {str(e)}")

    async def process_incident_notification(self, message_body: str) -> None:
        """
        Process an incident notification message.

        :param message_body: The message body as a string
        """
        try:
            incident_data = json.loads(message_body)
            logger.info(
                f"Processing incident notification: Incident #{incident_data.get('incident_id', 'unknown')}"
            )

            # Handle incident notification - trigger any follow-up processes needed
            logger.info(
                f"Incident detected: {incident_data.get('title', 'Untitled')} - "
                f"Severity: {incident_data.get('severity', 'unknown')}"
            )

            # Send Slack notification
            await self._send_incident_slack_notification(incident_data)

            # Create JIRA ticket
            await self._create_incident_jira_ticket(incident_data)

        except json.JSONDecodeError:
            logger.error(
                f"Failed to parse incident notification as JSON: {message_body}"
            )
        except Exception as e:
            logger.error(f"Error processing incident notification: {str(e)}")

    async def _send_incident_slack_notification(
        self, incident_data: Dict[str, Any]
    ) -> None:
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
            incident_id = incident_data.get("incident_id", "unknown")
            title = incident_data.get("title", "Untitled Incident")
            description = incident_data.get("description", "No description available")
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
            logger.info(f"Sending Slack notification for incident #{incident_id}")
            template = SlackMessageTemplate.create_incident_notification(
                incident_id=str(incident_id),
                title=title,
                severity=severity,
                description=description,
                timestamp=detection_time,
            )

            await slack_client.send_template(template)
            logger.info(f"Slack notification sent for incident #{incident_id}")

        except Exception as e:
            logger.error(f"Error sending Slack notification for incident: {str(e)}")

    async def _create_incident_jira_ticket(self, incident_data: Dict[str, Any]) -> None:
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
                    "JIRA is not properly configured, incident ticket will not be created"
                )
                return

            # Get incident details
            incident_id = incident_data.get("incident_id", "unknown")
            title = incident_data.get("title", "Untitled Incident")
            description = incident_data.get("description", "No description available")
            severity = incident_data.get("severity", "medium")

            # Get source information if available
            original_event = incident_data.get("original_event", {})
            source = original_event.get("source", "unknown")
            event_type = original_event.get("event_type", "unknown")

            # Create JIRA ticket
            logger.info(f"Creating JIRA ticket for incident #{incident_id}")
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
                    f"JIRA ticket created successfully: {result['key']} for incident #{incident_id}"
                )
            else:
                logger.error(
                    f"Failed to create JIRA ticket for incident #{incident_id}: {result.get('error', 'unknown error')}"
                )

        except Exception as e:
            logger.error(f"Error creating JIRA ticket for incident: {str(e)}")

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
                f"{message_prefix} Processing message, length: {len(message_body)} characters"
            )

            try:
                # Parse JSON message
                message_dict = json.loads(message_body)

                # Extract required fields
                source = message_dict.get("source", "unknown")
                event_type = message_dict.get("event_type", "unknown")
                event_id = message_dict.get("event_id", "unknown")
                raw_payload = message_dict.get("raw_payload", {})

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
                        f"{message_prefix} Malformed message, missing source or event_type"
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
                        f"{message_prefix} Storing event {source}/{event_type} to database"
                    )

                    # Special handling for Datadog events
                    if source == "datadog":
                        logger.info(
                            f"{message_prefix} Raw payload sample for Datadog event: {str(raw_payload)[:200]}..."
                        )

                    event = await self.store_event(
                        session, source, event_type, raw_payload
                    )

                    if not event:
                        logger.error(
                            f"{message_prefix} Failed to store event {source}/{event_type}"
                        )
                        return

                    logger.info(
                        f"{message_prefix} Stored event {source}/{event_type}, ID: {event.id}"
                    )

                    # Check if we need to create an incident
                    if not event.incident_id:  # Not already linked
                        logger.debug(
                            f"{message_prefix} Checking if incident needs creation for event ID {event.id}"
                        )

                        # Use any available incident detector
                        detector = self.incident_detector

                        logger.debug(
                            f"{message_prefix} Using incident_detector to check event ID {event.id}"
                        )

                        # Check if this event meets basic conditions for an incident
                        if await detector.check_event_conditions(
                            event.content, event.source, event.event_type
                        ):
                            logger.info(
                                f"{message_prefix} Event ID {event.id} meets incident creation conditions"
                            )

                            # Use appropriate methods to determine whether to create an incident
                            # Check if thresholds are exceeded
                            threshold_breach = await detector.check_event_thresholds(
                                session, event.source, event.event_type
                            )

                            # Check for keywords
                            has_keywords = await detector.analyze_content_for_keywords(
                                event.content, event.source, event.event_type
                            )

                            # If any detection method is triggered, create an incident
                            should_create = threshold_breach or has_keywords

                            if should_create:
                                logger.info(
                                    f"{message_prefix} Creating incident for event {event.source}/{event.event_type}"
                                )

                                # Create incident
                                incident = await detector.create_incident_from_event(
                                    session,
                                    event.source,
                                    event.event_type,
                                    event.content,
                                    event.id,
                                )

                                if not incident:
                                    logger.error(
                                        f"{message_prefix} Failed to create incident for event ID {event.id}"
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
                                            # Simplified logging, remove unnecessary details

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

                                            # Publish incident detection notification
                                            logger.info(
                                                f"{message_prefix} Publishing notification for incident ID {incident.id}"
                                            )
                                            await MessagePublisher.publish_incident_detection(
                                                kafka_producer=self.kafka_producer,
                                                rmq_channel_pool=self.rmq_channel_pool,
                                                event_message=event_message,
                                                incident_id=incident.id,
                                                incident_info=incident_info,
                                            )

                                            logger.info(
                                                f"{message_prefix} Completed incident #{incident.id} creation and notification"
                                            )
                                        except Exception as e:
                                            logger.error(
                                                f"{message_prefix} Error publishing incident notification: {str(e)}"
                                            )
            except Exception as session_error:
                logger.error(
                    f"{message_prefix} Error during database session: {str(session_error)}"
                )

        except Exception as e:
            logger.error(f"Uncaught error in message processing: {str(e)}")
            # No longer output detailed exception information to reduce log volume

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
            # 添加事件去重邏輯：首先檢查是否存在具有相同事件ID的記錄
            # 這對於datadog事件特別重要，因為它們可能被處理兩次

            # 從原始負載中提取事件ID
            event_id = None
            if source == "datadog" and "alert_id" in raw_payload:
                event_id = raw_payload["alert_id"]
            elif "id" in raw_payload:
                event_id = raw_payload["id"]

            # 如果有事件ID，檢查是否已存在
            if event_id:
                # 構建查詢，尋找相同來源、類型和事件ID的事件
                # 以防萬一重複路由時間很短，將時間範圍設置為最近5分鐘
                time_threshold = datetime.utcnow() - timedelta(minutes=5)

                # 檢查事件內容中是否包含相同的事件ID
                stmt = select(Event).where(
                    (Event.source == source)
                    & (Event.event_type == event_type)
                    & (Event.created_at > time_threshold)
                )
                result = await session.execute(stmt)
                existing_events = result.scalars().all()

                # 檢查所有現有事件的內容是否包含相同的事件ID
                for existing_event in existing_events:
                    try:
                        if (
                            isinstance(existing_event.content, dict)
                            and existing_event.content.get("alert_id") == event_id
                        ):
                            logger.info(
                                f"[DB] Skipping duplicate event: {source}/{event_type}, ID: {event_id}"
                            )
                            return existing_event
                    except:
                        # 如果解析內容失敗，繼續檢查其他事件
                        pass

            # 如果沒有找到重複事件，繼續存儲新事件
            logger.info(f"[DB] Storing event to database: {source}/{event_type}")

            # If no explicit incident_id, try to find relevant incident
            if incident_id is None:
                incident_id = await self.find_relevant_incident(
                    session, source, event_type, raw_payload
                )
                if incident_id:
                    logger.info(f"[DB] Found related incident: #{incident_id}")

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

            # Add database operation logging
            logger.debug(
                f"[DB] Executing insert operation: event[{source}/{event_type}]"
            )

            # Commit transaction
            try:
                await session.commit()
                logger.debug(
                    f"[DB] Transaction committed successfully: event[{source}/{event_type}]"
                )
            except Exception as commit_error:
                logger.error(f"[DB] Failed to commit transaction: {str(commit_error)}")
                # Try to rollback transaction
                try:
                    await session.rollback()
                    logger.info(f"[DB] Transaction has been rolled back")
                except Exception as rollback_error:
                    logger.error(
                        f"[DB] Failed to rollback transaction: {str(rollback_error)}"
                    )
                raise commit_error

            # Reload event object to get latest data (like auto-generated ID)
            try:
                await session.refresh(event)
                logger.debug(f"[DB] Event object reloaded successfully: ID={event.id}")
            except Exception as refresh_error:
                logger.error(f"[DB] Cannot reload Event object: {str(refresh_error)}")
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
                        logger.debug(
                            f"[DB] Found recently created Event by query, ID: {event.id}"
                        )
                    else:
                        logger.warning(
                            f"[DB] Unable to find recently created Event by query"
                        )
                except Exception as query_error:
                    logger.error(
                        f"[DB] Failed to retrieve Event by query: {str(query_error)}"
                    )
                    raise refresh_error

            # Verify event was created and has ID
            if event and event.id:
                logger.info(
                    f"[DB] Stored {source} event, type: {event_type}, ID: {event.id}"
                )
                return event
            else:
                logger.error(f"[DB] Event appears to be stored but has no ID assigned")
                return None

        except Exception as e:
            logger.error(
                f"[DB] Unexpected error storing event {source}/{event_type}: {str(e)}"
            )
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
