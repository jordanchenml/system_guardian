"""Event consumer for processing events from message queues."""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple
from uuid import UUID
import re
import hashlib

from loguru import logger
from aio_pika import connect_robust, IncomingMessage, ExchangeType, Message
from aio_pika.channel import Channel
from aio_pika.exceptions import QueueEmpty
from aio_pika.abc import AbstractQueue
from aio_pika.pool import Pool
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy import select, or_, and_, func

from system_guardian.db.models.incidents import Event, Incident
from system_guardian.settings import settings
from system_guardian.services.ai.incident_detector import IncidentDetector
from system_guardian.services.config import ConfigManager
from system_guardian.services.ai.severity_classifier import SeverityClassifier
from system_guardian.services.ingest.message_publisher import MessagePublisher
from system_guardian.services.slack.client import SlackClient
from system_guardian.services.slack.templates import SlackMessageTemplate, AlertSeverity
from system_guardian.services.jira.client import JiraClient
from system_guardian.services.ai.engine import AIEngine

# Remove direct import of StandardEventMessage, use type annotation instead
# from system_guardian.web.api.ingest.schema import StandardEventMessage


class EventConsumer:
    """Consumer for processing events from RabbitMQ."""

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

        # Store RabbitMQ channel pool
        self.rmq_channel_pool = rmq_channel_pool

    async def start(self) -> None:
        """Start consuming messages from RabbitMQ."""
        # 首先進行 DB 連接測試
        await self.test_database_connection()
        # 開始消費
        await self.start_rabbitmq_consumer()

    async def stop(self) -> None:
        """Stop all consumers."""
        self.should_exit = True
        logger.info("Event consumer stopping")

    async def test_database_connection(self) -> None:
        """Test database connection directly."""
        logger.info("Performing direct database connection test...")
        try:
            # Create a test session
            async with self.db_session_factory() as session:
                # Try to execute a simple query
                from sqlalchemy import text

                result = await session.execute(text("SELECT 1 as test"))
                value = result.scalar()
                logger.info(f"Database connection test successful, result: {value}")

                # Check if events table exists
                try:
                    table_check = await session.execute(
                        text(
                            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'events')"
                        )
                    )
                    table_exists = table_check.scalar()
                    logger.info(f"Events table exists: {table_exists}")

                    if table_exists:
                        # Check count of events
                        count_query = await session.execute(
                            text("SELECT COUNT(*) FROM events")
                        )
                        count = count_query.scalar()
                        logger.info(f"Current events table count: {count}")
                except Exception as table_error:
                    logger.error(f"Error checking events table: {str(table_error)}")
                    logger.exception("Table check error details:")
        except Exception as db_error:
            logger.error(f"Database connection test failed: {str(db_error)}")
            logger.exception("Database test error details:")

    async def start_rabbitmq_consumer(self) -> None:
        """Start consuming messages from RabbitMQ."""
        logger.info(f"Starting RabbitMQ consumer for exchange: {self.rmq_exchange}")

        max_retries = 3
        retry_count = 0
        retry_delay = 5  # seconds

        while retry_count < max_retries:
            try:
                # Create connection
                logger.info(
                    f"Attempting to connect to RabbitMQ: host={settings.rabbit_host}, port={settings.rabbit_port}"
                )

                # 增加連接超時和心跳設置
                connection = await connect_robust(
                    host=settings.rabbit_host,
                    port=settings.rabbit_port,
                    login=settings.rabbit_user,
                    password=settings.rabbit_pass,
                    virtualhost=settings.rabbit_vhost,
                    timeout=10,  # 增加連接超時
                    heartbeat=60,  # 定期心跳確保連接活躍
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
                            get_local_channel,
                            max_size=settings.rabbit_channel_pool_size,
                        )
                        logger.info("Successfully created local RabbitMQ channel pool")
                    except Exception as e:
                        logger.error(
                            f"Failed to create RabbitMQ channel pool: {str(e)}"
                        )
                        logger.exception("Detailed error information:")
                        logger.warning(
                            "RabbitMQ incident notifications will be disabled"
                        )

                # ===== Main RabbitMQ consumer setup =====

                # 1. Setup regular events exchange and queue
                logger.info(f"Declaring webhook events exchange: {self.rmq_exchange}")
                webhook_exchange = await channel.declare_exchange(
                    name=self.rmq_exchange,
                    type=ExchangeType.TOPIC,
                    auto_delete=False,
                    durable=False,
                )

                logger.info(f"Declaring webhook events queue: {self.rmq_queue}")
                webhook_queue = await channel.declare_queue(
                    name=self.rmq_queue,
                    durable=True,
                    auto_delete=False,
                )

                # 2. Bind queue to exchange with routing keys
                for routing_key in self.rmq_routing_keys:
                    # Direct exchange 需要精確匹配路由鍵，我們需要為每個可能的路由鍵模式創建綁定
                    if "*" in routing_key:
                        base_key = routing_key.replace("*", "")
                        logger.info(
                            f"Creating multiple bindings for pattern: {routing_key}"
                        )

                        # 對於 github.* 模式，我們需要綁定常見的 github 事件
                        if base_key == "github.":
                            common_events = [
                                "push",
                                "pull_request",
                                "issues",
                                "commit_comment",
                                "release",
                            ]
                            for event in common_events:
                                specific_key = f"github.{event}"
                                logger.info(
                                    f"Binding queue to specific routing key: {specific_key}"
                                )
                                await webhook_queue.bind(
                                    exchange=webhook_exchange,
                                    routing_key=specific_key,
                                )

                        # 對於 jira.* 模式，綁定常見的 jira 事件
                        elif base_key == "jira.":
                            common_events = [
                                "issue_created",
                                "issue_updated",
                                "comment_added",
                                "issue_deleted",
                            ]
                            for event in common_events:
                                specific_key = f"jira.{event}"
                                logger.info(
                                    f"Binding queue to specific routing key: {specific_key}"
                                )
                                await webhook_queue.bind(
                                    exchange=webhook_exchange,
                                    routing_key=specific_key,
                                )

                        # 對於 datadog.* 模式，綁定常見的 datadog 事件
                        elif base_key == "datadog.":
                            common_events = ["alert", "metric", "event", "monitor"]
                            for event in common_events:
                                specific_key = f"datadog.{event}"
                                logger.info(
                                    f"Binding queue to specific routing key: {specific_key}"
                                )
                                await webhook_queue.bind(
                                    exchange=webhook_exchange,
                                    routing_key=specific_key,
                                )
                    else:
                        # 對於沒有通配符的路由鍵，直接綁定
                        logger.info(f"Binding queue to routing key: {routing_key}")
                        await webhook_queue.bind(
                            exchange=webhook_exchange,
                            routing_key=routing_key,
                        )

                # 確保我們總是綁定測試路由鍵
                logger.info("Binding queue to test routing key: test.event")
                await webhook_queue.bind(
                    exchange=webhook_exchange,
                    routing_key="test.event",
                )

                # 添加一個通用路由鍵，捕獲所有消息
                logger.info("Binding queue to fallback routing key: #")
                await webhook_queue.bind(
                    exchange=webhook_exchange,
                    routing_key="#",
                )

                # 3. Set up message consumer
                logger.info("Setting up RabbitMQ message consumer...")

                async def process_webhook_message(message: IncomingMessage) -> None:
                    """Process a webhook message."""
                    # Add immediate logging at the start
                    logger.info(
                        f"===> RECEIVED MESSAGE: routing_key={message.routing_key}, message_id={message.message_id}"
                    )

                    # Log EVERY incoming message
                    logger.warning(
                        f"!!INCOMING MESSAGE DETAILS: routing_key={message.routing_key}, content_type={message.content_type}, size={len(message.body) if message.body else 0}"
                    )

                    try:
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

                                # 添加更詳細的日誌
                                logger.warning(f"!!MESSAGE BODY START: {body[:500]}")

                                # Log the first part of the message for debugging
                                logger.info(
                                    f"Message content (truncated): {body[:200]}..."
                                )

                                # Determine if this is a priority message based on routing key or content
                                is_priority = any(
                                    critical_type in message.routing_key.lower()
                                    for critical_type in self.CRITICAL_EVENT_TYPES
                                )

                                # DEBUG: Add detailed message logs
                                logger.debug(f"Message body sample: {body[:200]}...")

                                # Process the message
                                logger.info(
                                    f"Starting to process message: {message.routing_key}"
                                )
                                await self.process_message(
                                    body, is_priority=is_priority
                                )
                                logger.info(
                                    f"Finished processing message: {message.routing_key}"
                                )
                            except UnicodeDecodeError as ude:
                                logger.error(f"Error decoding message body: {str(ude)}")
                                logger.debug(
                                    f"Message body (raw bytes): {message.body[:100]}"
                                )
                            except Exception as e:
                                logger.error(
                                    f"Error processing RabbitMQ message: {str(e)}"
                                )
                                logger.exception("Detailed error information:")
                    except Exception as process_error:
                        logger.error(
                            f"Error in message.process(): {str(process_error)}"
                        )
                        logger.exception("Message processing error details:")

                # Start consuming with prefetch count set to 10 for processing multiple messages
                logger.info(f"Setting QoS prefetch count to 10 for better throughput")
                await channel.set_qos(prefetch_count=10)

                logger.info(f"Starting to consume messages from queue {self.rmq_queue}")
                consumer_tag = await webhook_queue.consume(process_webhook_message)
                logger.info(f"Consumer started with tag: {consumer_tag}")

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
                async def process_incident_notification(
                    message: IncomingMessage,
                ) -> None:
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

                # 正常退出
                logger.info("Closing RabbitMQ consumer gracefully")
                await connection.close()
                logger.info("RabbitMQ connection closed")
                return

            except Exception as channel_error:
                logger.error(f"Channel error: {str(channel_error)}")
                if connection and not connection.is_closed:
                    await connection.close()
                raise  # 重新拋出異常以觸發重試

            except Exception as e:
                retry_count += 1
                logger.error(f"Error starting RabbitMQ consumer: {str(e)}")
                logger.exception("Detailed error information:")

                if retry_count < max_retries:
                    wait_time = retry_delay * retry_count
                    logger.info(f"Will attempt to reconnect in {wait_time} seconds...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(
                        f"Failed to start RabbitMQ consumer after {max_retries} retries"
                    )
                    break

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

            # Validate incident data structure
            if not isinstance(incident_data, dict):
                logger.error(
                    f"Incident data is not a dictionary: {type(incident_data)}"
                )
                return

            # Ensure we have all required fields for notifications
            required_fields = ["incident_id", "title", "severity"]
            missing_fields = [
                field for field in required_fields if field not in incident_data
            ]
            if missing_fields:
                logger.warning(
                    f"Incident notification missing required fields: {missing_fields}"
                )
                # Set defaults for missing fields
                for field in missing_fields:
                    if field == "title":
                        incident_data["title"] = f"Incident #{incident_id}"
                    elif field == "severity":
                        incident_data["severity"] = "medium"

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
        Process a message from the queue.

        :param message_body: The message body as a string
        :param is_priority: Whether this is a priority message
        """
        # Generate a trace ID for tracking this message
        trace_id = hashlib.md5(message_body.encode()).hexdigest()[:8]
        logger.info(
            f"[PROCESS][{trace_id}] Processing {'priority ' if is_priority else ''}message"
        )

        try:
            # Parse the message body as JSON
            data = json.loads(message_body)
            logger.debug(f"[PROCESS][{trace_id}] Message parsed successfully")

            # Extract metadata
            source = data.get("source", "unknown")
            event_type = data.get("event_type", "unknown")
            raw_payload = data.get("raw_payload", {})

            # 從消息中獲取auto_detect_incident參數，如果不存在則根據來源設置默認值
            # 根據預設：JIRA設為False，其他都是True
            auto_detect_incident = data.get("auto_detect_incident")

            # 如果消息中沒有設置，則根據來源使用默認值
            if auto_detect_incident is None:
                if source.lower() == "jira":
                    auto_detect_incident = False
                else:
                    auto_detect_incident = True

            # 記錄是否將為此事件自動創建incident
            logger.info(
                f"[PROCESS][{trace_id}] Event auto_detect_incident set to {auto_detect_incident} for {source}/{event_type}"
            )

            # Process based on the source and event type
            logger.info(f"[PROCESS][{trace_id}] Processing {source}/{event_type} event")

            if not auto_detect_incident:
                logger.info(
                    f"[PROCESS][{trace_id}] Auto incident detection disabled for source: {source}"
                )

            # 正常處理流程
            try:
                async with self.db_session_factory() as session:
                    # 儲存事件到資料庫
                    event = await self.store_event(
                        session, source, event_type, raw_payload
                    )

                    if not event:
                        logger.error(f"[PROCESS][{trace_id}] Failed to store event")
                        return

                    logger.info(
                        f"[PROCESS][{trace_id}] Successfully stored event ID: {event.id}"
                    )

                    # 只有在auto_detect_incident=True且auto_incident_creation=True的情況下進行事件檢測
                    if (
                        auto_detect_incident
                        and not event.related_incident_id
                        and self.auto_incident_creation
                    ):
                        # 檢查是否需要創建事件
                        logger.info(
                            f"[PROCESS][{trace_id}] Checking if incident should be created"
                        )
                        detector = self.incident_detector

                        # 執行事件檢測邏輯
                        conditions_met = await detector.check_event_conditions(
                            event.content, event.source, event.event_type
                        )

                        if conditions_met:
                            logger.info(
                                f"[PROCESS][{trace_id}] Event meets conditions for incident creation"
                            )

                            # 檢查閾值和關鍵字
                            threshold_breach = await detector.check_event_thresholds(
                                session, event.source, event.event_type
                            )

                            has_keywords = await detector.analyze_content_for_keywords(
                                event.content, event.source, event.event_type
                            )

                            if threshold_breach or has_keywords:
                                logger.info(
                                    f"[PROCESS][{trace_id}] Creating incident for event ID: {event.id}"
                                )

                                # 創建事件
                                incident = await detector.create_incident_from_event(
                                    session,
                                    event.source,
                                    event.event_type,
                                    event.content,
                                    event.id,
                                )

                                if incident:
                                    logger.info(
                                        f"[PROCESS][{trace_id}] Created incident ID: {incident.id}"
                                    )

                                    # 發布事件通知
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

                                    await MessagePublisher.publish_incident_detection(
                                        rmq_channel_pool=self.rmq_channel_pool,
                                        event_message=event_message,
                                        incident_id=incident.id,
                                        incident_info=incident_info,
                                    )
                    else:
                        if not auto_detect_incident:
                            logger.info(
                                f"[PROCESS][{trace_id}] Auto incident detection disabled for {source}"
                            )
                        elif event.related_incident_id:
                            logger.info(
                                f"[PROCESS][{trace_id}] Event already associated with incident ID: {event.related_incident_id}"
                            )
                        elif not self.auto_incident_creation:
                            logger.info(
                                f"[PROCESS][{trace_id}] Auto incident creation is disabled"
                            )

            except Exception as db_error:
                logger.error(f"[PROCESS][{trace_id}] Database error: {str(db_error)}")
                logger.exception(f"[PROCESS][{trace_id}] Detailed error:")

        except Exception as e:
            logger.error(f"[PROCESS][{trace_id}] Error processing message: {str(e)}")
            logger.exception(f"[PROCESS][{trace_id}] Detailed error:")

        logger.info(f"[PROCESS][{trace_id}] Message processing completed")

    async def store_event(
        self,
        session: AsyncSession,
        source: str,
        event_type: str,
        raw_payload: Dict[str, Any],
        related_incident_id: Optional[int] = None,
    ) -> Optional[Event]:
        """
        Store an event to the database.

        :param session: Database session
        :param source: Event source
        :param event_type: Event type
        :param raw_payload: Event payload
        :param related_incident_id: Optional incident ID to associate with
        :returns: The created event
        """
        # Add a unique identifier for tracking this specific event processing
        event_trace_id = (
            f"{source}_{event_type}_{datetime.utcnow().strftime('%H%M%S%f')}"
        )

        try:
            logger.info(
                f"[DB][{event_trace_id}] Attempting to store event: {source}/{event_type}"
            )

            # Try direct SQL execution test
            try:
                from sqlalchemy import text

                # First check if we can do a basic SELECT query
                logger.info(f"[DB][{event_trace_id}] Testing database connection")
                check_query = await session.execute(text("SELECT 1 as test"))
                test_result = check_query.scalar()
                logger.info(
                    f"[DB][{event_trace_id}] Basic SELECT test result: {test_result}"
                )

                # Check if events table exists
                logger.info(f"[DB][{event_trace_id}] Checking if events table exists")
                try:
                    table_check = await session.execute(
                        text(
                            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'events')"
                        )
                    )
                    table_exists = table_check.scalar()
                    logger.info(
                        f"[DB][{event_trace_id}] Events table exists: {table_exists}"
                    )

                    if not table_exists:
                        logger.error(
                            f"[DB][{event_trace_id}] CRITICAL ERROR: Events table does not exist!"
                        )
                        return None

                except Exception as table_check_err:
                    logger.error(
                        f"[DB][{event_trace_id}] Error checking table existence: {str(table_check_err)}"
                    )

                # Try direct SQL insert
                logger.info(f"[DB][{event_trace_id}] Attempting direct SQL insert")

                # Create simple content
                event_content = json.dumps(raw_payload)

                # Create timestamp
                current_time = datetime.utcnow()

                # Prepare SQL with related_incident_id if provided
                sql_insert_query = """
                    INSERT INTO events (source, event_type, content, created_at{related_incident_col}) 
                    VALUES (:source, :event_type, :content, :created_at{related_incident_val})
                    RETURNING id
                """

                # SQL parameters
                sql_params = {
                    "source": source,
                    "event_type": event_type,
                    "content": event_content,
                    "created_at": current_time,
                }
                logger.warning(f"[DB][{event_trace_id}] SQL parameters: {sql_params}")

                # Add related_incident_id if provided
                related_incident_col = (
                    ", related_incident_id" if related_incident_id is not None else ""
                )
                related_incident_val = (
                    ", :related_incident_id" if related_incident_id is not None else ""
                )

                if related_incident_id is not None:
                    sql_params["related_incident_id"] = related_incident_id

                # Format the SQL query with the correct columns
                formatted_sql = sql_insert_query.format(
                    related_incident_col=related_incident_col,
                    related_incident_val=related_incident_val,
                )

                # Try raw SQL insert
                try:
                    raw_insert = await session.execute(
                        text(formatted_sql),
                        sql_params,
                    )

                    new_id = raw_insert.scalar()
                    logger.info(
                        f"[DB][{event_trace_id}] Direct SQL insert successful, ID: {new_id}"
                    )

                    # Commit the transaction
                    await session.commit()
                    logger.info(
                        f"[DB][{event_trace_id}] Transaction committed successfully"
                    )

                    if new_id:
                        # Create an event object with the actual payload data
                        event = Event(
                            id=new_id,
                            source=source,
                            event_type=event_type,
                            content=raw_payload,
                            created_at=current_time,
                            related_incident_id=related_incident_id,
                        )
                        return event

                except Exception as insert_err:
                    logger.error(
                        f"[DB][{event_trace_id}] Direct SQL insert failed: {str(insert_err)}"
                    )
                    logger.exception(f"[DB][{event_trace_id}] Insert error details:")

                    # Try to get specific PostgreSQL error info
                    if hasattr(insert_err, "__cause__") and insert_err.__cause__:
                        logger.error(
                            f"[DB][{event_trace_id}] PostgreSQL error: {str(insert_err.__cause__)}"
                        )

            except Exception as sql_err:
                logger.error(
                    f"[DB][{event_trace_id}] SQL execution test failed: {str(sql_err)}"
                )
                logger.exception(f"[DB][{event_trace_id}] SQL test error details:")

            # If direct SQL failed, try one more time with ORM
            try:
                logger.info(f"[DB][{event_trace_id}] Attempting ORM insert")

                # Create an event object with the actual payload data
                event = Event(
                    source=source,
                    event_type=event_type,
                    content=raw_payload,
                    created_at=datetime.utcnow(),
                    related_incident_id=related_incident_id,
                )

                # Add to session and commit
                session.add(event)
                await session.commit()
                await session.refresh(event)

                logger.info(
                    f"[DB][{event_trace_id}] ORM insert successful, ID: {event.id}"
                )
                return event

            except Exception as orm_err:
                logger.error(
                    f"[DB][{event_trace_id}] ORM insert failed: {str(orm_err)}"
                )
                logger.exception(f"[DB][{event_trace_id}] ORM error details:")
                return None

        except Exception as e:
            logger.error(f"[DB][{event_trace_id}] Fatal error storing event: {str(e)}")
            logger.exception(f"[DB][{event_trace_id}] Detailed error:")
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
        # Generate trace id for logging
        trace_id = f"{source}_{event_type}_{datetime.utcnow().strftime('%H%M%S')}"
        logger.debug(
            f"[FIND][{trace_id}] Finding relevant incident for {source}/{event_type} event"
        )

        try:
            # 1. First attempt to find open incidents from the same source that are recent
            query = (
                select(Incident)
                .where(Incident.source == source)
                .where(Incident.status.in_(["open", "investigating"]))
                .order_by(Incident.created_at.desc())
            )

            # Log the query for debugging
            logger.debug(f"[FIND][{trace_id}] Executing query: {str(query)}")

            # Add error handling for database operations
            try:
                # Execute the query and get the first five recent open incidents (to limit comparison)
                result = await session.execute(query)
                recent_incidents = result.scalars().fetchmany(5)

                # Log the results
                logger.debug(
                    f"[FIND][{trace_id}] Found {len(recent_incidents)} recent incidents"
                )
            except Exception as db_error:
                logger.error(
                    f"[FIND][{trace_id}] Database error finding incidents: {str(db_error)}"
                )
                logger.exception(f"[FIND][{trace_id}] Database error details:")
                return None

            if not recent_incidents:
                logger.debug(f"[FIND][{trace_id}] No open {source} incidents found")
                return None

            # 2. Extract meaningful content from the event
            event_title = ""
            event_description = ""

            # Handle different sources differently to extract the most relevant content
            try:
                if source == "github":
                    if "issue" in event_type:
                        event_title = raw_payload.get("issue", {}).get("title", "")
                        event_description = raw_payload.get("issue", {}).get("body", "")
                    elif "pull_request" in event_type:
                        event_title = raw_payload.get("pull_request", {}).get(
                            "title", ""
                        )
                        event_description = raw_payload.get("pull_request", {}).get(
                            "body", ""
                        )
                elif source == "jira":
                    event_title = (
                        raw_payload.get("issue", {})
                        .get("fields", {})
                        .get("summary", "")
                    )
                    event_description = (
                        raw_payload.get("issue", {})
                        .get("fields", {})
                        .get("description", "")
                    )
                elif source == "datadog":
                    event_title = raw_payload.get("title", "")
                    event_description = raw_payload.get(
                        "message", ""
                    ) or raw_payload.get("text", "")
                elif source == "slack":
                    event_description = raw_payload.get("text", "") or raw_payload.get(
                        "message", {}
                    ).get("text", "")

                logger.debug(
                    f"[FIND][{trace_id}] Extracted title: '{event_title[:50]}...' and description (length: {len(event_description)})"
                )
            except Exception as extract_error:
                logger.error(
                    f"[FIND][{trace_id}] Error extracting content from payload: {str(extract_error)}"
                )
                # Continue with empty strings rather than fail

            # If we couldn't extract meaningful content, fallback to simpler methods
            if not event_title and not event_description:
                # Simple check - if this is the first event of this type in last 24 hours,
                # associate with most recent incident of the same source
                most_recent = recent_incidents[0] if recent_incidents else None
                if most_recent:
                    logger.debug(
                        f"[FIND][{trace_id}] No meaningful content extracted, associating with most recent incident #{most_recent.id}"
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
                    logger.debug(
                        f"[FIND][{trace_id}] Looking for similar incidents among IDs: {open_incident_ids}"
                    )

                    # Create filter condition
                    filter_condition = {
                        "must": [
                            {"key": "incident_id", "match": {"any": open_incident_ids}}
                        ]
                    }

                    # Find similar incidents
                    try:
                        similar_incidents = (
                            await similarity_service.find_similar_incidents(
                                query_text=event_text,
                                limit=3,
                                filter_condition=filter_condition,
                            )
                        )

                        # Log the results
                        for idx, similar in enumerate(similar_incidents):
                            logger.debug(
                                f"[FIND][{trace_id}] Similar incident #{idx+1}: ID={similar.get('incident_id')}, score={similar.get('similarity_score', 0):.2f}"
                            )

                        # Check if any incident has high similarity (threshold: 0.75)
                        for similar in similar_incidents:
                            if similar.get("similarity_score", 0) > 0.75:
                                incident_id = similar.get("incident_id")
                                if incident_id:
                                    logger.info(
                                        f"[FIND][{trace_id}] Found similar incident #{incident_id} with score {similar['similarity_score']:.2f}"
                                    )
                                    return int(incident_id)
                    except Exception as similarity_error:
                        logger.error(
                            f"[FIND][{trace_id}] Error in similarity search: {str(similarity_error)}"
                        )
                        # Continue to keyword matching as fallback

                    logger.debug(
                        f"[FIND][{trace_id}] No sufficiently similar incidents found with similarity search"
                    )
            except Exception as e:
                logger.warning(
                    f"[FIND][{trace_id}] Error using similarity service: {str(e)}"
                )
                # Continue to fallback method

            # 4. Fallback: Basic keyword matching between event and incident titles
            best_match = None
            best_score = 0

            # Log that we're starting keyword matching
            logger.debug(
                f"[FIND][{trace_id}] Starting keyword matching for {len(recent_incidents)} incidents"
            )

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
                        logger.debug(
                            f"[FIND][{trace_id}] Incident #{incident.id} match score: {score:.2f}"
                        )

                if score > best_score and score > 0.3:  # Threshold of 0.3
                    best_score = score
                    best_match = incident

            if best_match:
                logger.info(
                    f"[FIND][{trace_id}] Found related incident #{best_match.id} with keyword matching score {best_score:.2f}"
                )
                return best_match.id

            # 5. No good match found
            logger.debug(
                f"[FIND][{trace_id}] No relevant incident found for {source}/{event_type} event"
            )
            return None

        except Exception as e:
            logger.error(
                f"[FIND][{trace_id}] Error finding relevant incident: {str(e)}"
            )
            logger.exception(f"[FIND][{trace_id}] Detailed error information:")
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
                event.related_incident_id = incident.id
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
            & (Event.related_incident_id.is_(None))
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
            event.related_incident_id = incident.id
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
