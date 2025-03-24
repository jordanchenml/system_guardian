"""Event consumer for processing events from message queues."""
import asyncio
import json
from datetime import datetime
from typing import Dict, List, Optional, Any, Union

from loguru import logger
from aiokafka import AIOKafkaConsumer
from aio_pika import connect_robust, IncomingMessage
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from system_guardian.db.models.incidents import Event, Incident
from system_guardian.web.api.ingest.schema import StandardEventMessage
from system_guardian.settings import settings
from system_guardian.services.ai.incident_detector import IncidentDetector


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
    ):
        """
        Initialize the event consumer.

        :param db_session_factory: Factory for creating database sessions
        :param kafka_topics: List of Kafka topics to consume from
        :param rmq_exchange: RabbitMQ exchange name
        :param rmq_queue: RabbitMQ queue name
        :param rmq_routing_keys: List of RabbitMQ routing keys to bind
        :param auto_incident_creation: Whether to automatically create incidents from events
        """
        self.db_session_factory = db_session_factory
        self.kafka_topics = kafka_topics or ["github_events", "jira_events", "webhook_events"]
        self.rmq_exchange = rmq_exchange
        self.rmq_queue = rmq_queue
        self.rmq_routing_keys = rmq_routing_keys or ["github.*", "jira.*"]
        self.should_exit = False
        self.auto_incident_creation = auto_incident_creation
        
        # Initialize incident detector
        self.incident_detector = IncidentDetector(db_session_factory)

    async def start(self) -> None:
        """Start consuming messages from Kafka and RabbitMQ."""
        # Start consumers in separate tasks
        consumers = [
            self.start_kafka_consumer(),
            self.start_rabbitmq_consumer(),
        ]
        
        # Run all consumers concurrently
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
        
        await consumer.start()
        
        try:
            # Consume messages in an infinite loop
            while not self.should_exit:
                try:
                    # Fetch messages with a timeout
                    batch = await consumer.getmany(timeout_ms=1000)
                    
                    for tp, messages in batch.items():
                        logger.info(f"Received {len(messages)} messages from Kafka topic: {tp.topic}")
                        
                        for message in messages:
                            # Process each message
                            await self.process_message(message.value.decode("utf-8"))
                            
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
        
        # Declare exchange
        exchange = await channel.declare_exchange(
            name=self.rmq_exchange,
            auto_delete=True,
        )
        
        # Declare queue
        queue = await channel.declare_queue(
            name=self.rmq_queue,
            durable=True,
            auto_delete=False,
        )
        
        # Bind queue to exchange with routing keys
        for routing_key in self.rmq_routing_keys:
            await queue.bind(exchange, routing_key)
            
        # Set up message consumption
        await queue.consume(self.on_rabbitmq_message)
        
        try:
            # Keep the consumer running
            while not self.should_exit:
                await asyncio.sleep(1)
        finally:
            # Clean up
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

    async def process_message(self, message_body: str) -> None:
        """
        Process a message from either Kafka or RabbitMQ.
        
        :param message_body: The message body as a string
        """
        try:
            # Parse the message
            message_data = json.loads(message_body)
            
            # Validate that it's a standardized event message
            if all(key in message_data for key in ["source", "event_type", "timestamp", "raw_payload"]):
                # Create a session
                async with self.db_session_factory() as session:
                    # Store the event
                    event = await self.store_event(
                        session=session,
                        source=message_data["source"],
                        event_type=message_data["event_type"],
                        raw_payload=message_data["raw_payload"],
                    )
                    
                    # If auto incident creation is enabled, check if an incident should be created
                    if self.auto_incident_creation and event and event.incident_id is None:
                        logger.warning(f"Checking for auto incident creation for {event.source} {event.event_type}")
                        await self.check_for_auto_incident_creation(
                            session=session,
                            event_id=event.id,
                            source=event.source,
                            event_type=event.event_type, 
                            payload=event.content
                        )
            else:
                logger.warning(f"Received message in unexpected format: {message_data}")
                
        except json.JSONDecodeError:
            logger.error(f"Failed to parse message as JSON: {message_body}")
        except Exception as e:
            logger.error(f"Error processing message: {str(e)}")

    async def store_event(
        self,
        session: AsyncSession,
        source: str,
        event_type: str,
        raw_payload: Dict[str, Any],
        incident_id: Optional[int] = None,
    ) -> Event:
        """
        Store an event in the database.
        
        :param session: Database session
        :param source: Event source (github, jira, etc.)
        :param event_type: Event type
        :param raw_payload: Event payload
        :param incident_id: Optional ID of related incident
        :returns: The created event
        """
        # If there's no explicit incident_id, try to find a relevant incident
        logger.info(f"Inserting event {source} {event_type} into database")
        if incident_id is None:
            incident_id = await self.find_relevant_incident(session, source, event_type, raw_payload)
        
        # Create the event
        event = Event(
            incident_id=incident_id,
            source=source,
            event_type=event_type,
            content=raw_payload,
            created_at=datetime.utcnow(),
        )
        
        # Add and commit
        session.add(event)
        await session.commit()
        await session.refresh(event)
        
        logger.info(f"Stored {source} event of type {event_type} with ID {event.id}")
        return event
    
    async def find_relevant_incident(
        self,
        session: AsyncSession,
        source: str,
        event_type: str,
        raw_payload: Dict[str, Any],
    ) -> Optional[int]:
        """
        Find a relevant incident for the event.
        
        This is a simple implementation, but could be enhanced with more sophisticated
        incident correlation logic based on the event source and type.
        
        :param session: Database session
        :param source: Event source
        :param event_type: Event type
        :param raw_payload: Event payload
        :returns: Incident ID or None
        """
        # Simple implementation: find the most recent open incident for this source
        query = (
            select(Incident)
            .where(Incident.source == source)
            .where(Incident.status.in_(["open", "investigating"]))
            .order_by(Incident.created_at.desc())
        )
        # TODO: Add more sophisticated logic here
        
        result = await session.execute(query)
        incident = result.scalars().first()
        
        # If there's a relevant incident, return its ID
        # if incident:
        #     return incident.id
            
        # Otherwise, return None (event will be stored without an incident relation)
        return None 

    async def check_for_auto_incident_creation(
        self,
        session: AsyncSession,
        event_id: int,
        source: str,
        event_type: str,
        payload: Dict[str, Any]
    ) -> Optional[Incident]:
        """
        Check if an incident should be automatically created from this event.
        
        :param session: Database session
        :param event_id: Event ID
        :param source: Event source
        :param event_type: Event type
        :param payload: Event payload
        :returns: Created incident or None
        """
        logger.debug(f"Checking if incident should be created for {source}/{event_type} event")
        
        try:
            # Check if the event meets the conditions for creating an incident
            meets_conditions = await self.incident_detector.check_event_conditions(
                payload, source, event_type
            )

            logger.warning(f"Meets conditions: {meets_conditions}")
            
            if not meets_conditions:
                logger.debug(f"Event does not meet basic conditions for incident creation")
                return None
                
            # Check for threshold breach
            threshold_breach = await self.incident_detector.check_event_thresholds(
                session, source, event_type
            )
            
            # Check for keywords
            has_keywords = await self.incident_detector.analyze_content_for_keywords(
                payload, source, event_type
            )
            
            # Create incident if any detection method is triggered
            if threshold_breach or has_keywords:
                logger.info(f"Auto-creating incident from {source}/{event_type} event " +
                           f"(threshold_breach={threshold_breach}, has_keywords={has_keywords})")
                
                return await self.incident_detector.create_incident_from_event(
                    session, source, event_type, payload, event_id
                )
            else:
                logger.debug(f"Event did not trigger incident creation criteria")
                return None
                
        except Exception as e:
            logger.error(f"Error in auto incident detection: {str(e)}")
            return None 