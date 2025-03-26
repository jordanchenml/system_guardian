"""Services for processing and forwarding webhook events."""
import asyncio
import re
from typing import Optional, Dict, List, Any
import json

from loguru import logger
from aio_pika import Channel, Message
from aio_pika.pool import Pool
from aiokafka import AIOKafkaProducer

class MessagePublisher:
    """Service for publishing messages to different message queues."""

    # Default topics mapping
    DEFAULT_TOPICS: Dict[str, str] = {
        "github": "github_events",
        "jira": "jira_events",
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
        sanitized = re.sub(r'[^a-zA-Z0-9\._-]', '_', topic)
        
        # Ensure the topic name doesn't start with a dot or underscore (Kafka recommendation)
        if sanitized and sanitized[0] in ['.', '_']:
            sanitized = 'topic' + sanitized
            
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
        if any(critical_type in event_type_lower for critical_type in MessagePublisher.CRITICAL_EVENT_TYPES):
            return True
            
        # Check if the payload contains urgent/critical flags
        payload = event_message.raw_payload
        if isinstance(payload, dict):
            # Check for priority or severity indicators in common fields
            priority = payload.get("priority", "").lower()
            severity = payload.get("severity", "").lower()
            
            if any(word in priority for word in ["high", "urgent", "critical"]):
                return True
                
            if any(word in severity for word in ["high", "urgent", "critical", "fatal"]):
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
                
            logger.debug(f"Using topic '{topic}' for event from source '{event_message.source}'")
        else:
            # If a topic was provided, still ensure it's valid
            sanitized_topic = MessagePublisher.sanitize_topic_name(topic)
            if topic != sanitized_topic:
                logger.warning(f"Provided topic name '{topic}' was sanitized to '{sanitized_topic}'")
                topic = sanitized_topic
        
        try:
            # 簡化日誌輸出，只在DEBUG級別顯示詳情
            logger.debug(f"Sending message to Kafka topic: {topic}")
            await producer.send(
                topic=topic,
                value=event_message.to_json().encode("utf-8"),
            )
            # 不再輸出這條成功消息，減少日誌量
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
            logger.debug(f"Sending message to RabbitMQ: {exchange_name}/{routing_key}")
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
            # 不再輸出這條成功消息，減少日誌量
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
        incident_payload = {
            "original_event": {
                "source": event_message.source,
                "event_type": event_message.event_type,
                "timestamp": event_message.timestamp.isoformat(),
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
                # 簡化日誌輸出
                logger.info(f"Publishing incident #{incident_id} notification to RabbitMQ")
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
                logger.error(f"Failed to publish incident notification to RabbitMQ: {str(e)}")
        
        # Also store in Kafka for historical record and analytics
        if kafka_producer:
            try:
                # 簡化日誌輸出
                logger.info(f"Storing incident #{incident_id} record in Kafka")
                await kafka_producer.send(
                    topic=MessagePublisher.INCIDENT_TOPIC,
                    value=json.dumps(incident_payload).encode("utf-8"),
                )
            except Exception as e:
                logger.error(f"Failed to store incident record in Kafka: {str(e)}")

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
        
        # Determine if this is a critical event needing real-time processing
        is_critical = MessagePublisher.is_critical_event(event_message)

        # Always store all events in Kafka for historical record
        if kafka_producer:
            tasks.append(
                MessagePublisher.send_to_kafka(kafka_producer, event_message)
            )
        
        # 原始邏輯為向RabbitMQ發送所有消息
        # 優化後的邏輯是只向RabbitMQ發送關鍵事件
        # 為了確保後續處理正常，我們需要調整為:
        # 1. 所有事件仍發送到RabbitMQ以確保現有功能(如插入DB和incident檢測)正常運作
        # 2. 但使用不同的路由模式，以便根據優先級區分處理方式
        
        if rmq_channel_pool:
            # 所有事件都發送到RabbitMQ，但是...
            if is_critical:
                # 關鍵事件使用專門的路由鍵，指示需要立即處理
                tasks.append(
                    MessagePublisher.send_to_rabbitmq(
                        rmq_channel_pool, 
                        event_message,
                        routing_key=f"priority.{event_message.source}.{event_message.event_type}"
                    )
                )
            else:
                # 非關鍵事件使用標準路由鍵
                tasks.append(
                    MessagePublisher.send_to_rabbitmq(rmq_channel_pool, event_message)
                )
        
        if tasks:
            # Run all publishing tasks concurrently
            await asyncio.gather(*tasks, return_exceptions=True)
        else:
            logger.warning("No message queue configured for event publishing") 