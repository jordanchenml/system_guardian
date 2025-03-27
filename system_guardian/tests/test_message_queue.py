#!/usr/bin/env python3
"""
Test message queue connection and message sending functionality
"""
import sys
import json
import asyncio
import random
import uuid
import time
from datetime import datetime, timezone, timedelta

import aiokafka
import aio_pika
from loguru import logger
import asyncpg
from sqlalchemy import text, select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from aio_pika import Message

# Import configuration from settings
from system_guardian.settings import settings
from system_guardian.db.models.incidents import Event
from system_guardian.db.models import load_all_models

# Define default queue names
DEFAULT_EVENTS_QUEUE_NAME = "webhook_events_queue"
DEFAULT_EVENTS_TOPIC_NAME = "webhook_events"


# Get queue names with fallbacks to defaults
def get_events_queue_name():
    try:
        return settings.events_queue_name
    except (AttributeError, ValueError):
        logger.warning(
            f"settings.events_queue_name not found, using default: '{DEFAULT_EVENTS_QUEUE_NAME}'",
        )
        return DEFAULT_EVENTS_QUEUE_NAME


def get_events_topic_name():
    try:
        return settings.events_topic_name
    except (AttributeError, ValueError):
        logger.warning(
            f"settings.events_topic_name not found, using default: '{DEFAULT_EVENTS_TOPIC_NAME}'",
        )
        return DEFAULT_EVENTS_TOPIC_NAME


# Test data generation
def generate_test_event():
    """Generate test event data"""
    event_types = [
        "server_startup",
        "server_shutdown",
        "cpu_usage_high",
        "memory_usage_high",
        "disk_space_low",
        "network_traffic_spike",
        "application_error",
        "database_connection_failure",
    ]

    sources = [
        "web_server_01",
        "app_server_02",
        "database_01",
        "load_balancer",
        "cache_server",
        "auth_service",
    ]

    # Generate a unique test ID for tracking this specific test event
    test_id = f"test_{uuid.uuid4()}"

    # Use the exact format expected by the event consumer:
    # {
    #   "source": str,
    #   "event_type": str,
    #   "timestamp": str (ISO format),
    #   "raw_payload": dict
    # }
    timestamp = datetime.now(timezone.utc).isoformat()

    # Generate random event data with a consistent format
    # This should match the StandardEventMessage format expected by the consumer
    return {
        "source": random.choice(sources),
        "event_type": random.choice(event_types),
        "timestamp": timestamp,
        "raw_payload": {
            "test_id": test_id,
            "severity": random.choice(["info", "warning", "error", "critical"]),
            "message": f"Test message - {datetime.now().isoformat()}",
            "details": {
                "cpu": random.randint(0, 100),
                "memory": random.randint(0, 100),
                "disk": random.randint(0, 100),
                "test_value": random.random(),
            },
        },
    }, test_id


async def test_rabbitmq_connection():
    """Test RabbitMQ connection and message sending"""
    logger.info("Testing RabbitMQ connection...")
    success = False
    test_id = str(uuid.uuid4())
    test_data = {
        "test_id": test_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    try:
        logger.info(f"RabbitMQ URL: {settings.rabbit_url}")

        # Connect to RabbitMQ
        connection = await aio_pika.connect_robust(
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
            "test_exchange", type="direct", durable=False, auto_delete=True,
        )

        # Declare queue
        queue = await channel.declare_queue(
            "test_queue", durable=False, auto_delete=True,
        )

        # Bind queue to exchange
        await queue.bind(exchange, routing_key="test")

        logger.info("RabbitMQ connection successful!")

        # Send test message
        await exchange.publish(
            Message(
                body=json.dumps(test_data).encode(),
                content_type="application/json",
            ),
            routing_key="test",
        )

        logger.info(f"Successfully sent test message to RabbitMQ: {test_data}")

        # Consume the message
        async with queue.iterator() as queue_iterator:
            # Get one message and break the loop
            async for message in queue_iterator:
                async with message.process():
                    received_data = json.loads(message.body.decode())
                    logger.info(f"Received message from RabbitMQ: {received_data}")

                    if received_data.get("test_id") == test_id:
                        success = True
                        logger.info("Message received successfully, data matched!")
                        break

                # Only process one message
                break

        # Close the connection
        await connection.close()

        return success, test_id, test_data

    except Exception as e:
        logger.error(f"RabbitMQ test failed: {e}")
        return False, test_id, test_data


async def test_event_publishing(num_events=3):
    """Test publishing events through message queue"""
    logger.info(f"Testing event publishing with {num_events} test events")

    # Track test statistics
    success_count = 0
    test_ids = []

    try:
        # Connect to RabbitMQ
        connection = await aio_pika.connect_robust(
            host=settings.rabbit_host,
            port=settings.rabbit_port,
            login=settings.rabbit_user,
            password=settings.rabbit_pass,
            virtualhost=settings.rabbit_vhost,
        )

        # Create channel
        channel = await connection.channel()

        # Declare exchange - must match the actual system's configuration
        exchange = await channel.declare_exchange(
            "webhook_events", type="topic", durable=False, auto_delete=True,
        )

        # Create publisher function to simulate MessagePublisher
        async def publish_test_event(test_id, event_type="test"):
            test_data = {
                "id": test_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "type": event_type,
                "content": f"Test event {test_id}",
                "source": "test_message_queue",
            }

            routing_key = f"test.{event_type}"

            # Send using RabbitMQ
            await exchange.publish(
                Message(
                    body=json.dumps(test_data).encode(),
                    content_type="application/json",
                ),
                routing_key=routing_key,
            )
            logger.info(
                f"RabbitMQ - Successfully sent test event {i+1}/{num_events} with ID {test_id}",
            )

            return test_id

        # Send test events
        for i in range(num_events):
            test_id = str(uuid.uuid4())
            test_ids.append(await publish_test_event(test_id))
            success_count += 1

        # Close the connection
        await connection.close()

        logger.info(
            f"Event publishing test completed: {success_count}/{num_events} events published successfully",
        )
        return success_count == num_events, test_ids

    except Exception as e:
        logger.error(f"Event publishing test failed: {e}")
        return False, test_ids


async def perform_system_test():
    """
    Perform a complete system test of messaging capabilities.

    This test ensures:
    1. RabbitMQ connectivity
    2. End-to-end message sending and receiving

    :return: True if all tests passed, False otherwise
    """
    logger.info("Starting system message queue test")

    # Test 1: RabbitMQ connection test
    rmq_test, rmq_test_id, rmq_test_data = await test_rabbitmq_connection()

    # Test 2: Event publishing test
    e2e_test, test_ids = await test_event_publishing(num_events=2)

    # Evaluate results
    tests_passed = 0
    tests_total = 2  # Number of tests performed

    if rmq_test:
        tests_passed += 1
        logger.info("✅ RabbitMQ connection test passed")
    else:
        logger.error("❌ RabbitMQ connection test failed")

    if e2e_test:
        tests_passed += 1
        logger.info("✅ Event publishing test passed")
    else:
        logger.error("❌ Event publishing test failed")

    success_rate = tests_passed / tests_total * 100
    logger.info(
        f"System message queue test completed: {tests_passed}/{tests_total} tests passed ({success_rate:.1f}%)",
    )

    # Print detailed test results
    logger.info("=== Detailed Test Results ===")
    logger.info(
        f"RabbitMQ Connection Test: {'✅ Success' if rmq_test else '❌ Failed'}",
    )

    if rmq_test or e2e_test:
        logger.info(
            f"Message Queue System Status: {'🟢 Operational' if success_rate == 100 else '🟠 Partially Operational' if success_rate > 0 else '🔴 Non-Operational'}",
        )
    else:
        logger.error("Message Queue System Status: 🔴 Non-Operational")
        logger.error(
            "CRITICAL: Message queue tests failed. System may not process events properly.",
        )
        logger.info("📋 Troubleshooting checklist:")
        logger.info("1. Check if RabbitMQ server is running")
        logger.info("2. Verify connection settings in .env file")
        logger.info("3. Ensure firewalls allow connection to message queue")
        logger.info("4. Ensure message queue is properly configured")

    return success_rate == 100


# Run tests if executed directly
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger.info("Starting message queue tests...")

    # Run the system test
    asyncio.run(perform_system_test())
