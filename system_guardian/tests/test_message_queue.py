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
        logger.warning(f"settings.events_queue_name not found, using default: '{DEFAULT_EVENTS_QUEUE_NAME}'")
        return DEFAULT_EVENTS_QUEUE_NAME

def get_events_topic_name():
    try:
        return settings.events_topic_name
    except (AttributeError, ValueError):
        logger.warning(f"settings.events_topic_name not found, using default: '{DEFAULT_EVENTS_TOPIC_NAME}'")
        return DEFAULT_EVENTS_TOPIC_NAME

# Test data generation
def generate_test_event():
    """Generate test event data"""
    event_types = [
        "server_startup", "server_shutdown", "cpu_usage_high", 
        "memory_usage_high", "disk_space_low", "network_traffic_spike",
        "application_error", "database_connection_failure"
    ]
    
    sources = [
        "web_server_01", "app_server_02", "database_01", 
        "load_balancer", "cache_server", "auth_service"
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
                "test_value": random.random()
            }
        }
    }, test_id


async def test_rabbitmq_connection():
    """Test RabbitMQ connection and message sending"""
    logger.info("Testing RabbitMQ connection...")
    
    # Display connection settings
    logger.info(f"RabbitMQ URL: {settings.rabbit_url}")
    
    try:
        # Establish connection
        connection = await aio_pika.connect_robust(str(settings.rabbit_url))
        
        async with connection:
            logger.info("RabbitMQ connection successful!")
            
            # Create channel
            channel = await connection.channel()
            
            # Declare queue
            queue_name = "test_events"
            queue = await channel.declare_queue(
                queue_name,
                durable=True,
                auto_delete=False
            )
            
            logger.info(f"Successfully declared queue: {queue_name}")
            
            # Send test message
            test_data, test_id = generate_test_event()
            message_body = json.dumps(test_data).encode()
            
            await channel.default_exchange.publish(
                aio_pika.Message(
                    body=message_body,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                    content_type="application/json"
                ),
                routing_key=queue_name
            )
            
            logger.info(f"Successfully sent test message to RabbitMQ: {test_data}")
            
            # Attempt to receive message to confirm
            message_count = 0
            async with queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        received_data = json.loads(message.body.decode())
                        logger.info(f"Received message from RabbitMQ: {received_data}")
                        message_count += 1
                        if message_count >= 1:  # Only read one message and exit
                            break
            
            return True, test_id, test_data
    except Exception as e:
        logger.error(f"RabbitMQ test failed: {e}")
        return False, None, None


async def test_kafka_connection():
    """Test Kafka connection and message sending"""
    logger.info("Testing Kafka connection...")
    
    # Display connection settings
    logger.info(f"Kafka Bootstrap Servers: {settings.kafka_bootstrap_servers}")
    
    producer = None
    consumer = None
    
    try:
        # Create producer
        producer = aiokafka.AIOKafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers
        )
        await producer.start()
        
        # Test topic
        topic = "test_events"
        
        # Send test message
        test_data, test_id = generate_test_event()
        message_value = json.dumps(test_data).encode()
        
        await producer.send_and_wait(topic, message_value)
        logger.info(f"Successfully sent test message to Kafka: {test_data}")
        
        # Create consumer
        consumer = aiokafka.AIOKafkaConsumer(
            topic,
            bootstrap_servers=settings.kafka_bootstrap_servers,
            group_id="test_consumer",
            auto_offset_reset="earliest"
        )
        await consumer.start()
        
        # Read messages
        message_count = 0
        async for msg in consumer:
            try:
                received_data = json.loads(msg.value.decode())
                logger.info(f"Received message from Kafka: {received_data}")
                message_count += 1
                if message_count >= 1:  # Only read one message and exit
                    break
            except Exception as e:
                logger.error(f"Error processing Kafka message: {e}")
        
        return True, test_id, test_data
        
    except Exception as e:
        logger.error(f"Kafka test failed: {e}")
        return False, None, None
    finally:
        # Close connections
        if producer:
            await producer.stop()
        if consumer:
            await consumer.stop()


async def send_test_events_to_system():
    """Send test events to the system"""
    logger.info("Sending test events to system...")
    
    # Decide which message queue to use
    use_rabbitmq = True  # Can be switched as needed
    
    test_ids = []
    test_events = []
    
    try:
        if use_rabbitmq:
            # Use RabbitMQ to send
            connection = await aio_pika.connect_robust(str(settings.rabbit_url))
            
            async with connection:
                channel = await connection.channel()
                
                # Use the actual queue name used by the system
                queue_name = get_events_queue_name()
                queue = await channel.declare_queue(
                    queue_name,
                    durable=True,
                    auto_delete=False
                )
                
                # Send test events
                num_events = 5  # Reduced number for easier tracking
                for i in range(num_events):
                    test_data, test_id = generate_test_event()
                    message_body = json.dumps(test_data).encode()
                    
                    # Keep track of test IDs for later verification
                    test_ids.append(test_id)
                    test_events.append(test_data)
                    
                    await channel.default_exchange.publish(
                        aio_pika.Message(
                            body=message_body,
                            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
                            content_type="application/json"
                        ),
                        routing_key=queue_name
                    )
                    
                    logger.info(f"RabbitMQ - Successfully sent test event {i+1}/{num_events} with ID {test_id}")
                    await asyncio.sleep(0.2)  # Small pause to avoid sending too quickly
        else:
            # Use Kafka to send
            producer = aiokafka.AIOKafkaProducer(
                bootstrap_servers=settings.kafka_bootstrap_servers
            )
            await producer.start()
            
            # Use the actual topic name used by the system
            topic = get_events_topic_name()
            
            # Send test events
            try:
                num_events = 5  # Reduced number for easier tracking
                for i in range(num_events):
                    test_data, test_id = generate_test_event()
                    message_value = json.dumps(test_data).encode()
                    
                    # Keep track of test IDs for later verification
                    test_ids.append(test_id)
                    test_events.append(test_data)
                    
                    await producer.send_and_wait(topic, message_value)
                    logger.info(f"Kafka - Successfully sent test event {i+1}/{num_events} with ID {test_id}")
                    await asyncio.sleep(0.2)  # Small pause to avoid sending too quickly
            finally:
                await producer.stop()
                
        logger.info("Test events sending completed!")
        return True, test_ids, test_events
        
    except Exception as e:
        logger.error(f"Failed to send test events: {e}")
        return False, [], []


async def verify_events_in_database(test_ids, wait_time=5):
    """
    Verify that test events have been written to the database
    
    Args:
        test_ids: List of test IDs to look for in the database
        wait_time: Time to wait before checking the database (seconds)
    
    Returns:
        tuple: (success, found_events, missing_events)
    """
    logger.info(f"Waiting {wait_time} seconds for events to be processed...")
    await asyncio.sleep(wait_time)
    
    logger.info(f"Verifying {len(test_ids)} events in database...")
    
    # Load models
    load_all_models()
    
    # Create database engine
    engine = create_async_engine(
        str(settings.db_url),
        echo=False,
        pool_pre_ping=True
    )
    
    session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
        class_=AsyncSession
    )
    
    found_events = []
    missing_events = []
    
    try:
        async with session_factory() as session:
            # Check for each test ID
            for test_id in test_ids:
                logger.info(f"Checking for event with test_id: {test_id}")
                
                # Query using direct SQL for more flexibility
                query = text("""
                    SELECT id, source, event_type, created_at 
                    FROM events 
                    WHERE content::text LIKE :pattern
                    ORDER BY created_at DESC
                    LIMIT 1
                """)
                
                result = await session.execute(
                    query, 
                    {"pattern": f"%{test_id}%"}
                )
                
                row = result.first()
                
                if row:
                    logger.info(f"✅ Found event in database: ID={row[0]}, Source={row[1]}, Type={row[2]}, Created={row[3]}")
                    found_events.append({
                        "db_id": row[0],
                        "source": row[1],
                        "event_type": row[2],
                        "created_at": row[3],
                        "test_id": test_id
                    })
                else:
                    logger.warning(f"❌ Event with test_id {test_id} not found in database")
                    missing_events.append(test_id)
            
            # Get the count of events in the last minute
            count_query = text("""
                SELECT COUNT(*) FROM events
                WHERE created_at > NOW() - INTERVAL '1 minute'
            """)
            count_result = await session.execute(count_query)
            recent_count = count_result.scalar_one()
            logger.info(f"Total events created in the last minute: {recent_count}")
            
            # Check consumer logs if events are missing
            if missing_events:
                logger.warning("Some events are missing. Checking recent events...")
                
                # Get the most recent events regardless of test_id
                recent_query = text("""
                    SELECT id, source, event_type, created_at, content::text
                    FROM events
                    ORDER BY created_at DESC
                    LIMIT 10
                """)
                
                recent_result = await session.execute(recent_query)
                recent_events = recent_result.fetchall()
                
                logger.info("Recent events in database:")
                for event in recent_events:
                    logger.info(f"ID={event[0]}, Source={event[1]}, Type={event[2]}, Created={event[3]}")
                    # Log a snippet of the content to help troubleshoot
                    content_snippet = event[4][:100] + "..." if len(event[4]) > 100 else event[4]
                    logger.debug(f"Content snippet: {content_snippet}")
                
                # Also check if the consumer is running
                logger.info("Checking if event consumer is running...")
                try:
                    import psutil
                    consumer_running = False
                    for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                        cmdline = proc.info.get('cmdline')
                        if cmdline and any('event_consumer_service' in cmd for cmd in cmdline):
                            consumer_running = True
                            logger.info(f"Event consumer process found: PID={proc.info['pid']}")
                    if not consumer_running:
                        logger.warning("No event consumer process found! Events won't be processed.")
                except ImportError:
                    logger.warning("psutil not installed, can't check if consumer is running")
                
        # Return results
        success = len(found_events) == len(test_ids)
        return success, found_events, missing_events
        
    except Exception as e:
        logger.error(f"Error verifying events in database: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False, [], test_ids
    finally:
        await engine.dispose()


async def test_end_to_end_flow():
    """Test the entire flow from message queue to database"""
    logger.info("Testing end-to-end flow: message queue → consumer → database")
    
    # 1. Send test events to the system
    success, test_ids, test_events = await send_test_events_to_system()
    
    if not success or not test_ids:
        logger.error("Failed to send test events, cannot continue end-to-end test")
        return False
    
    logger.info(f"Sent {len(test_ids)} test events to the system")
    
    # 2. Verify events in database
    logger.info("Checking if events were processed and stored in database...")
    
    # Wait longer for events to be processed
    db_success, found_events, missing_events = await verify_events_in_database(test_ids, wait_time=10)
    
    if db_success:
        logger.info("✅ All test events were successfully stored in the database!")
        logger.info(f"Found {len(found_events)} out of {len(test_ids)} events")
        return True
    else:
        logger.warning(f"❌ Some test events were not found in the database")
        logger.warning(f"Found {len(found_events)} out of {len(test_ids)} events")
        logger.warning(f"Missing test IDs: {missing_events}")
        
        # Provide troubleshooting guidance
        logger.info("\n=== Troubleshooting Tips ===")
        logger.info("1. Ensure the event consumer service is running")
        logger.info("2. Check the event consumer service logs for errors")
        logger.info("3. Verify database credentials and connection settings")
        logger.info("4. Ensure message queue (RabbitMQ/Kafka) is properly configured")
        
        return False


async def main():
    """Run all tests"""
    # Setup logging
    logger.remove()
    logger.add(sys.stderr, level="DEBUG")
    
    logger.info("Starting message queue tests...")
    
    # Basic connection tests
    rmq_test, rmq_test_id, rmq_test_data = await test_rabbitmq_connection()
    kafka_test, kafka_test_id, kafka_test_data = await test_kafka_connection()
    
    # End-to-end test (only if basic tests pass)
    e2e_test = False
    if rmq_test or kafka_test:
        logger.info("Basic connection tests passed. Starting end-to-end flow test...")
        e2e_test = await test_end_to_end_flow()
    
    # Display results
    logger.info("\n=== Test Results Summary ===")
    logger.info(f"RabbitMQ Connection Test: {'✅ Success' if rmq_test else '❌ Failed'}")
    logger.info(f"Kafka Connection Test: {'✅ Success' if kafka_test else '❌ Failed'}")
    logger.info(f"End-to-End Flow Test: {'✅ Success' if e2e_test else '❌ Failed'}")
    
    # Determine overall result
    if (rmq_test or kafka_test) and e2e_test:
        logger.info("🎉 All tests passed! Message queue to database flow is working properly!")
        return 0
    else:
        logger.error("❌ Some tests failed, please check the error messages above")
        
        # If end-to-end test failed, give more specific guidance
        if not e2e_test and (rmq_test or kafka_test):
            logger.warning("\nThe message queues are working, but events aren't being written to the database.")
            logger.warning("Possible issues:")
            logger.warning("1. Event consumer service isn't running or has configuration issues")
            logger.warning("2. Database connection issues in the consumer service")
            logger.warning("3. Message format doesn't match what the consumer expects")
            logger.warning("4. Errors during event processing (check consumer logs)")
        
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code) 