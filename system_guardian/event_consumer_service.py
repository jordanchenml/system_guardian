"""
Event Consumer Service

This module provides a standalone service that consumes event messages
from Kafka and RabbitMQ and stores them in the database.

Usage:
    python -m system_guardian.event_consumer_service
"""

import asyncio
import signal
import sys
from contextlib import asynccontextmanager

from loguru import logger
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from system_guardian.db.models import load_all_models
from system_guardian.services.consumers.event_consumer import EventConsumer
from system_guardian.settings import settings


async def main():
    """Run the event consumer service."""
    # Configure logging
    logger.info("Starting Event Consumer Service")
    
    # Load all database models
    load_all_models()
    
    # Create database engine and session factory
    engine = create_async_engine(str(settings.db_url), echo=settings.db_echo)
    session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
    )
    
    # Create event consumer
    consumer = EventConsumer(session_factory)
    
    # Setup signal handlers for graceful shutdown
    loop = asyncio.get_running_loop()
    signals = (signal.SIGINT, signal.SIGTERM)
    for sig in signals:
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(consumer, engine)))
    
    # Start the consumer
    try:
        await consumer.start()
    except asyncio.CancelledError:
        logger.info("Consumer was cancelled, shutting down...")
    finally:
        await shutdown(consumer, engine)
    
    logger.info("Event Consumer Service terminated")


async def shutdown(consumer, engine):
    """Gracefully shut down the service."""
    logger.info("Shutting down event consumer service...")
    
    # Stop the consumer
    await consumer.stop()
    
    # Close database connections
    await engine.dispose()
    
    # Exit the program
    asyncio.get_running_loop().stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down...")
        sys.exit(0) 