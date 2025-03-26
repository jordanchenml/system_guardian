from typing import Awaitable, Callable
import asyncio
import logging

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from system_guardian.db.meta import meta
from system_guardian.db.models import load_all_models
from system_guardian.services.kafka.lifetime import init_kafka, shutdown_kafka
from system_guardian.services.rabbit.lifetime import init_rabbit, shutdown_rabbit
from system_guardian.services.consumers.event_consumer import EventConsumer
from system_guardian.settings import settings
from system_guardian.logging_config import configure_sqlalchemy_logging


def _setup_db(app: FastAPI) -> None:  # pragma: no cover
    """
    Creates connection to the database.

    This function creates SQLAlchemy engine instance,
    session_factory for creating sessions
    and stores them in the application's state property.

    :param app: fastAPI application.
    """
    # 確保 SQLAlchemy 日誌被禁用
    configure_sqlalchemy_logging()
    for logger_name in ['sqlalchemy', 'sqlalchemy.engine', 'sqlalchemy.pool', 'sqlalchemy.orm']:
        logging.getLogger(logger_name).setLevel(logging.CRITICAL + 10)
        logging.getLogger(logger_name).disabled = True
        logging.getLogger(logger_name).propagate = False
    
    # 創建引擎時明確禁用 echo
    engine = create_async_engine(str(settings.db_url), echo=False)
    session_factory = async_sessionmaker(
        engine,
        expire_on_commit=False,
    )
    app.state.db_engine = engine
    app.state.db_session_factory = session_factory


async def _create_tables() -> None:  # pragma: no cover
    """Populate tables with predefined data."""
    load_all_models()


async def _start_event_consumer(app: FastAPI) -> None:  # pragma: no cover
    """
    Start the event consumer.
    
    :param app: fastAPI application.
    """
    # Create event consumer
    event_consumer = EventConsumer(app.state.db_session_factory)
    
    # Store it in app state for later reference
    app.state.event_consumer = event_consumer
    
    # Start it as a background task
    app.state.event_consumer_task = asyncio.create_task(event_consumer.start())


async def _stop_event_consumer(app: FastAPI) -> None:  # pragma: no cover
    """
    Stop the event consumer.
    
    :param app: fastAPI application.
    """
    if hasattr(app.state, "event_consumer"):
        # Signal consumer to stop
        await app.state.event_consumer.stop()
        
        # Wait for the task to complete
        if hasattr(app.state, "event_consumer_task"):
            try:
                await asyncio.wait_for(app.state.event_consumer_task, timeout=5.0)
            except asyncio.TimeoutError:
                # If it doesn't stop in time, cancel it
                app.state.event_consumer_task.cancel()


def register_startup_event(
    app: FastAPI,
) -> Callable[[], Awaitable[None]]:  # pragma: no cover
    """
    Actions to run on application startup.

    This function uses fastAPI app to store data
    in the state, such as db_engine.

    :param app: the fastAPI application.
    :return: function that actually performs actions.
    """

    @app.on_event("startup")
    async def _startup() -> None:  # noqa: WPS430
        app.middleware_stack = None
        _setup_db(app)
        await _create_tables()
        init_rabbit(app)
        await init_kafka(app)
        await _start_event_consumer(app)
        app.middleware_stack = app.build_middleware_stack()
        pass  # noqa: WPS420

    return _startup


def register_shutdown_event(
    app: FastAPI,
) -> Callable[[], Awaitable[None]]:  # pragma: no cover
    """
    Actions to run on application's shutdown.

    :param app: fastAPI application.
    :return: function that actually performs actions.
    """

    @app.on_event("shutdown")
    async def _shutdown() -> None:  # noqa: WPS430
        await _stop_event_consumer(app)
        await app.state.db_engine.dispose()
        await shutdown_rabbit(app)
        await shutdown_kafka(app)
        pass  # noqa: WPS420

    return _shutdown
