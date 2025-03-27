"""Tests for incident detection and management workflow."""

import asyncio
import os
import uuid
from datetime import datetime
from typing import Dict, Any

import pytest
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession

from system_guardian.db.models.incidents import Incident, Event
from system_guardian.services.ai.incident_detector import IncidentDetector
from system_guardian.services.ai.severity_classifier import SeverityClassifier
from system_guardian.services.config import ConfigManager
from system_guardian.services.consumers.event_consumer import EventConsumer
from system_guardian.settings import settings


async def create_test_incident(session: AsyncSession) -> Incident:
    """Create a test incident in the database."""
    # Create incident
    incident = Incident(
        source="test",
        title=f"Test Incident {uuid.uuid4()}",
        description="This is a test incident created for testing purposes.",
        severity="medium",
        status="open",
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        closed_at=None,
        resolution=None,
        assignee=None,
        metadata={"test": True},
    )

    # Add and commit
    session.add(incident)
    await session.commit()

    # Refresh to get ID
    await session.refresh(incident)

    return incident


@pytest.mark.asyncio
async def test_incident_detector_workflow(dbsession: AsyncSession):
    """Test the complete incident detection workflow."""
    # Create configuration
    config = ConfigManager()

    # Create incident detector components
    severity_classifier = SeverityClassifier()

    # Create incident detector
    incident_detector = IncidentDetector(
        config_manager=config,
        severity_classifier=severity_classifier,
    )

    # Create a test event consumer without Kafka
    consumer = EventConsumer(
        db_session_factory=lambda: dbsession,
        rmq_channel_pool=None,
    )

    # Generate test event data
    source = "github"
    event_type = "issue"
    content = {
        "action": "opened",
        "issue": {
            "number": 123,
            "title": "CRITICAL: System is down and unresponsive",
            "body": "The production system is completely down. Users are unable to access the application. This is causing a major outage and needs immediate attention.",
            "state": "open",
            "created_at": datetime.utcnow().isoformat(),
        },
        "repository": {
            "name": "test-repo",
            "full_name": "test-org/test-repo",
        },
        "sender": {
            "login": "test-user",
        },
    }

    # Check conditions
    conditions_met = await incident_detector.check_event_conditions(
        content, source, event_type,
    )
    assert conditions_met is True, "Event conditions should be met"

    # Create event in database
    event = Event(
        source=source,
        event_type=event_type,
        content=content,
        created_at=datetime.utcnow(),
        incident_id=None,  # No incident associated yet
    )
    dbsession.add(event)
    await dbsession.commit()
    await dbsession.refresh(event)

    # Create incident from event
    incident = await incident_detector.create_incident_from_event(
        session=dbsession,
        source=source,
        event_type=event_type,
        payload=content,
        event_id=event.id,
    )

    # Verify incident was created
    assert incident is not None, "Incident should be created"
    assert incident.id is not None, "Incident should have an ID"
    assert incident.source == source, "Incident source should match event source"
    assert "CRITICAL" in incident.title, "Incident title should contain severity"
    assert incident.severity in [
        "high",
        "critical",
    ], "Severity should be high or critical"

    # Verify event was linked to incident
    await dbsession.refresh(event)
    assert (
        event.incident_id == incident.id
    ), "Event should be linked to the created incident"

    # Test retrieving the incident
    stmt = select(Incident).where(Incident.id == incident.id)
    result = await dbsession.execute(stmt)
    retrieved_incident = result.scalar_one_or_none()

    assert (
        retrieved_incident is not None
    ), "Incident should be retrievable from database"
    assert (
        retrieved_incident.id == incident.id
    ), "Retrieved incident should have the same ID"

    # Clean up (optional for tests as they usually use a test database)
    await dbsession.delete(incident)
    await dbsession.delete(event)
    await dbsession.commit()
