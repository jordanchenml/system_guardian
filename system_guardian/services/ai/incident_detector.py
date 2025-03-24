"""Incident detector service for automatically creating incidents from events."""
import json
import hashlib
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta

from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import func, and_, text

from system_guardian.db.models.incidents import Event, Incident
from system_guardian.services.ai.severity_classifier import SeverityClassifierService
from system_guardian.services.config.incident_rules import ConfigManager, IncidentDetectionConfig
from system_guardian.services.ai.incident_similarity import IncidentSimilarityService, IncidentEmbedding
from system_guardian.services.vector_db.dependencies import get_qdrant_client


class IncidentDetector:
    """Service for automatically detecting and creating incidents from events."""

    def __init__(self, db_session_factory, severity_classifier=None, config_manager=None):
        """
        Initialize the incident detector.

        :param db_session_factory: Factory for creating database sessions
        :param severity_classifier: Optional severity classifier service
        :param config_manager: Optional configuration manager instance
        """
        self.db_session_factory = db_session_factory
        self.severity_classifier = severity_classifier or SeverityClassifierService()
        self.config_manager = config_manager or ConfigManager()
        self.config = None
        
    async def ensure_config_loaded(self) -> IncidentDetectionConfig:
        """
        Ensure configuration is loaded.
        
        :returns: Incident detection configuration
        """
        if self.config is None:
            self.config = await self.config_manager.load_config()
        return self.config
    
    def get_rule_for_event(self, source: str, event_type: str) -> Optional[Dict[str, Any]]:
        """
        Get the rule for a specific event source and type.
        
        :param source: Event source (e.g., 'github', 'jira')
        :param event_type: Event type (e.g., 'issue', 'pull_request')
        :returns: Rule dictionary or None if no rule exists
        """
        if not self.config:
            return None
        
        return self.config.get_rules_for_event(source, event_type)
    
    async def check_event_thresholds(self, session: AsyncSession, source: str, 
                                    event_type: str, time_minutes: int = 60) -> bool:
        """
        Check if events exceed threshold in a specific time window.
        
        :param session: Database session
        :param source: Event source
        :param event_type: Event type
        :param time_minutes: Time window in minutes
        :returns: True if threshold is exceeded, False otherwise
        """
        # Ensure config is loaded
        await self.ensure_config_loaded()
        
        # Get relevant rule
        rule = self.get_rule_for_event(source, event_type)
        if not rule:
            logger.debug(f"No rule found for {source}/{event_type}")
            return False
            
        threshold = rule.min_events_threshold
        window = rule.time_window_minutes or time_minutes
        
        # Query event count within time window
        time_cutoff = datetime.utcnow() - timedelta(minutes=window)
        
        query = (
            select(func.count(Event.id))
            .where(
                and_(
                    Event.source == source,
                    Event.event_type == event_type,
                    Event.created_at >= time_cutoff,
                    Event.incident_id.is_(None)  # Events not yet associated with an incident
                )
            )
        )
        
        result = await session.execute(query)
        count = result.scalar_one()
        
        logger.info(f"Found {count} {source}/{event_type} events in the last {window} minutes (threshold: {threshold})")
        return count >= threshold
    
    async def analyze_content_for_keywords(self, payload: Dict[str, Any], 
                                          source: str, event_type: str) -> bool:
        """
        Analyze event content for keywords indicative of incidents.
        
        :param payload: Event payload
        :param source: Event source
        :param event_type: Event type
        :returns: True if keywords found, False otherwise
        """
        # Ensure config is loaded
        await self.ensure_config_loaded()
        
        # Get all applicable keywords
        keywords = self.config.get_keywords_for_event(source, event_type)
        if not keywords:
            return False
            
        # Extract text content from the payload
        text_content = self._extract_text_content(payload, source, event_type)
        text_content = text_content.lower()
        
        found_keywords = []
        for keyword in keywords:
            if keyword.lower() in text_content:
                found_keywords.append(keyword)
                
        if found_keywords:
            logger.info(f"Found keywords in {source}/{event_type} event: {', '.join(found_keywords)}")
            return True
                
        return False
        
    def _extract_text_content(self, payload: Dict[str, Any], source: str, event_type: str) -> str:
        """
        Extract text content from different source event payloads.
        
        :param payload: Event payload
        :param source: Event source
        :param event_type: Event type
        :returns: Text content from the payload
        """
        if source == "github":
            if event_type == "issue":
                return f"{payload.get('issue', {}).get('title', '')} {payload.get('issue', {}).get('body', '')}"
            elif event_type == "pull_request":
                return f"{payload.get('pull_request', {}).get('title', '')} {payload.get('pull_request', {}).get('body', '')}"
        
        elif source == "jira":
            if "issue" in payload:
                issue = payload.get("issue", {})
                fields = issue.get("fields", {})
                return f"{fields.get('summary', '')} {fields.get('description', '')}"
            return str(payload)
            
        # Default case - convert payload to string
        try:
            return json.dumps(payload)
        except:
            return str(payload)
    
    async def check_event_conditions(self, payload: Dict[str, Any], source: str, event_type: str) -> bool:
        """
        Check if event meets the conditions for creating an incident.
        
        :param payload: Event payload
        :param source: Event source
        :param event_type: Event type
        :returns: True if conditions are met, False otherwise
        """
        # Ensure config is loaded
        await self.ensure_config_loaded()
        
        # Get relevant rule
        rule = self.get_rule_for_event(source, event_type)
        if not rule:
            return False
            
        # Check if this is an event type that should create an incident
        if rule.create_incident_on:
            # Extract action or state from payload
            action = self._extract_action_from_payload(payload, source, event_type)
            if action not in rule.create_incident_on:
                return False
                
        # Check additional conditions
        if rule.field_conditions:
            for key, value in rule.field_conditions.items():
                payload_value = self._extract_value_from_payload(payload, key, source, event_type)
                if payload_value != value:
                    return False
                    
        return True
    
    def _extract_action_from_payload(self, payload: Dict[str, Any], source: str, event_type: str) -> str:
        """
        Extract action or state from event payload.
        
        :param payload: Event payload
        :param source: Event source
        :param event_type: Event type
        :returns: Action or state
        """
        if source == "github":
            if event_type == "issue":
                return payload.get("action", "unknown")
            elif event_type == "pull_request":
                return payload.get("action", "unknown")
        
        elif source == "jira":
            if "webhookEvent" in payload:
                return payload["webhookEvent"].split("_")[-1]  # Extract 'created', 'updated', etc.
                
        return "unknown"
    
    def _extract_value_from_payload(self, payload: Dict[str, Any], key: str, 
                                   source: str, event_type: str) -> Any:
        """
        Extract a value from event payload.
        
        :param payload: Event payload
        :param key: Key to extract
        :param source: Event source
        :param event_type: Event type
        :returns: Extracted value
        """
        if source == "github":
            if event_type == "pull_request" and key in ["state", "merged"]:
                return payload.get("pull_request", {}).get(key)
            
        # Generic fallback - direct key access
        return payload.get(key)
    
    async def create_incident_from_event(self, session: AsyncSession, 
                                        source: str, event_type: str, 
                                        payload: Dict[str, Any], 
                                        event_id: int) -> Optional[Incident]:
        """
        Create a new incident from an event.
        
        :param session: Database session
        :param source: Event source
        :param event_type: Event type
        :param payload: Event payload
        :param event_id: ID of the triggering event
        :returns: Created incident or None if failed
        """
        try:
            # Extract title and description
            title, description = self._extract_title_description(source, event_type, payload)
            
            logger.info(f"Auto-creating incident from {source}/{event_type} event")
            
            # Auto-classify severity
            severity = await self.severity_classifier.classify_severity(
                incident_title=title,
                incident_description=description,
                source=source,
                events_data=[payload]
            )
            
            # Create new incident
            new_incident = Incident(
                title=title,
                description=description,
                severity=severity,
                status="open",  # New incidents default to open
                source=source,
                created_at=datetime.utcnow()
            )
            
            session.add(new_incident)
            await session.commit()
            await session.refresh(new_incident)
            
            # Update event incident relation
            await self._update_event_incident_relation(session, event_id, new_incident.id)
            
            # Associate other related unlinked events
            await self._associate_related_events(session, source, event_type, new_incident.id)
            
            # Add incident to vector database for similarity search
            try:
                # Get Qdrant client
                qdrant_client = get_qdrant_client()
                
                # Initialize similarity service
                similarity_service = IncidentSimilarityService(qdrant_client=qdrant_client)
                
                # Create embedding model
                incident_embedding = IncidentEmbedding(
                    incident_id=str(new_incident.id),
                    title=new_incident.title,
                    description=new_incident.description,
                    severity=new_incident.severity,
                    status=new_incident.status,
                    source=new_incident.source,
                    created_at=new_incident.created_at.isoformat()
                )
                
                # Index in vector database
                indexed = await similarity_service.index_incident(incident_embedding)
                if indexed:
                    logger.info(f"Indexed incident {new_incident.id} in vector database")
                else:
                    logger.error(f"Failed to index incident {new_incident.id} in vector database")
            except Exception as e:
                logger.error(f"Error indexing incident in vector database: {str(e)}")
            
            logger.info(f"Auto-created incident: {new_incident.id} - {title} (severity: {severity})")
            return new_incident
            
        except Exception as e:
            logger.error(f"Failed to create incident: {str(e)}")
            return None
            
    def _extract_title_description(self, source: str, event_type: str, 
                                 payload: Dict[str, Any]) -> Tuple[str, str]:
        """
        Extract title and description from event payload.
        
        :param source: Event source
        :param event_type: Event type
        :param payload: Event payload
        :returns: Tuple of (title, description)
        """
        title = f"Auto-detected {source} {event_type} issue"
        description = f"Automatically generated from {source} {event_type} event."
        
        if source == "github":
            if event_type == "issue":
                issue = payload.get("issue", {})
                title = f"GitHub Issue: {issue.get('title', 'No Title')}"
                description = issue.get("body", "No description") or "No description"
                description += f"\n\nURL: {issue.get('html_url', '')}"
                description += f"\n\nReporter: {issue.get('user', {}).get('login', 'unknown')}"
                
            elif event_type == "pull_request":
                pr = payload.get("pull_request", {})
                title = f"GitHub PR: {pr.get('title', 'No Title')}"
                description = pr.get("body", "No description") or "No description"
                description += f"\n\nURL: {pr.get('html_url', '')}"
                description += f"\n\nAuthor: {pr.get('user', {}).get('login', 'unknown')}"
                description += f"\n\nStatus: {pr.get('state', 'unknown')}, Merged: {pr.get('merged', False)}"
                
        elif source == "jira":
            if "issue" in payload:
                issue = payload.get("issue", {})
                fields = issue.get("fields", {})
                title = f"Jira Issue: {fields.get('summary', 'No Title')}"
                description = fields.get("description", "No description") or "No description"
                description += f"\n\nKey: {issue.get('key', '')}"
                
                if "priority" in fields:
                    description += f"\n\nPriority: {fields['priority'].get('name', 'unknown')}"
                    
                if "reporter" in fields:
                    description += f"\n\nReporter: {fields['reporter'].get('displayName', 'unknown')}"
            else:
                # For other Jira webhook events
                title = f"Jira Event: {payload.get('webhookEvent', 'unknown')}"
                description = "Jira webhook event details:\n" + json.dumps(payload, indent=2)[:1000] + "..."
                
        # Add timestamp to description
        description += f"\n\nDetected at: {datetime.utcnow().isoformat()}"
        
        return title, description
    
    async def _update_event_incident_relation(self, session: AsyncSession, 
                                            event_id: int, incident_id: int) -> None:
        """
        Update the incident relation for an event.
        
        :param session: Database session
        :param event_id: Event ID
        :param incident_id: Incident ID
        """
        # Get the event
        event = await session.get(Event, event_id)
        if event:
            # Update incident ID
            event.incident_id = incident_id
            await session.commit()
            logger.debug(f"Associated event {event_id} with incident {incident_id}")
    
    async def _associate_related_events(self, session: AsyncSession, 
                                      source: str, event_type: str, 
                                      incident_id: int, max_age_hours: int = 24) -> None:
        """
        Associate related unlinked events with the new incident.
        
        :param session: Database session
        :param source: Event source
        :param event_type: Event type
        :param incident_id: Incident ID
        :param max_age_hours: Maximum age of events to associate (in hours)
        """
        # Find related events with no incident association
        time_cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)
        
        query = (
            select(Event)
            .where(
                and_(
                    Event.source == source,
                    Event.event_type == event_type,
                    Event.created_at >= time_cutoff,
                    Event.incident_id.is_(None)  # Not associated with any incident
                )
            )
            .order_by(Event.created_at.desc())
        )
        
        result = await session.execute(query)
        events = result.scalars().all()
        
        # Link events to this incident
        for event in events:
            event.incident_id = incident_id
            
        if events:
            await session.commit()
            logger.info(f"Associated {len(events)} additional events with incident {incident_id}") 