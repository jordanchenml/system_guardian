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
from system_guardian.services.ai.severity_classifier import SeverityClassifier
from system_guardian.services.config import ConfigManager, IncidentDetectionConfig
from system_guardian.services.ai.incident_similarity import (
    IncidentSimilarityService,
    IncidentEmbedding,
)
from system_guardian.services.vector_db.qdrant_client import get_qdrant_client
from system_guardian.settings import settings
from openai import AsyncOpenAI


class IncidentDetector:
    """Service for automatically detecting and creating incidents from events."""

    def __init__(
        self,
        config_manager: ConfigManager,
        llm_client: Optional[AsyncOpenAI] = None,
        llm_model: Optional[str] = None,
        severity_classifier: Optional[SeverityClassifier] = None,
    ):
        """
        Initialize the incident detector.

        :param config_manager: Configuration manager instance
        :param llm_client: OpenAI client for LLM-based detection
        :param llm_model: LLM model to use
        :param severity_classifier: Optional severity classifier service
        """
        self.config_manager = config_manager
        self.detection_rules = {}
        self.config_loaded = False
        self.llm_client = llm_client or AsyncOpenAI(api_key=settings.openai_api_key)
        self.llm_model = llm_model or (
            settings.ai_incident_detection_model
            if settings.ai_allow_advanced_models
            else settings.openai_completion_model
        )
        self.severity_classifier = severity_classifier or SeverityClassifier()
        self._last_analysis = None

    async def ensure_config_loaded(self) -> IncidentDetectionConfig:
        """
        Ensure configuration is loaded.

        :returns: Incident detection configuration
        """
        if self.config_loaded is False:
            self.config_loaded = True
            self.config = await self.config_manager.load_config()
        return self.config

    def get_rule_for_event(
        self, source: str, event_type: str
    ) -> Optional[Dict[str, Any]]:
        """
        Get the rule for a specific event source and type.

        :param source: Event source (e.g., 'github', 'jira')
        :param event_type: Event type (e.g., 'issue', 'pull_request')
        :returns: Rule dictionary or None if no rule exists
        """
        if not self.config:
            return None

        return self.config.get_rules_for_event(source, event_type)

    async def check_event_thresholds(
        self,
        session: AsyncSession,
        source: str,
        event_type: str,
        time_minutes: int = 60,
    ) -> bool:
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

        query = select(func.count(Event.id)).where(
            and_(
                Event.source == source,
                Event.event_type == event_type,
                Event.created_at >= time_cutoff,
                Event.incident_id.is_(
                    None
                ),  # Events not yet associated with an incident
            )
        )

        result = await session.execute(query)
        count = result.scalar_one()

        logger.info(
            f"Found {count} {source}/{event_type} events in the last {window} minutes (threshold: {threshold})"
        )
        return count >= threshold

    async def analyze_content_for_keywords(
        self, payload: Dict[str, Any], source: str, event_type: str
    ) -> bool:
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

        # Traditional keyword matching
        found_keywords = []
        for keyword in keywords:
            if keyword.lower() in text_content:
                found_keywords.append(keyword)

        # If we have LLM client available and no keywords found, try semantic search
        if self.llm_client and not found_keywords:
            try:
                # Convert keywords to a searchable format
                keywords_text = ", ".join(keywords)
                query_text = f"Event: {text_content}\n\nDoes this event contain any of these keywords or concepts: {keywords_text}?"

                # Use LLM to determine if there's a match
                response = await self.llm_client.chat.completions.create(
                    model=self.llm_model,
                    messages=[
                        {
                            "role": "system",
                            "content": "You are an incident detection assistant that helps identify if an event contains keywords or concepts that indicate an incident.",
                        },
                        {"role": "user", "content": query_text},
                    ],
                    temperature=0.1,
                    max_tokens=100,
                )

                response_text = response.choices[0].message.content.lower()
                has_semantic_match = any(
                    phrase in response_text
                    for phrase in [
                        "yes",
                        "match",
                        "contains",
                        "found",
                        "present",
                        "detected",
                    ]
                )

                if has_semantic_match:
                    logger.info(
                        f"LLM-enhanced keyword analysis found semantic match in {source}/{event_type} event"
                    )
                    return True

            except Exception as e:
                logger.error(f"Error in LLM-enhanced keyword analysis: {str(e)}")
                # Fall back to traditional method if LLM analysis fails

        if found_keywords:
            logger.info(
                f"Found keywords in {source}/{event_type} event: {', '.join(found_keywords)}"
            )
            return True

        return False

    def _extract_text_content(
        self, payload: Dict[str, Any], source: str, event_type: str
    ) -> str:
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

    async def _analyze_with_llm(
        self, payload: Dict[str, Any], source: str, event_type: str
    ) -> Dict[str, Any]:
        """
        Use LLM to analyze if an event should be considered an incident.

        :param payload: Event payload
        :param source: Event source
        :param event_type: Event type
        :returns: Analysis result with decision and explanation
        """
        try:
            # Prepare the prompt
            prompt = f"""
            Analyze this event and determine if it should be considered a system incident that requires attention.
            
            Event Details:
            - Source: {source}
            - Event Type: {event_type}
            - Payload: {payload}
            
            Consider the following factors:
            1. Severity of the event
            2. Potential impact on system operations
            3. Whether immediate action is required
            4. Historical context (if similar events typically indicate problems)
            
            Return your analysis as a JSON object with the following structure:
            {{
                "is_incident": boolean,
                "confidence": float,  # 0.0 to 1.0
                "severity": string,  # "low", "medium", "high", or "critical"
                "reasoning": string,
                "recommended_actions": [string]
            }}
            """

            # Call LLM for analysis
            response = await self.llm_client.chat.completions.create(
                model=self.llm_model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert system incident analyzer. Your task is to determine if events should be classified as incidents requiring attention.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
                response_format={"type": "json_object"},
            )

            # Parse response
            analysis = response.choices[0].message.content
            return json.loads(analysis)

        except Exception as e:
            logger.error(f"Error in LLM analysis: {str(e)}")
            # Fall back to rule-based approach if LLM fails
            return {
                "is_incident": None,
                "confidence": 0.0,
                "severity": None,
                "reasoning": f"LLM analysis failed: {str(e)}",
                "recommended_actions": [],
            }

    async def check_event_conditions(
        self, payload: Dict[str, Any], source: str, event_type: str
    ) -> bool:
        """
        Check if event meets the conditions for creating an incident.
        Uses both LLM and rule-based approaches for robust detection.

        :param payload: Event payload
        :param source: Event source
        :param event_type: Event type
        :returns: True if conditions are met, False otherwise
        """
        # First, try LLM-based analysis
        llm_analysis = await self._analyze_with_llm(payload, source, event_type)

        # If LLM gives a high-confidence result, use it
        if llm_analysis["confidence"] >= 0.8:
            if llm_analysis["is_incident"]:
                logger.info(
                    f"LLM detected incident with confidence {llm_analysis['confidence']}: {llm_analysis['reasoning']}"
                )
                # Store the analysis for later use in incident creation
                self._last_analysis = llm_analysis
                return True
            return False

        # Fall back to rule-based approach if LLM is not confident
        logger.info("LLM confidence low, falling back to rule-based detection")

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
                payload_value = self._extract_value_from_payload(
                    payload, key, source, event_type
                )
                if payload_value != value:
                    return False

        return True

    def _extract_action_from_payload(
        self, payload: Dict[str, Any], source: str, event_type: str
    ) -> str:
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
                return payload["webhookEvent"].split("_")[
                    -1
                ]  # Extract 'created', 'updated', etc.

        return "unknown"

    def _extract_value_from_payload(
        self, payload: Dict[str, Any], key: str, source: str, event_type: str
    ) -> Any:
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

    async def create_incident_from_event(
        self,
        session: AsyncSession,
        source: str,
        event_type: str,
        payload: Dict[str, Any],
        event_id: int,
    ) -> Optional[Incident]:
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
            title, description = self._extract_title_description(
                source, event_type, payload
            )

            logger.info(f"Auto-creating incident from {source}/{event_type} event")

            # Auto-classify severity
            severity = await self.severity_classifier.classify_severity(
                incident_title=title,
                incident_description=description,
                source=source,
                events_data=[payload],
            )

            # Create new incident
            new_incident = Incident(
                title=title,
                description=description,
                severity=severity,
                status="open",  # New incidents default to open
                source=source,
                created_at=datetime.utcnow(),
            )

            logger.debug(
                f"[DB] Executing insert operation: incident[{source}/{severity}] '{title[:30]}...'"
            )
            session.add(new_incident)
            await session.commit()
            logger.debug(f"[DB] Incident commit successful")
            await session.refresh(new_incident)
            logger.debug(
                f"[DB] Incident object reloaded successfully: ID={new_incident.id}"
            )

            # Update event incident relation
            await self._update_event_incident_relation(
                session, event_id, new_incident.id
            )

            # Associate other related unlinked events
            await self._associate_related_events(
                session, new_incident, source, event_type
            )

            # Add incident to vector database for similarity search
            try:
                logger.debug(
                    f"[VECTOR_DB] Preparing to index incident #{new_incident.id} in vector database"
                )
                # Get Qdrant client
                qdrant_client = get_qdrant_client()

                # Initialize similarity service
                similarity_service = IncidentSimilarityService(
                    qdrant_client=qdrant_client
                )

                # Create embedding model
                incident_embedding = IncidentEmbedding(
                    incident_id=str(new_incident.id),
                    title=new_incident.title,
                    description=new_incident.description,
                    severity=new_incident.severity,
                    status=new_incident.status,
                    source=new_incident.source,
                    created_at=new_incident.created_at.isoformat(),
                )

                # Index in vector database
                logger.debug(
                    f"[VECTOR_DB] Starting vector embedding generation for incident #{new_incident.id}"
                )
                indexed = await similarity_service.index_incident(incident_embedding)
                if indexed:
                    logger.info(
                        f"Indexed incident {new_incident.id} in vector database"
                    )
                    logger.debug(
                        f"[VECTOR_DB] Incident #{new_incident.id} successfully added to vector database"
                    )
                else:
                    logger.error(
                        f"Failed to index incident {new_incident.id} in vector database"
                    )
            except Exception as e:
                logger.error(f"Error indexing incident in vector database: {str(e)}")

            logger.info(
                f"Auto-created incident: {new_incident.id} - {title} (severity: {severity})"
            )
            return new_incident

        except Exception as e:
            logger.error(f"Failed to create incident: {str(e)}")
            return None

    def _extract_title_description(
        self, source: str, event_type: str, payload: Dict[str, Any]
    ) -> Tuple[str, str]:
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
                description += (
                    f"\n\nReporter: {issue.get('user', {}).get('login', 'unknown')}"
                )

            elif event_type == "pull_request":
                pr = payload.get("pull_request", {})
                title = f"GitHub PR: {pr.get('title', 'No Title')}"
                description = pr.get("body", "No description") or "No description"
                description += f"\n\nURL: {pr.get('html_url', '')}"
                description += (
                    f"\n\nAuthor: {pr.get('user', {}).get('login', 'unknown')}"
                )
                description += f"\n\nStatus: {pr.get('state', 'unknown')}, Merged: {pr.get('merged', False)}"

        elif source == "jira":
            if "issue" in payload:
                issue = payload.get("issue", {})
                fields = issue.get("fields", {})
                title = f"Jira Issue: {fields.get('summary', 'No Title')}"
                description = (
                    fields.get("description", "No description") or "No description"
                )
                description += f"\n\nKey: {issue.get('key', '')}"

                if "priority" in fields:
                    description += (
                        f"\n\nPriority: {fields['priority'].get('name', 'unknown')}"
                    )

                if "reporter" in fields:
                    description += f"\n\nReporter: {fields['reporter'].get('displayName', 'unknown')}"
            else:
                # For other Jira webhook events
                title = f"Jira Event: {payload.get('webhookEvent', 'unknown')}"
                description = (
                    "Jira webhook event details:\n"
                    + json.dumps(payload, indent=2)[:1000]
                    + "..."
                )

        # Add timestamp to description
        description += f"\n\nDetected at: {datetime.utcnow().isoformat()}"

        return title, description

    async def _update_event_incident_relation(
        self, session: AsyncSession, event_id: int, incident_id: int
    ) -> None:
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

    async def _associate_related_events(
        self,
        session: AsyncSession,
        incident: Incident,
        source: str,
        event_type: str,
        max_age_hours: int = 24,
    ) -> None:
        """
        Associate related unlinked events with the new incident.

        :param session: Database session
        :param incident: The incident object
        :param source: Event source
        :param event_type: Event type
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
                    Event.incident_id.is_(None),  # Not associated with any incident
                )
            )
            .order_by(Event.created_at.desc())
        )

        result = await session.execute(query)
        events = result.scalars().all()

        if events:
            logger.debug(
                f"[DB] Associating {len(events)} related events to incident #{incident.id}"
            )

        # Link events to this incident
        for event in events:
            event.incident_id = incident.id
            logger.debug(
                f"[DB] Associating event ID={event.id} to incident #{incident.id}"
            )

        if events:
            await session.commit()
            logger.debug(f"[DB] Association update committed successfully")
            logger.info(
                f"Associated {len(events)} additional events with incident {incident.id}"
            )
