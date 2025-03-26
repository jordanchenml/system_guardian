"""
Resolution Generator Module

This module provides functionality to generate and suggest resolutions for incidents.
It uses AI to analyze incident data, related events, and similar past incidents to
generate actionable resolution steps.
"""

from typing import Dict, List, Optional, Any, Tuple
import json
import time
from datetime import datetime
from loguru import logger
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from system_guardian.db.models.incidents import Incident, Event, Resolution

class ResolutionGenerator:
    """
    Generator for incident resolutions based on AI analysis of incident data,
    related events, and similar historical incidents.
    """
    
    def __init__(self, ai_engine):
        """
        Initialize the resolution generator.
        
        :param ai_engine: AI Engine instance for generating embeddings and text
        """
        self.ai_engine = ai_engine
        logger.info("Resolution Generator initialized")
    
    async def generate_resolution(
        self,
        incident_id: int,
        session: AsyncSession,
        force_regenerate: bool = False,
        model: Optional[str] = None,
        temperature: float = 0.3,
    ) -> Optional[Dict[str, Any]]:
        """
        Generate a resolution for an incident.
        
        :param incident_id: ID of the incident
        :param session: Database session
        :param force_regenerate: If True, generate a new resolution even if one exists
        :param model: Optional model override to use for resolution generation
        :param temperature: Temperature for LLM generation
        :return: Resolution data or None if generation failed
        """
        try:
            # Check if resolution already exists and not forcing regeneration
            if not force_regenerate:
                existing_resolution = await self._get_existing_resolution(incident_id, session)
                if existing_resolution:
                    logger.info(f"Using existing resolution for incident #{incident_id}")
                    return existing_resolution
            
            # Get incident details
            incident_data = await self.ai_engine._get_incident_details(session, incident_id)
            if not incident_data:
                logger.error(f"Failed to get incident details for #{incident_id}")
                return None
            
            # Find similar incidents
            query_text = await self.ai_engine._generate_query_text_from_incident(incident_data)
            similar_incidents = await self.ai_engine.find_similar_incidents(
                incident_text=query_text,
                limit=5,
                min_similarity_score=0.6
            )
            
            # Generate resolution
            resolution_text, confidence = await self._create_resolution(
                incident_data, 
                similar_incidents,
                model=model,
                temperature=temperature
            )
            
            # Store in database
            resolution_data = await self._store_resolution(
                incident_id, 
                resolution_text, 
                confidence, 
                session
            )
            
            return resolution_data
        except Exception as e:
            logger.error(f"Error generating resolution: {str(e)}")
            return None
    
    async def _get_existing_resolution(self, incident_id: int, session: AsyncSession) -> Optional[Dict]:
        """
        Get existing resolution for an incident.
        
        :param incident_id: Incident ID
        :param session: Database session
        :return: Resolution data or None if not found
        """
        try:
            # Query for existing resolution
            stmt = select(Resolution).where(Resolution.incident_id == incident_id)
            result = await session.execute(stmt)
            resolution = result.scalar_one_or_none()
            
            if resolution:
                return {
                    "id": resolution.id,
                    "incident_id": resolution.incident_id,
                    "suggestion": resolution.suggestion,
                    "confidence": resolution.confidence,
                    "is_applied": resolution.is_applied,
                    "generated_at": resolution.generated_at,
                    "feedback_score": resolution.feedback_score
                }
            return None
        except Exception as e:
            logger.error(f"Error getting existing resolution: {str(e)}")
            return None
    
    async def _create_resolution(
        self,
        incident_data: Dict,
        similar_incidents: List[Dict],
        model: Optional[str] = None,
        temperature: float = 0.3
    ) -> Tuple[str, float]:
        """
        Create resolution content using AI and calculate confidence.
        
        :param incident_data: Incident data
        :param similar_incidents: Similar incidents data
        :param model: Model to use for generation
        :param temperature: Temperature for generation
        :return: Tuple of (resolution_text, confidence)
        """
        # Format inputs for the LLM
        incident_text = self._format_incident_for_prompt(incident_data)
        similar_incidents_text = self._format_similar_incidents_for_prompt(similar_incidents)
        
        # Create the prompt
        prompt = self._create_resolution_prompt(incident_text, similar_incidents_text)
        
        # Use the AI engine to generate the resolution
        start_time = time.time()
        try:
            # Use the specified model or default to the engine's default
            model_to_use = model or self.ai_engine.llm_model
            
            logger.debug(f"Generating resolution using model: {model_to_use}")
            response = await self.ai_engine.llm.chat.completions.create(
                model=model_to_use,
                temperature=temperature,
                messages=[
                    {"role": "system", "content": "You are an expert IT operations and SRE specialist. Your task is to analyze incident data and provide clear, actionable steps to resolve the incident."},
                    {"role": "user", "content": prompt}
                ]
            )
            
            # Extract the response
            resolution_text = response.choices[0].message.content.strip()
            
            # Calculate confidence based on similar incidents and model
            confidence = self._calculate_confidence(similar_incidents, incident_data, model_to_use)
            
            logger.info(f"Generated resolution with confidence: {confidence:.2f}")
            return resolution_text, confidence
        except Exception as e:
            logger.error(f"Error generating resolution text: {str(e)}")
            return "Failed to generate resolution due to an error.", 0.0
        finally:
            logger.debug(f"Resolution generation took {time.time() - start_time:.2f}s")
    
    def _format_incident_for_prompt(self, incident: Dict) -> str:
        """
        Format incident data for inclusion in a prompt.
        
        :param incident: Incident data
        :return: Formatted text
        """
        # Build a clear text representation of the incident
        incident_text = f"INCIDENT #{incident['id']}: {incident['title']}\n"
        incident_text += f"Severity: {incident['severity']}\n"
        incident_text += f"Status: {incident['status']}\n"
        incident_text += f"Created: {incident['created_at']}\n"
        incident_text += f"Source: {incident['source']}\n\n"
        incident_text += f"Description: {incident['description']}\n\n"
        
        # Add related events
        incident_text += "RELATED EVENTS:\n"
        for idx, event in enumerate(incident.get("events", []), 1):
            incident_text += f"{idx}. [{event['source']}] {event['event_type']} - {event.get('summary', '')}\n"
            
            # Include relevant content from the event payload for context
            if 'content' in event and event['content']:
                content = event['content']
                if isinstance(content, str):
                    try:
                        content = json.loads(content)
                    except:
                        pass
                
                # Extract useful fields based on event source
                if event['source'] == 'datadog':
                    if isinstance(content, dict):
                        if 'alert_message' in content:
                            incident_text += f"   Alert Message: {content['alert_message']}\n"
                        if 'alert_threshold' in content:
                            incident_text += f"   Threshold: {content['alert_threshold']}\n"
                        if 'metric_value' in content:
                            incident_text += f"   Value: {content['metric_value']}\n"
                
                elif event['source'] == 'github':
                    if isinstance(content, dict):
                        if 'message' in content:
                            incident_text += f"   Message: {content['message']}\n"
                        if 'commit_id' in content:
                            incident_text += f"   Commit: {content['commit_id']}\n"
                
                elif event['source'] == 'jira':
                    if isinstance(content, dict):
                        if 'summary' in content:
                            incident_text += f"   Summary: {content['summary']}\n"
                        if 'priority' in content:
                            incident_text += f"   Priority: {content['priority']}\n"
        
        return incident_text
    
    def _format_similar_incidents_for_prompt(self, incidents: List[Dict]) -> str:
        """
        Format similar incidents for inclusion in a prompt.
        
        :param incidents: List of similar incidents
        :return: Formatted text
        """
        if not incidents:
            return "No similar incidents found."
        
        similar_text = "SIMILAR INCIDENTS:\n"
        for idx, incident in enumerate(incidents, 1):
            similar_text += f"{idx}. INCIDENT #{incident.get('incident_id', 'unknown')}: {incident.get('title', 'Untitled')}\n"
            similar_text += f"   Similarity: {incident.get('similarity_score', 0):.2f}\n"
            similar_text += f"   Status: {incident.get('status', 'unknown')}\n"
            
            # Add resolution if available
            if 'resolution' in incident and incident['resolution']:
                similar_text += f"   Resolution: {incident['resolution']}\n"
            
            # Add key events if available
            if 'key_events' in incident and incident['key_events']:
                similar_text += f"   Key Events: {incident['key_events']}\n"
            
            similar_text += "\n"
        
        return similar_text
    
    def _create_resolution_prompt(self, incident_text: str, similar_incidents_text: str) -> str:
        """
        Create a prompt for generating resolutions.
        
        :param incident_text: Formatted incident text
        :param similar_incidents_text: Formatted similar incidents text
        :return: Complete prompt text
        """
        prompt = "Your task is to analyze the following incident and provide a detailed resolution plan.\n\n"
        prompt += "Please structure your response in the following format:\n"
        prompt += "1. ROOT CAUSE ANALYSIS: Analyze the likely cause of the incident.\n"
        prompt += "2. RESOLUTION STEPS: Provide clear, actionable steps to resolve the incident.\n"
        prompt += "3. VERIFICATION: Suggest how to verify the incident is truly resolved.\n"
        prompt += "4. PREVENTION: Recommend steps to prevent similar incidents in the future.\n\n"
        
        prompt += "CURRENT INCIDENT DETAILS:\n"
        prompt += incident_text + "\n\n"
        
        prompt += "SIMILAR PAST INCIDENTS AND THEIR RESOLUTIONS:\n"
        prompt += similar_incidents_text + "\n\n"
        
        prompt += "Please provide a comprehensive resolution based on the incident details and any relevant information from similar past incidents."
        
        return prompt
    
    def _calculate_confidence(
        self,
        similar_incidents: List[Dict],
        incident_data: Dict,
        model: str
    ) -> float:
        """
        Calculate confidence score for the resolution.
        
        :param similar_incidents: Similar incidents
        :param incident_data: Current incident data
        :param model: Model used for generation
        :return: Confidence score (0.0 to 1.0)
        """
        base_confidence = 0.65  # Base confidence score
        
        # Factor 1: Model capability (better models get higher confidence)
        model_factor = 0.15 if "gpt-4" in model else 0.1
        
        # Factor 2: Similar incidents quality
        similar_incidents_factor = 0.0
        if similar_incidents:
            # Calculate average similarity score
            similarity_scores = [inc.get('similarity_score', 0) for inc in similar_incidents]
            avg_similarity = sum(similarity_scores) / len(similarity_scores) if similarity_scores else 0
            similar_incidents_factor = avg_similarity * 0.25
        
        # Factor 3: Data richness (more events = more context = higher confidence)
        events_count = len(incident_data.get('events', []))
        data_richness_factor = min(0.1, events_count * 0.02)  # Cap at 0.1
        
        # Calculate final confidence
        confidence = base_confidence + model_factor + similar_incidents_factor + data_richness_factor
        
        # Ensure confidence is between 0 and 1
        return max(0.0, min(1.0, confidence))
    
    async def _store_resolution(
        self,
        incident_id: int,
        suggestion: str,
        confidence: float,
        session: AsyncSession
    ) -> Dict:
        """
        Store the generated resolution in the database.
        
        :param incident_id: Incident ID
        :param suggestion: Resolution suggestion text
        :param confidence: Confidence score
        :param session: Database session
        :return: Resolution data dictionary
        """
        try:
            # Create resolution object
            resolution = Resolution(
                incident_id=incident_id,
                suggestion=suggestion,
                confidence=confidence,
                generated_at=datetime.utcnow(),
                is_applied=False,
                feedback_score=None
            )
            
            # Save to database
            session.add(resolution)
            await session.commit()
            await session.refresh(resolution)
            
            logger.info(f"Stored resolution #{resolution.id} for incident #{incident_id}")
            
            return {
                "id": resolution.id,
                "incident_id": resolution.incident_id,
                "suggestion": resolution.suggestion,
                "confidence": resolution.confidence,
                "is_applied": resolution.is_applied,
                "generated_at": resolution.generated_at,
                "feedback_score": resolution.feedback_score
            }
        except Exception as e:
            logger.error(f"Error storing resolution: {str(e)}")
            await session.rollback()
            raise 