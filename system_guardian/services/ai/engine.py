from typing import List, Dict, Optional, Any, Union, Callable
import json
from datetime import datetime
import time
from functools import lru_cache
import sqlalchemy
from sqlalchemy.future import select
from sqlalchemy.ext.asyncio import AsyncSession

from loguru import logger

class AIEngine:
    """AI engine for generating resolution suggestions and insights."""
    
    def __init__(
        self, 
        vector_db_client, 
        llm_client, 
        embedding_model: str = "text-embedding-ada-002",
        llm_model: str = "gpt-3.5-turbo",
        cache_size: int = 100,
        enable_metrics: bool = True
    ):
        """
        Initialize the AI Engine.
        
        :param vector_db_client: Client for vector database operations
        :param llm_client: Client for LLM operations (OpenAI)
        :param embedding_model: Model to use for embeddings generation
        :param llm_model: Default LLM model to use for text generation
        :param cache_size: Size of the LRU cache for embeddings
        :param enable_metrics: Whether to track performance metrics
        """
        self.vector_db = vector_db_client
        self.llm = llm_client
        self.embedding_model = embedding_model
        self.llm_model = llm_model
        self.cache_size = cache_size
        self.enable_metrics = enable_metrics
        self.metrics = {
            "embedding_calls": 0,
            "embedding_errors": 0,
            "llm_calls": 0,
            "llm_errors": 0,
            "vector_search_calls": 0,
            "total_processing_time": 0
        }
        
        logger.debug(f"Initialized AIEngine with embedding model: {embedding_model}, LLM model: {llm_model}")
        
        # Configure the embedding cache
        self._configure_embedding_cache()
        
    def _configure_embedding_cache(self):
        """Configure the LRU cache for embeddings."""
        @lru_cache(maxsize=self.cache_size)
        def _cached_embedding(text: str) -> List[float]:
            # This is just a placeholder to make the cache work
            # The actual implementation will call the async method
            return []
            
        self._embedding_cache = _cached_embedding
        logger.debug(f"Configured embedding cache with size: {self.cache_size}")
        
    def _track_metric(self, metric_name: str, increment: int = 1):
        """Track a performance metric if metrics are enabled."""
        if self.enable_metrics:
            self.metrics[metric_name] = self.metrics.get(metric_name, 0) + increment
            
    def get_metrics(self) -> Dict[str, Any]:
        """Get the current performance metrics."""
        logger.debug(f"AIEngine metrics: {self.metrics}")
        return self.metrics
        
    async def generate_embedding(self, text: str) -> List[float]:
        """
        Generate vector embedding for text.
        
        :param text: Text to generate embedding for
        :return: Vector embedding as list of floats
        """
        # Check if we have it in cache (using a cache key)
        cache_key = text.strip()[:1000]  # Limit key size
        
        # Track call
        self._track_metric("embedding_calls")
        start_time = time.time()
        
        try:
            # Try to get from LRU cache
            # Since lru_cache doesn't work with async functions directly,
            # we use it as a lookup mechanism only
            cached_result = self._embedding_cache(cache_key)
            if cached_result:
                logger.debug("Embedding cache hit")
                return cached_result
                
            # Generate new embedding
            logger.debug(f"Generating embedding using model: {self.embedding_model}")
            response = await self.llm.embeddings.create(
                model=self.embedding_model,
                input=text,
            )
            embedding = response.data[0].embedding
            
            # Update cache by calling the function (not ideal but works for this case)
            # In a production system, a proper async cache would be better
            self._embedding_cache.cache_clear()  # Clear to avoid growing too much
            self._embedding_cache(cache_key)
            self._embedding_cache.__wrapped__.__dict__[cache_key] = embedding
            
            return embedding
        except Exception as e:
            # Log and track the error
            self._track_metric("embedding_errors")
            logger.error(f"Error generating embedding: {str(e)}")
            # Fallback to zero vector
            return [0.0] * 1536  # Default embedding size
        finally:
            # Track processing time
            processing_time = time.time() - start_time
            self._track_metric("total_processing_time", processing_time)
        
    async def find_similar_incidents(
        self, 
        incident_text: str, 
        limit: int = 5,
        filter_condition: Optional[Dict] = None,
        min_similarity_score: float = 0.5
    ) -> List[Dict]:
        """
        Find similar past incidents using vector similarity.
        
        :param incident_text: Text to search for similar incidents
        :param limit: Maximum number of results to return
        :param filter_condition: Optional filter condition for the query
        :param min_similarity_score: Minimum similarity score for returned incidents
        :return: List of similar incidents with similarity scores
        """
        start_time = time.time()
        self._track_metric("vector_search_calls")
        
        try:
            # Generate embedding for the query text
            embedding = await self.generate_embedding(incident_text)
            
            # Search the vector database
            logger.debug(f"Searching vector database with limit: {limit*2}, filter: {filter_condition}")
            results = await self.vector_db.search(
                embedding, 
                limit=limit * 2,  # Request more than needed to account for filtering
                filter_condition=filter_condition
            )
            
            # Filter by similarity score
            filtered_results = [
                incident for incident in results 
                if incident.get("similarity_score", 0) >= min_similarity_score
            ]
            
            logger.info(f"Found {len(filtered_results)} similar incidents with similarity score >= {min_similarity_score}")
            # Return up to the requested limit
            return filtered_results[:limit]
        except Exception as e:
            logger.error(f"Error finding similar incidents: {str(e)}", exc_info=True)
            return []
        finally:
            processing_time = time.time() - start_time
            self._track_metric("total_processing_time", processing_time)
    
    # Helper method to get incident details
    async def _get_incident_details(self, db_session, incident_id: int) -> Dict:
        """
        Get incident details from the database.
        
        :param db_session: Database session
        :param incident_id: ID of the incident
        :return: Dictionary with incident details or None if not found
        """
        from system_guardian.db.models.incidents import Incident, Event, Resolution
        
        try:
            # Query the incident
            incident_query = select(Incident).where(Incident.id == incident_id)
            result = await db_session.execute(incident_query)
            incident = result.scalars().first()
            
            if not incident:
                logger.warning(f"Incident with ID {incident_id} not found")
                return None
            
            # Get related events
            events_query = select(Event).where(Event.incident_id == incident_id)
            events_result = await db_session.execute(events_query)
            related_events = events_result.scalars().all()
            logger.debug(f"Found {len(related_events)} events for incident {incident_id}")
            
            # Get resolution if any
            resolution_query = select(Resolution).where(Resolution.incident_id == incident_id)
            resolution_result = await db_session.execute(resolution_query)
            resolution = resolution_result.scalars().first()
            
            # Format incident data
            incident_data = {
                "id": incident.id,
                "title": incident.title,
                "description": incident.description,
                "severity": incident.severity,
                "status": incident.status,
                "source": incident.source,
                "created_at": incident.created_at.isoformat(),
                "resolved_at": incident.resolved_at.isoformat() if incident.resolved_at else None,
                "resolution": resolution.suggestion if resolution else None,
                "similarity_score": 1.0,  # Perfect match with itself
                "events": [
                    {
                        "id": event.id,
                        "source": event.source,
                        "event_type": event.event_type,
                        "content": event.content,
                        "created_at": event.created_at.isoformat()
                    } for event in related_events
                ]
            }
            
            return incident_data
        except Exception as e:
            logger.error(f"Error getting incident details: {str(e)}", exc_info=True)
            return None
    
    # Helper method to generate query text from incident
    def _generate_query_text_from_incident(self, incident_data: Dict) -> str:
        """
        Generate query text from incident data for similarity search.
        
        :param incident_data: Dictionary with incident details
        :return: Query text for similarity search
        """
        if not incident_data:
            return ""
            
        query_text = f"Incident: {incident_data['title']}\n"
        query_text += f"Description: {incident_data['description']}\n"
        query_text += f"Severity: {incident_data['severity']}\n"
        query_text += f"Source: {incident_data['source']}\n"
        
        # Add event information
        events = incident_data.get("events", [])
        for i, event in enumerate(events[:3]):  # Limit to first 3 events
            query_text += f"\nEvent {i+1}: {event['source']}/{event['event_type']}\n"
            
            # Extract relevant content fields
            if isinstance(event.get('content'), dict):
                content = event['content']
                for key in ['title', 'description', 'message', 'text']:
                    if key in content:
                        value = content[key]
                        if isinstance(value, str):
                            query_text += f"{key}: {value[:100]}...\n"
                        elif value is not None:
                            query_text += f"{key}: {json.dumps(value)[:100]}...\n"
        
        logger.debug(f"Generated query text from incident: {len(query_text)} characters")
        return query_text
        
    async def find_related_incidents(
        self,
        db_session,
        incident_id: Optional[int] = None,
        query_text: Optional[str] = None,
        limit: int = 5,
        include_resolved: bool = True,
        min_similarity_score: float = 0.5
    ) -> Dict:
        """
        Find similar past incidents and provide insights based on them.
        
        :param db_session: Database session
        :param incident_id: Optional ID of the incident to find related incidents for
        :param query_text: Optional text to search for related incidents
        :param limit: Maximum number of related incidents to return
        :param include_resolved: Whether to include resolved incidents
        :param min_similarity_score: Minimum similarity score for related incidents
        :return: Dict with related incidents, insights, and current incident
        """
        from system_guardian.services.ai.incident_similarity import IncidentSimilarityService
        
        start_time = time.time()
        logger.info(f"Finding related incidents for incident_id={incident_id}, query_text_provided={bool(query_text)}")
        
        try:
            # Initialize similarity service
            similarity_service = IncidentSimilarityService(
                qdrant_client=self.vector_db,
                openai_client=self.llm,
            )
            
            incident_data = None
            final_query_text = query_text
            
            # If incident ID is provided, get incident details
            if incident_id:
                incident_data = await self._get_incident_details(db_session, incident_id)
                
                if not incident_data:
                    raise ValueError(f"Incident with ID {incident_id} not found")
                
                # Create query text if not provided
                if not final_query_text:
                    final_query_text = self._generate_query_text_from_incident(incident_data)
            
            if not final_query_text:
                raise ValueError("Either incident_id or query_text must be provided")
            
            # Define filter condition based on parameters
            filter_condition = {}
            if not include_resolved:
                filter_condition = {
                    "must": [
                        {
                            "key": "status",
                            "match": {
                                "any": ["open", "investigating"]
                            }
                        }
                    ]
                }
                logger.debug("Applied filter to exclude resolved incidents")
            
            # Find similar incidents
            logger.debug(f"Searching for similar incidents with limit={limit}")
            similar_incidents_data = await similarity_service.find_similar_incidents(
                query_text=final_query_text,
                limit=limit,
                filter_condition=filter_condition,
            )
            
            # Filter by minimum similarity score
            similar_incidents_data = [
                incident for incident in similar_incidents_data 
                if incident.get("similarity_score", 0) >= min_similarity_score
            ]
            logger.info(f"Found {len(similar_incidents_data)} similar incidents with score >= {min_similarity_score}")
            
            # Convert to standardized format
            related_incidents = self._standardize_incident_results(
                similar_incidents_data, 
                current_incident_id=incident_data["id"] if incident_data else None
            )
            
            # Generate insights based on related incidents
            logger.debug("Generating insights based on related incidents")
            insights = await self.generate_insights(
                current_incident=incident_data,
                related_incidents=related_incidents
            )
            
            return {
                "incidents": related_incidents,
                "insights": insights,
                "current_incident": incident_data
            }
        except Exception as e:
            logger.error(f"Error finding related incidents: {str(e)}", exc_info=True)
            raise
        finally:
            processing_time = time.time() - start_time
            logger.debug(f"find_related_incidents completed in {processing_time:.2f}s")
            self._track_metric("total_processing_time", processing_time)
    
    def _standardize_incident_results(
        self, 
        incidents: List[Dict], 
        current_incident_id: Optional[int] = None
    ) -> List[Dict]:
        """
        Standardize incident results from the vector database.
        
        :param incidents: List of incidents from the vector database
        :param current_incident_id: ID of the current incident to exclude
        :return: List of standardized incident dictionaries
        """
        standardized_incidents = []
        
        for incident in incidents:
            # Skip the current incident if it's in the results
            if current_incident_id and str(current_incident_id) == str(incident.get("incident_id")):
                continue
                
            standardized_incidents.append({
                "id": int(incident.get("incident_id")) if incident.get("incident_id") else 0,
                "title": incident.get("title", ""),
                "description": incident.get("description", ""),
                "severity": incident.get("severity", "medium"),
                "status": incident.get("status", "unknown"),
                "source": incident.get("source", "unknown"),
                "created_at": incident.get("created_at", datetime.utcnow().isoformat()),
                "resolved_at": incident.get("resolved_at"),
                "resolution": incident.get("resolution"),
                "similarity_score": incident.get("similarity_score", 0)
            })
            
        logger.debug(f"Standardized {len(standardized_incidents)} incident results")
        return standardized_incidents
        
    async def generate_insights(
        self,
        current_incident: Optional[Dict],
        related_incidents: List[Dict]
    ) -> List[Dict]:
        """
        Generate insights based on the current incident and related incidents.
        
        :param current_incident: The current incident data if available
        :param related_incidents: List of related incidents
        :returns: List of insights derived from the incidents
        """
        if not related_incidents:
            logger.info("No related incidents provided, skipping insights generation")
            return []
        
        start_time = time.time()
        self._track_metric("llm_calls")
        logger.info(f"Generating insights based on {len(related_incidents)} related incidents")
        
        try:
            # Prepare incident data for the prompt
            current_incident_text = self._format_incident_for_prompt(current_incident, is_current=True)
            related_incidents_text = self._format_related_incidents_for_prompt(related_incidents)
            
            # Create prompt for the LLM
            prompt = f"""
            {current_incident_text}
            
            {related_incidents_text}
            
            Based on the information above, generate 3-5 key insights about these incidents. 
            Each insight should be in the following format:
            - type: the type of insight (pattern, frequency, severity, resolution, etc.)
            - description: detailed explanation of the insight
            - confidence: a number between 0 and 1 indicating how confident you are about this insight
            
            Example insight:
            {{
                "type": "pattern",
                "description": "80% of similar incidents originate from the authentication service, suggesting a systemic issue in that component.",
                "confidence": 0.85
            }}
            
            Return the insights as a JSON array with a key called "insights".
            """
            
            # Generate insights using LLM
            logger.debug(f"Calling LLM with model {self.llm_model} to generate insights")
            response = await self.llm.chat.completions.create(
                model=self.llm_model,
                messages=[
                    {"role": "system", "content": "You are an expert at analyzing IT incidents and identifying patterns and insights. Provide your response as a valid JSON object with an 'insights' array."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                response_format={"type": "json_object"}
            )
            
            # Parse insights from response
            insights_text = response.choices[0].message.content
            insights_data = json.loads(insights_text)
            
            # Convert to standardized format
            insights = []
            for insight in insights_data.get("insights", []):
                insights.append({
                    "type": insight.get("type", "unknown"),
                    "description": insight.get("description", ""),
                    "confidence": float(insight.get("confidence", 0.5))
                })
            
            logger.info(f"Generated {len(insights)} insights")
            return insights
        except Exception as e:
            # In case of error, log and return a generic insight
            self._track_metric("llm_errors")
            logger.exception(f"Error generating insights: {str(e)}")
            return [{
                "type": "error",
                "description": f"Failed to generate insights: {str(e)}",
                "confidence": 0.1
            }]
        finally:
            processing_time = time.time() - start_time
            logger.debug(f"Insights generation completed in {processing_time:.2f}s")
            self._track_metric("total_processing_time", processing_time)
    
    def _format_incident_for_prompt(self, incident: Optional[Dict], is_current: bool = False) -> str:
        """
        Format incident data for inclusion in an LLM prompt.
        
        :param incident: Incident data dictionary
        :param is_current: Whether this is the current incident
        :return: Formatted text for the prompt
        """
        if not incident:
            return ""
            
        header = "Current Incident:" if is_current else "Incident:"
        
        formatted_text = f"""
        {header}
        ID: {incident["id"]}
        Title: {incident["title"]}
        Description: {incident["description"] or 'N/A'}
        Severity: {incident["severity"]}
        Status: {incident["status"]}
        Source: {incident["source"]}
        Created: {incident["created_at"]}
        """
        
        if not is_current and incident.get("resolved_at"):
            formatted_text += f"Resolved: {incident['resolved_at']}\n"
            
        if incident.get("resolution"):
            formatted_text += f"Resolution: {incident['resolution']}\n"
            
        if not is_current and incident.get("similarity_score") is not None:
            formatted_text += f"Similarity Score: {incident['similarity_score']:.2f}\n"
            
        return formatted_text
        
    def _format_related_incidents_for_prompt(self, incidents: List[Dict]) -> str:
        """
        Format a list of related incidents for inclusion in an LLM prompt.
        
        :param incidents: List of incident dictionaries
        :return: Formatted text for the prompt
        """
        if not incidents:
            return "No related incidents found."
            
        formatted_text = "Related Incidents:\n"
        
        for i, incident in enumerate(incidents):
            formatted_text += f"""
            Incident {i+1}:
            ID: {incident["id"]}
            Title: {incident["title"]}
            Description: {incident["description"] or 'N/A'}
            Severity: {incident["severity"]}
            Status: {incident["status"]}
            Source: {incident["source"]}
            Created: {incident["created_at"]}
            Resolved: {incident["resolved_at"] or 'Not resolved'}
            Resolution: {incident["resolution"] or 'No resolution provided'}
            Similarity Score: {incident["similarity_score"]:.2f}
            """
            
        return formatted_text
            
    async def generate_resolution(
        self, 
        incident_id: int,
        session: AsyncSession,
        model: Optional[str] = None,
        temperature: float = 0.3,
        store_result: bool = True
    ) -> Dict:
        """
        Generate resolution suggestion for an incident.
        
        :param incident_id: The ID of the incident to generate a resolution for
        :param session: Database session to use for querying
        :param model: Optional model override for the LLM
        :param temperature: Temperature for the LLM generation (0.0-1.0)
        :param store_result: Whether to store the resolution in the database
        :return: Dictionary with resolution text, confidence, and metadata
        """
        from system_guardian.db.models.incidents import Incident, Event, Resolution
        
        start_time = time.time()
        self._track_metric("llm_calls")
        logger.info(f"Generating resolution for incident ID: {incident_id}")
        
        try:
            # Get incident details using the provided session
            incident_query = select(Incident).where(Incident.id == incident_id)
            result = await session.execute(incident_query)
            incident = result.scalars().first()
            
            if not incident:
                logger.warning(f"Incident with ID {incident_id} not found")
                raise ValueError(f"Incident with ID {incident_id} not found")
            
            # Get related events for this incident
            events_query = select(Event).where(Event.incident_id == incident_id)
            events_result = await session.execute(events_query)
            related_events = events_result.scalars().all()
            logger.debug(f"Found {len(related_events)} events for incident {incident_id}")
            
            # Format incident and events information
            incident_text = self._format_incident_for_resolution_prompt(incident, related_events)
            
            # Find similar past resolved incidents
            logger.debug("Searching for similar resolved incidents")
            similar_incidents = await self.find_similar_incidents(
                incident_text, 
                limit=3,
                filter_condition={"must": [{"key": "status", "match": {"any": ["resolved"]}}]},
                min_similarity_score=0.6
            )
            
            # Format similar incidents information
            similar_incidents_text = self._format_similar_incidents_for_resolution_prompt(similar_incidents)
            
            # Generate resolution suggestion using LLM
            prompt = self._create_resolution_prompt(incident_text, similar_incidents_text)
            
            # Use the specified model or fall back to default
            model_to_use = model or self.llm_model
            
            # Make sure we have a response format
            logger.debug(f"Calling LLM with model {model_to_use} to generate resolution")
            response = await self.llm.chat.completions.create(
                model=model_to_use,
                messages=[
                    {"role": "system", "content": "You are an expert IT incident resolver. Provide concise, actionable resolution steps."},
                    {"role": "user", "content": prompt}
                ],
                temperature=temperature,
                max_tokens=800
            )
            
            resolution_text = response.choices[0].message.content
            
            # Calculate a confidence score based on various factors
            confidence_score = self._calculate_resolution_confidence(
                similar_incidents=similar_incidents,
                incident=incident,
                model=model_to_use
            )
            logger.debug(f"Generated resolution with confidence score: {confidence_score:.2f}")
            
            # Store the generated resolution in the database if requested
            if store_result:
                resolution = Resolution(
                    incident_id=incident_id,
                    suggestion=resolution_text,
                    confidence=confidence_score,
                    is_applied=False,
                    generated_at=datetime.utcnow()
                )
                
                session.add(resolution)
                await session.commit()
                logger.info(f"Stored resolution in database for incident ID: {incident_id}")
            
            # Return comprehensive result
            return {
                "resolution_text": resolution_text,
                "confidence": confidence_score,
                "incident_id": incident_id,
                "generated_at": datetime.utcnow().isoformat(),
                "model_used": model_to_use,
                "similar_incidents_count": len(similar_incidents)
            }
        except Exception as e:
            self._track_metric("llm_errors")
            logger.exception(f"Error generating resolution: {str(e)}")
            raise
        finally:
            processing_time = time.time() - start_time
            logger.debug(f"Resolution generation completed in {processing_time:.2f}s")
            self._track_metric("total_processing_time", processing_time)
    
    def _format_incident_for_resolution_prompt(self, incident, events) -> str:
        """
        Format incident and events data for the resolution prompt.
        
        :param incident: Incident database model
        :param events: List of event database models
        :return: Formatted text for the incident
        """
        incident_text = f"Incident: {incident.title}\n"
        incident_text += f"Description: {incident.description}\n"
        incident_text += f"Severity: {incident.severity}\n"
        incident_text += f"Source: {incident.source}\n"
        incident_text += f"Status: {incident.status}\n"
        incident_text += f"Created at: {incident.created_at}\n\n"
        
        if events:
            incident_text += "Related Events:\n"
            for i, event in enumerate(events):
                incident_text += f"Event {i+1}: {event.source}/{event.event_type}\n"
                # Extract and format content more intelligently
                if isinstance(event.content, dict):
                    important_fields = ['error', 'message', 'reason', 'status', 'title', 'description']
                    content_text = ""
                    
                    # Extract important fields first
                    for field in important_fields:
                        if field in event.content:
                            value = event.content[field]
                            if value:
                                content_text += f"  {field}: {value}\n"
                    
                    # Add a sample of other fields
                    other_fields = [k for k in event.content.keys() if k not in important_fields][:3]
                    for field in other_fields:
                        value = event.content[field]
                        if isinstance(value, (str, int, float, bool)):
                            content_text += f"  {field}: {value}\n"
                    
                    incident_text += content_text
                else:
                    incident_text += f"Content: {str(event.content)[:300]}...(truncated)\n\n"
        
        return incident_text
    
    def _format_similar_incidents_for_resolution_prompt(self, incidents) -> str:
        """
        Format similar incidents for the resolution prompt.
        
        :param incidents: List of similar incidents
        :return: Formatted text for similar incidents
        """
        similar_incidents_text = ""
        for i, similar in enumerate(incidents):
            similar_incidents_text += f"Similar incident {i+1}: {similar.get('title')}\n"
            similar_incidents_text += f"Description: {similar.get('description')}\n"
            if similar.get('resolution'):
                similar_incidents_text += f"Resolution: {similar.get('resolution')}\n"
            similar_incidents_text += f"Similarity score: {similar.get('similarity_score', 0):.2f}\n\n"
        
        return similar_incidents_text
    
    def _create_resolution_prompt(self, incident_text, similar_incidents_text) -> str:
        """
        Create a prompt for resolution generation.
        
        :param incident_text: Formatted text for the current incident
        :param similar_incidents_text: Formatted text for similar incidents
        :return: Complete prompt for the LLM
        """
        return f"""
        Based on the following incident details and similar past incidents, generate a comprehensive resolution suggestion.
        
        CURRENT INCIDENT:
        {incident_text}
        
        SIMILAR PAST INCIDENTS:
        {similar_incidents_text or "No similar resolved incidents found."}
        
        Your task:
        1. Analyze the current incident details
        2. Consider solutions from similar past incidents
        3. Provide a step-by-step resolution plan
        4. Include any diagnostic steps needed
        5. Suggest preventive measures to avoid similar incidents in the future
        
        Resolution suggestion:
        """
    
    def _calculate_resolution_confidence(
        self,
        similar_incidents: List[Dict],
        incident,
        model: str
    ) -> float:
        """
        Calculate a confidence score for the generated resolution.
        
        :param similar_incidents: List of similar incidents
        :param incident: Current incident
        :param model: LLM model used
        :return: Confidence score between 0 and 1
        """
        # Base confidence - different for different models
        base_confidence = 0.7 if "gpt-4" in model else 0.6
        
        # Adjust based on the number and quality of similar incidents
        similar_incidents_factor = 0.0
        if similar_incidents:
            # Average similarity score of top incidents
            avg_similarity = sum(s.get("similarity_score", 0) for s in similar_incidents) / len(similar_incidents)
            similar_incidents_factor = min(0.2, avg_similarity * 0.25)
            
            # Bonus if there are resolutions in similar incidents
            has_resolutions = any(s.get("resolution") for s in similar_incidents)
            if has_resolutions:
                similar_incidents_factor += 0.05
        
        # Adjust based on incident severity - higher severity may be more complex
        severity_factor = 0.0
        if hasattr(incident, "severity"):
            if incident.severity == "critical":
                severity_factor = -0.05
            elif incident.severity == "low":
                severity_factor = 0.05
                
        # Combine factors, ensuring a reasonable range
        confidence = base_confidence + similar_incidents_factor + severity_factor
        return max(0.2, min(0.95, confidence))  # Clamp between 0.2 and 0.95
