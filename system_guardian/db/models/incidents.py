from sqlalchemy import Column, Integer, String, Text, DateTime, Enum, Boolean, ForeignKey, Float
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from datetime import datetime
from system_guardian.db.base import Base


class Incident(Base):
    """Incident model representing a system issue."""
    
    __tablename__ = "incidents"
    
    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    description = Column(Text)
    severity = Column(Enum("low", "medium", "high", "critical", name="severity_enum"))
    status = Column(Enum("open", "investigating", "resolving", "resolved", name="status_enum"))
    created_at = Column(DateTime, default=datetime.utcnow)
    resolved_at = Column(DateTime, nullable=True)
    source = Column(String)  # 'github', 'jira', 'slack', etc.
    
    # Relationships
    events = relationship("Event", back_populates="incident")
    resolutions = relationship("Resolution", back_populates="incident")
    
class Event(Base):
    """Events related to an incident from various sources."""
    
    __tablename__ = "events"
    
    id = Column(Integer, primary_key=True)
    incident_id = Column(Integer, ForeignKey("incidents.id"))
    source = Column(String)  # 'github', 'jira', 'slack', etc.
    event_type = Column(String)  # 'commit', 'issue', 'message', etc.
    content = Column(JSONB)  # Store the full event payload
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    incident = relationship("Incident", back_populates="events")
    
class Resolution(Base):
    """AI-generated or manual resolution for an incident."""
    
    __tablename__ = "resolutions"
    
    id = Column(Integer, primary_key=True)
    incident_id = Column(Integer, ForeignKey("incidents.id"))
    suggestion = Column(Text)
    confidence = Column(Float)  # AI confidence score
    is_applied = Column(Boolean, default=False)
    generated_at = Column(DateTime, default=datetime.utcnow)
    feedback_score = Column(Integer, nullable=True)  # User feedback
    
    # Relationships
    incident = relationship("Incident", back_populates="resolutions")