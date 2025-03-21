"""Standard schema for ingestion events."""
import json
from datetime import datetime
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class StandardEventMessage(BaseModel):
    """Standard format for all ingested events."""

    source: str  # 'github' or 'jira'
    event_type: str  # The event type (e.g., 'push', 'pull_request', 'issue', etc.)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    raw_payload: Dict[str, Any]  # The original webhook payload
    
    def to_json(self) -> str:
        """Convert the event to a JSON string."""
        # Custom JSON serialization to handle datetime
        return json.dumps(
            {
                "source": self.source,
                "event_type": self.event_type,
                "timestamp": self.timestamp.isoformat(),
                "raw_payload": self.raw_payload,
            }
        ) 