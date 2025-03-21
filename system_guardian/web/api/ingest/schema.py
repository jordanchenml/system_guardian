from pydantic import BaseModel, Field
from typing import Dict, Any, Union

class Message(BaseModel):
    """Simple message model."""

    message: Union[str, Dict[str, Any], Any] = Field(description="Can be a simple string message or a GitHub webhook payload")
