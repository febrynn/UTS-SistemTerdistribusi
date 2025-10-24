from pydantic import BaseModel, Field, validator
from typing import Any, Dict, List, Optional
from datetime import datetime

class Event(BaseModel):
    topic: str
    event_id: str
    timestamp: str
    source: str
    payload: Dict[str, Any]

    @validator("timestamp")
    def valid_iso(cls, v):
        # basic ISO8601 validation
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except Exception as e:
            raise ValueError("timestamp must be ISO8601") from e
        return v

class PublishRequest(BaseModel):
    events: Optional[List[Event]] = None
    # support single event body also via direct Event in endpoint
