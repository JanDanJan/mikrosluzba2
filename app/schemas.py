from pydantic import BaseModel
from typing import Optional

class CalcRequest(BaseModel):
    event_id: str
    entity_type: str
    entity_id: str
    requested_at: Optional[str] = None
    trace_id: Optional[str] = None

class CalcCompleted(BaseModel):
    event_id: str
    correlation_id: str
    entity_type: str
    entity_id: str
    result_id: str
    status: str
    duration_ms: int
    completed_at: str
