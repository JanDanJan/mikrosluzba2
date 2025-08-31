from pydantic import BaseModel
from typing import Optional

class CalcRequest(BaseModel):
    """Inbound calculation request parsed from a Kafka message.

    Attributes:
        event_id: Unique identifier of the request event.
        entity_type: Target table (entity type) to look up.
        entity_id: Primary key of the entity to process.
        requested_at: Optional ISO8601 timestamp indicating when the request was created.
        trace_id: Optional correlation/trace identifier for observability.
    """
    event_id: str
    entity_type: str
    entity_id: str
    requested_at: Optional[str] = None
    trace_id: Optional[str] = None

class CalcCompleted(BaseModel):
    """Outbox completion event emitted after processing a request.

    Attributes:
        event_id: Identifier of this completion event.
        correlation_id: Original request's event_id for correlation.
        entity_type: Processed entity type.
        entity_id: Processed entity id.
        result_id: Primary key of the persisted `CalcResult`.
        status: Outcome status, e.g. 'SUCCESS' or 'FAILED'.
        duration_ms: Time spent processing the request in milliseconds.
        completed_at: ISO8601 timestamp when the result was produced.
    """
    event_id: str
    correlation_id: str
    entity_type: str
    entity_id: str
    result_id: str
    status: str
    duration_ms: int
    completed_at: str
