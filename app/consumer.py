import json, time, uuid
from datetime import datetime, timezone
from typing import Any, Dict

from tenacity import retry, wait_exponential, stop_after_attempt
from confluent_kafka import Consumer
from app.config import (KAFKA_BOOTSTRAP, KAFKA_GROUP_ID, TOPIC_REQUEST, TOPIC_COMPLETED)
from app.db import SessionLocal, CalcResult, OutboxEvent, InboxEvent, fetch_entity_row, get_entity_table
from app.schemas import CalcRequest, CalcCompleted
from sqlalchemy.exc import OperationalError
from tenacity import retry_if_exception_type
from app.logging_setup import log_json
from traceback import format_exc


def _mk_consumer():
    """
    Build a configured Kafka `Consumer` instance for this service.

    @return Consumer: Confluent Kafka consumer configured with bootstrap servers,
        group id, manual commit, and sane defaults for this workload.
    """
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": KAFKA_GROUP_ID,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "max.poll.interval.ms": 600000
    })

def _numeric_fields_score(row: Dict[str, Any]) -> float:
    """
    Compute a simple score by summing numeric-looking values in an entity row.

    @param row: Mapping of column name to value from a reflected table.
    @return float: Sum of numeric fields (ints/floats); attempts to parse
        numeric strings. Rounded to 6 decimal places.
    """
    total = 0.0
    for k, v in row.items():
        if isinstance(v, (int, float)):
            total += float(v)
        elif isinstance(v, str):
            try:
                total += float(v)
            except ValueError:
                pass
    return round(total, 6)

def mock_compute(entity_row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Perform the placeholder computation over an entity row.

    This stub sums numeric-looking fields and records the input columns. Replace
    with real domain logic when available.

    @param entity_row: Mapping of column name to value for the entity.
    @return Dict[str, Any]: Result payload with keys `score` and `source_columns`.
    """
    return {
        "score": _numeric_fields_score(entity_row),
        "source_columns": list(entity_row.keys())
    }

@retry(
    wait=wait_exponential(min=0.5, max=10),
    stop=stop_after_attempt(5),
    retry=retry_if_exception_type(OperationalError),
    reraise=True,
)
def handle_message(msg, c: Consumer):
    """
    Handle a single Kafka message: validate, dedupe, compute, persist, enqueue.

    Behavior:
    - Parses a pipe-delimited payload: `event_id|entity_type|entity_id`.
    - Deduplicates via `InboxEvent` (idempotency) before doing work.
    - On missing entities, records `FAILED` and enqueues a completion outbox event.
    - On success, computes a result, persists `CalcResult`, and enqueues outbox.
    - Commits the Kafka offset after the database transaction commits.
    - Retries on transient SQLAlchemy `OperationalError` via tenacity.

    @param msg: Kafka message-like object with `value()` returning bytes.
    @param c: Confluent Kafka `Consumer`, used for manual commit.
    @return None
    """
    try:
        data = msg.value().decode("utf-8").strip()
        items = data.split("|")
        print(f"Items: {items}")
        req = CalcRequest(
            event_id=items[0],
            entity_type=items[1],
            entity_id=items[2]
        )
    except Exception as e:
        log_json(message="Error while handling message", data=data, traceback=format_exc())
        c.commit(message=msg, asynchronous=False)
        return

    log_json(event="received", topic="calc.request", event_id=req.event_id, entity_type=req.entity_type, entity_id=req.entity_id)

    started = time.perf_counter()
    with SessionLocal() as s:
        # Deduplication via inbox
        if s.get(InboxEvent, req.event_id):
            c.commit(message=msg, asynchronous=False)
            return

        row = fetch_entity_row(s, req.entity_type, req.entity_id)
        if row is None:
            # Persist a FAILED result but still emit completion event
            result = CalcResult(
                entity_type=req.entity_type,
                entity_id=req.entity_id,
                status="FAILED",
                payload={"reason": "entity_not_found"}
            )
            s.add(result)
            completed_payload = _completed_payload(req, result.id, "FAILED", started, extra={"reason":"entity_not_found"})
            s.add(OutboxEvent(topic=TOPIC_COMPLETED, key=req.entity_id, payload=completed_payload))
            s.add(InboxEvent(event_id=req.event_id))
            s.commit()
            c.commit(message=msg, asynchronous=False)
            return

        result_payload = mock_compute(row)

        result = CalcResult(
            entity_type=req.entity_type,
            entity_id=req.entity_id,
            status="SUCCESS",
            payload=result_payload
        )
        s.add(result)

        completed_payload = _completed_payload(req, result.id, "SUCCESS", started)
        s.add(OutboxEvent(
            topic=TOPIC_COMPLETED,
            key=req.entity_id,
            payload=completed_payload
        ))

        s.add(InboxEvent(event_id=req.event_id))
        s.commit()

    # Commit Kafka offset only after DB commit
    c.commit(message=msg, asynchronous=False)
    log_json(event="db_commit", result_id=result.id, status="SUCCESS")

def _completed_payload(req: CalcRequest, result_id: str, status: str, started: float, extra: Dict[str, Any] | None = None) -> Dict[str, Any]:
    """
    Build the completion event payload corresponding to a processed request.

    @param req: Original calculation request model.
    @param result_id: Identifier of the persisted `CalcResult` row.
    @param status: Outcome status, typically 'SUCCESS' or 'FAILED'.
    @param started: Monotonic timestamp captured before processing, used to compute duration.
    @param extra: Optional additional fields to merge into the payload (e.g., reason).
    @return Dict[str, Any]: JSON-serializable completion event payload.
    """
    duration_ms = int((time.perf_counter() - started) * 1000)
    payload = {
        "event_id": str(uuid.uuid4()),
        "correlation_id": req.event_id,
        "entity_type": req.entity_type,
        "entity_id": req.entity_id,
        "result_id": result_id,
        "status": status,
        "duration_ms": duration_ms,
        "completed_at": datetime.now(timezone.utc).isoformat()
    }
    if extra:
        payload.update(extra)
    return payload

def run_consumer(stop_flag):
    """
    Run the Kafka consumer loop until `stop_flag` is set.

    @param stop_flag: `threading.Event`-like object; when set, exits the loop.
    @return None
    """
    c = _mk_consumer()
    c.subscribe([TOPIC_REQUEST])
    try:
        while not stop_flag.is_set():
            msg = c.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                continue
            handle_message(msg, c)
    finally:
        c.close()
