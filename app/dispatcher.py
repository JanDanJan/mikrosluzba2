# app/dispatcher.py
import json
import time
from tenacity import retry, wait_exponential, stop_after_attempt
from confluent_kafka import Producer
from sqlalchemy import select
from app.config import KAFKA_BOOTSTRAP, OUTBOX_POLL_MS, OUTBOX_BATCH
from app.db import SessionLocal, OutboxEvent
from app.logging_setup import log_json

# Allow tests to monkeypatch this (set to a fake producer).
# If left as None, we'll lazily create a real Producer.
producer = None

def _get_producer():
    """Return the current producer (fake in tests) or lazily create a real one."""
    global producer
    if producer is not None:
        return producer
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "enable.idempotence": True,
        "acks": "all",
    })
    return producer

@retry(wait=wait_exponential(min=0.5, max=10), stop=stop_after_attempt(5))
def _flush(p):
    p.flush(10)

def run_outbox_dispatcher(stop_flag, prod=None):
    """
    Dispatch unsent outbox events to Kafka until stop_flag is set.
    """
    p = prod or _get_producer()
    while not stop_flag.is_set():
        with SessionLocal() as s:
            rows = s.execute(
                select(OutboxEvent).where(OutboxEvent.sent == False).limit(OUTBOX_BATCH)
            ).scalars().all()
            for row in rows:
                p.produce(
                    topic=row.topic,
                    key=row.key,
                    value=json.dumps(row.payload).encode("utf-8"),
                    on_delivery=_make_delivery_cb(row.id)
                )
            if rows:
                _flush(p)
        time.sleep(OUTBOX_POLL_MS / 1000.0)

def _make_delivery_cb(outbox_id):
    """
    Return a delivery callback that marks the outbox event as sent.
    Accepts a third arg for compatibility with test fakes that call on_delivery(err, msg, rid).
    """
    return lambda err, msg, rid=None: _on_delivery(err, msg, outbox_id)

def _on_delivery(err, msg, outbox_id):
    with SessionLocal() as s:
        ob = s.get(OutboxEvent, outbox_id)
        if err:
            s.commit(); return
        ob.sent = True
        s.commit()
    log_json(event="published", topic=msg.topic(), outbox_id=outbox_id)
