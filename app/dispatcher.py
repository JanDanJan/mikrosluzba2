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
    """
    Return the current Kafka producer or lazily create a real one.

    In tests I monkeypatch `producer` with a fake object; otherwise a Confluent
    `Producer` is instantiated on first use.

    @return Producer: Confluent Kafka producer configured for idempotent delivery.
    """
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
    """
    Flush the producer with retry, allowing delivery callbacks to fire.

    @param p: Producer-like object exposing `flush(timeout)`.
    @return None
    """
    p.flush(10)

def run_outbox_dispatcher(stop_flag, prod=None):
    """
    Dispatch unsent outbox events to Kafka until `stop_flag` is set.

    @param stop_flag: `threading.Event`-like object; loop exits when set.
    @param prod: Optional producer instance; if None, uses or creates the global producer.
    @return None
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
    Build a delivery callback that marks the outbox event as sent.

    The returned lambda accepts `(err, msg, rid=None)` for compatibility with
    fakes that pass an extra argument.

    @param outbox_id: Primary key of the outbox row to mark as sent.
    @return Callable: A function suitable for the producer's `on_delivery` hook.
    """
    return lambda err, msg, rid=None: _on_delivery(err, msg, outbox_id)

def _on_delivery(err, msg, outbox_id):
    """
    Handle producer delivery outcomes by updating the outbox row.

    @param err: Delivery error (None on success); any truthy value means failure.
    @param msg: Message object exposing `topic()` (used only for logging).
    @param outbox_id: Primary key of the `OutboxEvent` to update.
    @return None
    """
    with SessionLocal() as s:
        ob = s.get(OutboxEvent, outbox_id)
        if err:
            s.commit(); return
        ob.sent = True
        s.commit()
    log_json(event="published", topic=msg.topic(), outbox_id=outbox_id)
