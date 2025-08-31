import json
from types import SimpleNamespace

def _msg_parts(event_id: str, entity_type: str, entity_id: str):
    # Minimal message object used by handle_message (pipe-delimited payload)
    b = f"{event_id}|{entity_type}|{entity_id}".encode("utf-8")
    return SimpleNamespace(value=lambda: b, error=lambda: None)

def _fake_consumer():
    committed = {"count": 0}
    def commit(message=None, asynchronous=False):
        committed["count"] += 1
    return SimpleNamespace(commit=commit), committed

def test_happy_path(fresh_db, walrus_tables, sample_row, fake_producer):
    # Import after DB prepared
    from app.db import SessionLocal, OutboxEvent, CalcResult, init_db
    from app.consumer import handle_message
    from app.dispatcher import run_outbox_dispatcher
    import threading, time

    init_db()

    # 1) Process a request
    # New message format: "event_id|entity_type|entity_id"
    evt_id = "evt-001"
    msg = _msg_parts(evt_id, sample_row["entity_type"], sample_row["entity_id"])
    consumer, committed = _fake_consumer()

    handle_message(msg, consumer)
    assert committed["count"] == 1

    # DB should have a result + one outbox event unsent
    with SessionLocal() as s:
        results = s.query(CalcResult).all()
        assert len(results) == 1
        assert results[0].status == "SUCCESS"

        ob = s.query(OutboxEvent).filter_by(sent=False).all()
        assert len(ob) == 1

    # 2) Run dispatcher briefly to publish and mark sent
    stop = threading.Event()
    t = threading.Thread(target=run_outbox_dispatcher, args=(stop,), daemon=True)
    t.start()
    time.sleep(0.5)
    stop.set(); t.join(timeout=2)

    # Outbox should be marked sent and fake_producer should have captured one message
    with SessionLocal() as s:
        ob = s.query(OutboxEvent).filter_by(sent=True).all()
        assert len(ob) == 1

    assert len(fake_producer) == 1
    assert fake_producer[0]["topic"] == "calc.completed"
    assert fake_producer[0]["value"]["correlation_id"] == evt_id

def test_idempotency(fresh_db, walrus_tables, sample_row, fake_producer):
    from app.db import SessionLocal, CalcResult, init_db
    from app.consumer import handle_message

    init_db()

    msg = _msg_parts("evt-dup", sample_row["entity_type"], sample_row["entity_id"])
    consumer, _ = _fake_consumer()
    handle_message(msg, consumer)
    handle_message(msg, consumer)  # same event again

    with SessionLocal() as s:
        results = s.query(CalcResult).all()
        assert len(results) == 1  # processed once

def test_entity_not_found(fresh_db, walrus_tables, fake_producer):
    from app.db import SessionLocal, CalcResult, OutboxEvent, init_db
    from app.consumer import handle_message

    init_db()
    msg = _msg_parts("evt-missing", "electric_sensor", "999")
    consumer, _ = _fake_consumer()
    handle_message(msg, consumer)

    with SessionLocal() as s:
        r = s.query(CalcResult).all()
        assert len(r) == 1
        assert r[0].status == "FAILED"
        ob = s.query(OutboxEvent).all()
        assert len(ob) == 1
        assert ob[0].payload.get("status") == "FAILED"
