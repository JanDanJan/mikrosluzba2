import os
import json
import types
import shutil
import pytest

TEST_DB = "sqlite:///./test.db"

@pytest.fixture(autouse=True, scope="session")
def _set_test_env():
    # Ensure the test DB is used by the app modules
    os.environ["DB_URL"] = TEST_DB
    os.environ.pop("ALLOWED_ENTITY_TABLES", None)  # reflect all
    yield
    # cleanup DB file after session
    try:
        os.remove("./test.db")
    except FileNotFoundError:
        pass

@pytest.fixture()
def fresh_db():
    # Import after env set so engine is created with TEST_DB
    from app.db import engine, Base, init_db
    # Drop our own tables if they exist (fresh start)
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    yield

@pytest.fixture()
def walrus_tables():
    # Create a minimal reflected table that the app can read from
    from sqlalchemy import text
    from app.db import engine
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS electric_sensor (
              id INTEGER PRIMARY KEY,
              power REAL,
              voltage REAL,
              energy REAL,
              curent REAL
            );
        """))
    yield

@pytest.fixture()
def sample_row():
    from sqlalchemy import text
    from app.db import engine
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO electric_sensor (id, power, voltage, energy, curent)
            VALUES (1, 10.0, 230.0, 0.5, 0.05)
            ON CONFLICT(id) DO NOTHING;
        """))
    return {"entity_type": "electric_sensor", "entity_id": "1"}

@pytest.fixture()
def fake_producer(monkeypatch):
    # Capture produced messages
    sent = []

    class _FakeProducer:
        def produce(self, topic, key=None, value=None, on_delivery=None):
            payload = {
                "topic": topic,
                "key": key.decode() if isinstance(key, (bytes, bytearray)) else key,
                "value": json.loads(value.decode() if isinstance(value, (bytes, bytearray)) else value)
            }
            sent.append(payload)
            # Simulate successful delivery
            if on_delivery:
                on_delivery(None, types.SimpleNamespace(topic=topic), payload["value"].get("event_id", "outbox-id-unknown"))

        def flush(self, timeout):
            return 0

    import app.dispatcher as dispatcher
    monkeypatch.setattr(dispatcher, "producer", _FakeProducer())
    return sent
