import os
try:
    # Load environment variables from a local .env file if present (no-op if missing)
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    # Safe fallback: proceed without .env support if package is unavailable
    pass

# Kafka
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "ms2-calculator")
TOPIC_REQUEST = os.getenv("TOPIC_REQUEST", "calc.request")
TOPIC_COMPLETED = os.getenv("TOPIC_COMPLETED", "calc.completed")

# Postgres
DB_URL = os.getenv("DB_URL", "postgresql+psycopg2://walrus:walrus@localhost:55432/walrus")

# Outbox tuning
OUTBOX_BATCH = int(os.getenv("OUTBOX_BATCH", "100"))
OUTBOX_POLL_MS = int(os.getenv("OUTBOX_POLL_MS", "1000"))

ALLOWED_ENTITY_TABLES = set(filter(None, os.getenv("ALLOWED_ENTITY_TABLES", "").split(",")))
