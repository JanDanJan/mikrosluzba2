from __future__ import annotations
import uuid
from typing import Optional, Dict, Any

from sqlalchemy import (
    create_engine, MetaData, Table, Column, String, Integer, Text, JSON,
    TIMESTAMP, func, Boolean, select
)
from sqlalchemy.orm import sessionmaker, DeclarativeBase

from app.config import DB_URL, ALLOWED_ENTITY_TABLES

# --- Engine / Session ---
engine = create_engine(DB_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, future=True)

# --- Reflect the Walrus schema ---
walrus_metadata = MetaData(schema=None)
# Lazy reflection (defer until first access) to avoid errors if the schema has not been loaded yet.
_reflected = False

def ensure_reflected() -> None:
    """
    Ensure the external (Walrus) schema is reflected into SQLAlchemy metadata.

    I call this lazily from places that need reflected tables so that tests and
    early imports don't fail when the database is not ready yet.

    @return None: Reflection state is cached in-module to avoid repeated work.
    """
    global _reflected
    if _reflected:
        return
    walrus_metadata.reflect(bind=engine)
    _reflected = True

def get_entity_table(entity_type: str) -> Optional[Table]:
    """
    Get a reflected `Table` object for a given entity type (i.e., table name).

    @param entity_type: Name of the table to fetch from the reflected metadata.
    @return Optional[Table]: The reflected table if it exists and passes
        `ALLOWED_ENTITY_TABLES` filtering; otherwise None.
    """
    ensure_reflected()
    if ALLOWED_ENTITY_TABLES:
        if entity_type not in ALLOWED_ENTITY_TABLES:
            return None
    return walrus_metadata.tables.get(entity_type)

# --- Native MS2 tables (created/managed by this service) such as inbox / outbox etc. ---
class Base(DeclarativeBase):
    """Declarative base for this service's native tables (inbox/outbox/results)."""
    pass

class CalcResult(Base):
    __tablename__ = "calc_results"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    entity_type = Column(String, nullable=False)
    entity_id = Column(String, nullable=False, index=True)
    status = Column(String, nullable=False)
    payload = Column(JSON, nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    """Persisted outcome of a computation for a single entity.

    Fields:
        id: UUID string identifier of the result row.
        entity_type: Name of the source table/entity type.
        entity_id: Primary key of the source entity (stringified).
        status: 'SUCCESS' or 'FAILED'.
        payload: Arbitrary JSON with result details or failure reason.
        created_at: Server-side timestamp of row creation.
    """

class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    topic = Column(String, nullable=False)
    key = Column(String, nullable=True)
    payload = Column(JSON, nullable=False)
    sent = Column(Boolean, nullable=False, default=False)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())
    """Outbox row to be dispatched to Kafka.

    Fields:
        id: UUID string of the outbox record.
        topic: Kafka topic to publish to.
        key: Kafka message key.
        payload: JSON payload to publish.
        sent: Marker set to True after successful delivery callback.
        created_at: Server-side timestamp of row creation.
    """

class InboxEvent(Base):
    __tablename__ = "inbox_events"
    event_id = Column(String, primary_key=True)
    """Inbox deduplication table storing processed event IDs."""

def init_db():
    """
    Create native tables if they don't exist.

    I call this at startup (and in tests) to ensure the service-managed tables
    are present regardless of the external reflected schema state.

    @return None
    """
    Base.metadata.create_all(bind=engine)

# --- Helpers to fetch a row from a reflected table ---
def fetch_entity_row(session, entity_type: str, entity_id: str) -> Optional[Dict[str, Any]]:
    """
    Fetch a single entity row from a reflected table by primary key `id`.

    @param session: An active SQLAlchemy session to execute the query.
    @param entity_type: Reflected table name to query.
    @param entity_id: Primary key value. I try to cast to int and fall back to str.
    @return Optional[Dict[str, Any]]: Mapping of column name to value if found; otherwise None.
    """
    table = get_entity_table(entity_type)
    if table is None:
        return None

    # Best effort cast to int".
    pk_val: Any
    try:
        pk_val = int(entity_id)
    except ValueError:
        pk_val = entity_id  # fall back (in case schema changes)

    stmt = select(table).where(table.c.id == pk_val)
    row = session.execute(stmt).mappings().first()
    return dict(row) if row else None
