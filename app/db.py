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
    """Reflect db tables once. Safe to call multiple times."""
    global _reflected
    if _reflected:
        return
    walrus_metadata.reflect(bind=engine)
    _reflected = True

def get_entity_table(entity_type: str) -> Optional[Table]:
    """Return a reflected Table for the given entity_type (table name)."""
    ensure_reflected()
    if ALLOWED_ENTITY_TABLES:
        if entity_type not in ALLOWED_ENTITY_TABLES:
            return None
    return walrus_metadata.tables.get(entity_type)

# --- Native MS2 tables (created/managed by this service) such as inbox / outbox etc. ---
class Base(DeclarativeBase):
    pass

class CalcResult(Base):
    __tablename__ = "calc_results"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    entity_type = Column(String, nullable=False)
    entity_id = Column(String, nullable=False, index=True)
    status = Column(String, nullable=False)
    payload = Column(JSON, nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())

class OutboxEvent(Base):
    __tablename__ = "outbox_events"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    topic = Column(String, nullable=False)
    key = Column(String, nullable=True)
    payload = Column(JSON, nullable=False)
    sent = Column(Boolean, nullable=False, default=False)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now())

class InboxEvent(Base):
    __tablename__ = "inbox_events"
    event_id = Column(String, primary_key=True)

def init_db():
    """
    Create native tables if they don't exist.
    """
    Base.metadata.create_all(bind=engine)

# --- Helpers to fetch a row from a reflected table ---
def fetch_entity_row(session, entity_type: str, entity_id: str) -> Optional[Dict[str, Any]]:
    """
    Load a row from the reflected db table by primary key 'id'.
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
