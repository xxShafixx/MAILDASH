from __future__ import annotations

import os
from typing import Optional

from sqlalchemy import (
    create_engine, String, Integer, DateTime, Text, Float, Index, func, text
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker
from ..config import DATABASE_URL

# --- SQLAlchemy setup ---
class Base(DeclarativeBase):
    pass

engine = create_engine(
    DATABASE_URL,
    echo=False,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

# -------------------------------
# Tables
# -------------------------------

class IngestRun(Base):
    """
    Optional: keep a log of each ingest attempt (for audit/debug).
    NOTE: This table is independent of metrics; keeping sender_email here is harmless.
    """
    __tablename__ = "ingest_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    provider: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)  # 'microsoft' | 'google' | 'local'
    client:   Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    region:   Mapped[Optional[str]] = mapped_column(String(128), nullable=True)

    sender_email: Mapped[Optional[str]] = mapped_column(String(256), nullable=True)  # optional audit only
    message_id:   Mapped[Optional[str]] = mapped_column(String(256), nullable=True)
    subject:      Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    received_at:  Mapped[Optional[DateTime]] = mapped_column(DateTime(timezone=True), nullable=True)

    status: Mapped[str] = mapped_column(String(16), default="success")  # success | partial | failed
    error_text: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class TimeseriesData(Base):
    """
    Main metrics table.

    Business rules for uniqueness:
      - For clients WITH regions:   (client, region, sheet_name, parameter, ts_utc) must be unique.
      - For clients WITHOUT region: (client, sheet_name, parameter, ts_utc) must be unique.

    We implement this using two PARTIAL UNIQUE INDEXes (sqlite_where).
    """
    __tablename__ = "timeseries_data"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # pair identity
    client:    Mapped[str] = mapped_column(String(128), index=True)
    region:    Mapped[Optional[str]] = mapped_column(String(128), nullable=True, index=True)   # optional
    workspace: Mapped[Optional[str]] = mapped_column(String(32),  nullable=True, index=True)   # e.g. LIVE/FED/MIG

    # mail identity
    sheet_name: Mapped[str] = mapped_column(String(256), index=True)
    parameter:  Mapped[str] = mapped_column(String(256), index=True)
    # store ts as ISO string "YYYY-MM-DD HH:MM:SS+00:00" to match existing pipeline
    ts_utc:     Mapped[str] = mapped_column(String(32),  index=True)

    # value + provenance
    value:       Mapped[Optional[Float]] = mapped_column(Float, nullable=True)
    message_id:  Mapped[Optional[str]]   = mapped_column(String(256), nullable=True)
    received_utc: Mapped[Optional[DateTime]] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        # 1) Unique when REGION IS NOT NULL (regioned clients)
        Index(
            "uq_tsd_with_region",
            "client", "region", "sheet_name", "parameter", "ts_utc",
            unique=True,
            sqlite_where=text("region IS NOT NULL")
        ),
        # 2) Unique when REGION IS NULL (regionless clients)
        Index(
            "uq_tsd_without_region",
            "client", "sheet_name", "parameter", "ts_utc",
            unique=True,
            sqlite_where=text("region IS NULL")
        ),
        # helpful read indices
        Index("ix_tsd_lookup_pair_ts", "client", "region", "workspace", "ts_utc"),
        Index("ix_tsd_param_ts", "client", "region", "parameter", "ts_utc"),
    )


def init_db():
    """
    Creates tables & indexes. Safe to call on startup.
    """
    Base.metadata.create_all(bind=engine)


# Dependency for FastAPI routes
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
