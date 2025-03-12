import os
from datetime import datetime, date
from typing import Generator, Any
from functools import lru_cache
from contextlib import contextmanager

from sqlalchemy import create_engine, Column, Integer, String, Float, Date, DateTime, Boolean, ForeignKey, func, desc
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
import psycopg2
import psycopg2.extras

from src.dol_analytics.config import get_settings

settings = get_settings()

# SQLAlchemy setup for existing models
SQLALCHEMY_DATABASE_URL = settings.DATABASE_URL

engine = create_engine(SQLALCHEMY_DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

# PostgreSQL connection for the external database
try:
    from src.dol_analytics.secrets import POSTGRES_URL
    # Use this URL when available
    POSTGRES_CONNECTION_STRING = POSTGRES_URL
except ImportError:
    # Fall back to environment variable
    POSTGRES_CONNECTION_STRING = settings.POSTGRES_DATABASE_URL


def get_db() -> Generator[Session, None, None]:
    """Dependency for database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_postgres_connection():
    """Dependency for PostgreSQL connection to the external database."""
    if settings.DEBUG and (
        not POSTGRES_CONNECTION_STRING or 
        POSTGRES_CONNECTION_STRING.startswith("sqlite:")
    ):
        # Development mode - use SQLAlchemy session instead
        print("WARNING: Using SQLite database for development. Some features may not work correctly.")
        db = SessionLocal()
        try:
            yield db
        finally:
            db.close()
    else:
        # Production mode - use PostgreSQL
        conn = psycopg2.connect(POSTGRES_CONNECTION_STRING)
        try:
            yield conn
        finally:
            conn.close()


def init_db():
    """Initialize database tables."""
    Base.metadata.create_all(bind=engine)


# Original models - can be phased out gradually as we migrate to external PostgreSQL
class DailyMetrics(Base):
    """Store daily aggregated metrics."""
    
    __tablename__ = "daily_metrics"
    
    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, unique=True, index=True)
    new_cases = Column(Integer, default=0)
    processed_cases = Column(Integer, default=0)
    backlog = Column(Integer, default=0)
    avg_processing_time = Column(Float, nullable=True)  # In days
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class CaseData(Base):
    """Store case data."""
    
    __tablename__ = "case_data"
    
    id = Column(Integer, primary_key=True, index=True)
    case_identifier = Column(String, unique=True, index=True)
    submit_date = Column(Date, index=True)
    processed_date = Column(Date, nullable=True, index=True)
    status = Column(String, index=True)
    agency = Column(String, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class PredictionModel(Base):
    """Store prediction model parameters."""
    
    __tablename__ = "prediction_models"
    
    id = Column(Integer, primary_key=True, index=True)
    model_date = Column(Date, unique=True, index=True)
    base_processing_time = Column(Float)  # Base processing time in days
    backlog_factor = Column(Float)  # Coefficient for backlog impact
    seasonal_factors = Column(String)  # JSON string for monthly/seasonal factors
    created_at = Column(DateTime, default=datetime.utcnow)