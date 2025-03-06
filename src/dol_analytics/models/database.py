from datetime import datetime
from typing import Optional
from sqlalchemy import Column, Integer, Float, String, DateTime, Date, ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, Session

from src.dol_analytics.config import get_settings

settings = get_settings()

Base = declarative_base()


class CaseData(Base):
    """Store information about DOL cases."""
    
    __tablename__ = "case_data"
    
    id = Column(Integer, primary_key=True, index=True)
    case_identifier = Column(String, unique=True, index=True)
    submit_date = Column(Date, index=True)
    status = Column(String)
    agency = Column(String)
    processed_date = Column(Date, nullable=True)
    estimated_completion_date = Column(Date, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


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


class PredictionModel(Base):
    """Store prediction model parameters."""
    
    __tablename__ = "prediction_models"
    
    id = Column(Integer, primary_key=True, index=True)
    model_date = Column(Date, unique=True, index=True)
    base_processing_time = Column(Float)  # Base time in days
    backlog_factor = Column(Float)  # Additional days per case in backlog
    seasonal_factors = Column(String)  # JSON string of monthly/daily factors
    created_at = Column(DateTime, default=datetime.utcnow)


# Database connection
engine = create_engine(settings.DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    """Dependency for database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """Initialize database tables."""
    Base.metadata.create_all(bind=engine)