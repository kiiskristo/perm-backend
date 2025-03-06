from datetime import date, datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


# Case schemas
class CaseBase(BaseModel):
    case_identifier: str
    submit_date: date
    status: str
    agency: str


class CaseCreate(CaseBase):
    pass


class CaseUpdate(BaseModel):
    status: Optional[str] = None
    processed_date: Optional[date] = None
    estimated_completion_date: Optional[date] = None


class CaseInDB(CaseBase):
    id: int
    processed_date: Optional[date] = None
    estimated_completion_date: Optional[date] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# Metrics schemas
class DailyMetricsBase(BaseModel):
    date: date
    new_cases: int
    processed_cases: int
    backlog: int
    avg_processing_time: Optional[float] = None


class DailyMetricsCreate(DailyMetricsBase):
    pass


class DailyMetricsInDB(DailyMetricsBase):
    id: int
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# Prediction model schemas
class PredictionModelBase(BaseModel):
    model_date: date
    base_processing_time: float
    backlog_factor: float
    seasonal_factors: str  # JSON string


class PredictionModelCreate(PredictionModelBase):
    pass


class PredictionModelInDB(PredictionModelBase):
    id: int
    created_at: datetime

    class Config:
        from_attributes = True


# Visualization response schemas
class DailyVolumeData(BaseModel):
    date: date
    count: int


class WeeklyAverageData(BaseModel):
    day_of_week: str
    average_volume: float


class WeeklyVolumeData(BaseModel):
    week_starting: date
    total_volume: int


class MonthlyVolumeData(BaseModel):
    month: str
    year: int
    total_volume: int


class DashboardData(BaseModel):
    daily_volume: List[DailyVolumeData]
    weekly_averages: List[WeeklyAverageData]
    weekly_volumes: List[WeeklyVolumeData]
    monthly_volumes: List[MonthlyVolumeData]
    todays_progress: Dict[str, Any]
    current_backlog: int


# Prediction response schema
class CasePrediction(BaseModel):
    case_identifier: str
    submit_date: date
    estimated_completion_date: date
    confidence_level: float = Field(..., ge=0.0, le=1.0)
    factors_considered: Dict[str, Any]