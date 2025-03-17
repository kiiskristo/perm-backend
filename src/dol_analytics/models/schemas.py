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


class TodaysProgressData(BaseModel):
    """Today's progress metrics."""
    
    new_cases: int
    processed_cases: int
    new_cases_change: float
    processed_cases_change: float
    date: date
    current_backlog: int
    comparison_period: str = "day"  # Add this field with default


class DashboardData(BaseModel):
    daily_volume: List[DailyVolumeData]
    weekly_averages: List[WeeklyAverageData]
    weekly_volumes: List[WeeklyVolumeData]
    monthly_volumes: List[MonthlyVolumeData]
    todays_progress: "TodaysProgressData"  # Use string literal for forward reference
    current_backlog: int


# Prediction response schema
class ProcessingTimePrediction(BaseModel):
    submit_date: date
    estimated_completion_date: date
    upper_bound_date: date
    estimated_days: int
    upper_bound_days: int
    factors_considered: Dict[str, Any]
    confidence_level: float


class CasePrediction(ProcessingTimePrediction):
    case_id: str
    note: Optional[str] = None


class MonthlyBacklogData(BaseModel):
    """Monthly backlog data showing ANALYST REVIEW cases."""
    month: str
    year: int
    backlog: int
    is_active: bool = False
    withdrawn: int = 0