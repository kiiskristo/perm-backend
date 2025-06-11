from datetime import date, datetime
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field, ConfigDict


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
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    processed_date: Optional[date] = None
    estimated_completion_date: Optional[date] = None
    created_at: datetime
    updated_at: datetime


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
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    created_at: datetime
    updated_at: datetime


# Prediction model schemas
class PredictionModelBase(BaseModel):
    model_date: date
    base_processing_time: float
    backlog_factor: float
    seasonal_factors: str  # JSON string


class PredictionModelCreate(PredictionModelBase):
    pass


class PredictionModelInDB(PredictionModelBase):
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    created_at: datetime


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
    """
    Daily progress metrics comparing the most recent day to the same day last week.
    Provides a consistent daily snapshot regardless of dashboard time period.
    """
    
    new_cases: int
    processed_cases: int
    new_cases_change: float
    processed_cases_change: float
    date: date
    current_backlog: int
    comparison_days: int = 7  # Always compare to 7 days ago (last week)
    comparison_period: str = "Same Day Last Week"  # Fixed comparison description
    period_label: str = "Today"  # Fixed period label


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


# PERM Cases schemas
class PermCaseActivityData(BaseModel):
    """PERM case activity data by employer first letter and month."""
    employer_first_letter: str
    submit_month: int
    certified_count: int
    review_count: Optional[int] = None  # Cases in ANALYST REVIEW status for this employer letter and month


class PermCasesMetrics(BaseModel):
    """PERM cases metrics for dashboard integration."""
    activity_data: List[PermCaseActivityData]
    most_active_letter: Optional[str] = None
    most_active_month: Optional[int] = None
    total_certified_cases: int
    data_date: date


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


# Prediction request schemas
class PredictionRequestBase(BaseModel):
    submit_date: date
    employer_first_letter: str = Field(..., min_length=1, max_length=1)
    case_number: Optional[str] = None


class PredictionRequestCreate(PredictionRequestBase):
    pass


class PredictionRequestInDB(PredictionRequestBase):
    model_config = ConfigDict(from_attributes=True)
    
    id: int
    request_timestamp: datetime
    estimated_completion_date: Optional[date] = None
    estimated_days: Optional[int] = None
    confidence_level: Optional[float] = None
    created_at: datetime


class PredictionRequestResponse(PredictionRequestBase):
    request_id: int
    estimated_completion_date: date
    upper_bound_date: date
    estimated_days: int
    remaining_days: int
    upper_bound_days: int
    queue_analysis: Dict[str, Any]
    factors_considered: Dict[str, Any]
    confidence_level: float