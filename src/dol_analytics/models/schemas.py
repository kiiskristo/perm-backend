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
    processed_count: Optional[int] = None  # Total processed cases (certified + denied + rfi + withdrawn)
    review_count: Optional[int] = None  # Cases in ANALYST REVIEW and RECONSIDERATION APPEALS status for this employer letter and month


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
    """Monthly backlog data showing ANALYST REVIEW and RECONSIDERATION APPEALS cases (combined as backlog) and other status counts."""
    month: str
    year: int
    backlog: int
    is_active: bool = False
    withdrawn: int = 0
    denied: int = 0
    rfi: int = 0


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


# Company search schemas
class CompanySearchRequest(BaseModel):
    query: str = Field(..., min_length=3, description="Company name search query (minimum 3 characters)")
    limit: int = Field(20, ge=1, le=100, description="Maximum number of results to return")
    recaptcha_token: str = Field(..., description="Google reCAPTCHA token")


class CompanySearchResponse(BaseModel):
    companies: List[str]
    total: int
    query: str


# Company cases schemas
class CompanyCasesRequest(BaseModel):
    company_name: str = Field(..., description="Exact company name")
    start_date: date = Field(..., description="Start date for case search (minimum: March 1st, 2024)")
    end_date: date = Field(..., description="End date for case search (maximum: October 31st, 2025, 2-week window)")
    limit: int = Field(100, ge=1, le=1000, description="Maximum number of cases to return")
    offset: int = Field(0, ge=0, description="Offset for pagination")
    recaptcha_token: str = Field(..., description="Google reCAPTCHA token")


class PermCaseData(BaseModel):
    """Individual PERM case data from perm_cases table."""
    case_number: str
    job_title: str
    submit_date: date
    employer_name: str
    employer_first_letter: str
    status: str


class CompanyCasesResponse(BaseModel):
    cases: List[PermCaseData]
    total: int
    limit: int
    offset: int
    company_name: str
    date_range: Dict[str, str]  # start_date and end_date as ISO strings


# Updated cases schemas (tracks by updated_at timestamp)
class UpdatedCasesRequest(BaseModel):
    target_date: date = Field(..., description="Date to search for case updates (ET timezone). Must be between March 1st, 2024 and today.")
    limit: int = Field(100, ge=1, le=1000, description="Maximum number of cases to return")
    offset: int = Field(0, ge=0, description="Offset for pagination")


class UpdatedPermCaseData(BaseModel):
    """Individual PERM case data with update timestamp information."""
    case_number: str
    job_title: Optional[str] = None
    submit_date: date
    employer_name: Optional[str] = None
    employer_first_letter: str
    status: str
    previous_status: Optional[str] = None  # Previous status before the update
    updated_at: datetime  # When the case was last updated (converted to ET)


class UpdatedCasesResponse(BaseModel):
    cases: List[UpdatedPermCaseData]
    total: int
    limit: int
    offset: int
    target_date: str  # ISO format date string
    timezone_note: str  # Note about timezone conversion


# Chatbot schemas
class ChatbotRequest(BaseModel):
    """Request for chatbot endpoint."""
    message: str


class ChatbotLink(BaseModel):
    """Link in chatbot response."""
    text: str
    url: str
    description: str


class ChatbotResponse(BaseModel):
    """Response from chatbot endpoint."""
    response: str
    type: str  # "count", "case_found", "case_not_found", "processing_time", "recent_activity", "help", "error", "unknown"
    data: Optional[Dict[str, Any]] = None
    links: List[ChatbotLink] = []