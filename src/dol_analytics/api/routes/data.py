from datetime import date, timedelta
from typing import Dict, Any, Optional
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session

# Use relative imports if running as a module
try:
    from ...models.database import get_db
    from ...services.data_processor import DataProcessor
    from ...services.dol_api import DOLAPIClient
    from ...models.schemas import DashboardData, DailyVolumeData, WeeklyAverageData, WeeklyVolumeData, MonthlyVolumeData
except ImportError:
    # Use absolute imports if running as a script
    from src.dol_analytics.models.database import get_db
    from src.dol_analytics.services.data_processor import DataProcessor
    from src.dol_analytics.services.dol_api import DOLAPIClient
    from src.dol_analytics.models.schemas import DashboardData, DailyVolumeData, WeeklyAverageData, WeeklyVolumeData, MonthlyVolumeData

router = APIRouter(prefix="/data", tags=["data"])


@router.get("/dashboard", response_model=DashboardData)
async def get_dashboard_data(
    days: int = Query(30, ge=1, le=365, description="Number of days to include in data"),
    db: Session = Depends(get_db)
):
    """
    Get dashboard visualization data including:
    - Daily volume over time
    - Weekly averages by day of week
    - Weekly volume as a bar chart
    - Monthly volume over past 2 months
    - Today's progress metrics
    - Current backlog
    """
    processor = DataProcessor(db)
    data = processor.get_dashboard_data(days=days)
    
    return DashboardData(
        daily_volume=data["daily_volume"],
        weekly_averages=data["weekly_averages"],
        weekly_volumes=data["weekly_volumes"],
        monthly_volumes=data["monthly_volumes"],
        todays_progress=data["todays_progress"],
        current_backlog=data["current_backlog"]
    )


@router.get("/daily-volume")
async def get_daily_volume(
    start_date: Optional[date] = Query(None, description="Start date (defaults to 30 days ago)"),
    end_date: Optional[date] = Query(None, description="End date (defaults to today)"),
    db: Session = Depends(get_db)
):
    """Get daily volume data for a specific date range."""
    # Set default dates if not provided
    if not end_date:
        end_date = date.today()
    
    if not start_date:
        start_date = end_date - timedelta(days=30)
    
    # Validate date range
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="Start date must be before end date")
    
    processor = DataProcessor(db)
    daily_data = processor._get_daily_volume(start_date, end_date)
    
    return {"data": daily_data}


@router.get("/weekly-averages")
async def get_weekly_averages(
    start_date: Optional[date] = Query(None, description="Start date (defaults to 30 days ago)"),
    end_date: Optional[date] = Query(None, description="End date (defaults to today)"),
    db: Session = Depends(get_db)
):
    """Get average volume by day of week."""
    # Set default dates if not provided
    if not end_date:
        end_date = date.today()
    
    if not start_date:
        start_date = end_date - timedelta(days=30)
    
    # Validate date range
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="Start date must be before end date")
    
    processor = DataProcessor(db)
    weekly_data = processor._get_weekly_averages(start_date, end_date)
    
    return {"data": weekly_data}


@router.get("/weekly-volumes")
async def get_weekly_volumes(
    start_date: Optional[date] = Query(None, description="Start date (defaults to 30 days ago)"),
    end_date: Optional[date] = Query(None, description="End date (defaults to today)"),
    db: Session = Depends(get_db)
):
    """Get weekly volume totals."""
    # Set default dates if not provided
    if not end_date:
        end_date = date.today()
    
    if not start_date:
        start_date = end_date - timedelta(days=30)
    
    # Validate date range
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="Start date must be before end date")
    
    processor = DataProcessor(db)
    weekly_data = processor._get_weekly_volumes(start_date, end_date)
    
    return {"data": weekly_data}


@router.get("/monthly-volumes")
async def get_monthly_volumes(
    months: int = Query(2, ge=1, le=24, description="Number of months to include"),
    db: Session = Depends(get_db)
):
    """Get monthly volume data."""
    today = date.today()
    
    # Calculate start date based on number of months
    end_date = today
    start_date = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
    
    # Go back additional months
    for _ in range(months - 1):
        start_date = (start_date.replace(day=1) - timedelta(days=1)).replace(day=1)
    
    processor = DataProcessor(db)
    monthly_data = processor._get_monthly_volumes(start_date, end_date)
    
    return {"data": monthly_data}


@router.get("/todays-progress")
async def get_todays_progress(
    db: Session = Depends(get_db)
):
    """Get today's progress metrics."""
    processor = DataProcessor(db)
    progress_data = processor._get_todays_progress()
    
    return progress_data


@router.post("/refresh")
async def refresh_data(
    db: Session = Depends(get_db)
):
    """Manually trigger data refresh from DOL API."""
    try:
        processor = DataProcessor(db)
        await processor.fetch_and_process_daily_data()
        return {"status": "success", "message": "Data refresh completed successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error refreshing data: {str(e)}")