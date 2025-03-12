from datetime import date, timedelta
from typing import Dict, Any, Optional, List
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
import psycopg2
import psycopg2.extras

# Use relative imports if running as a module
try:
    from ...models.database import get_db, get_postgres_connection
    from ...models.schemas import DashboardData, DailyVolumeData, WeeklyAverageData, WeeklyVolumeData, MonthlyVolumeData
except ImportError:
    # Use absolute imports if running as a script
    from src.dol_analytics.models.database import get_db, get_postgres_connection
    from src.dol_analytics.models.schemas import DashboardData, DailyVolumeData, WeeklyAverageData, WeeklyVolumeData, MonthlyVolumeData

router = APIRouter(prefix="/data", tags=["data"])


@router.get("/dashboard", response_model=DashboardData)
async def get_dashboard_data(
    days: int = Query(30, ge=1, le=365, description="Number of days to include in data"),
    conn=Depends(get_postgres_connection)
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
    # Get start date based on number of days
    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    
    # Get daily volume
    daily_volume = get_daily_volume_data(conn, start_date, end_date)
    
    # Get weekly averages by day of week
    weekly_averages = get_weekly_averages_data(conn, start_date, end_date)
    
    # Get weekly volumes
    weekly_volumes = get_weekly_volumes_data(conn, start_date, end_date)
    
    # Get monthly volumes (for past 2 months)
    two_months_ago = (end_date.replace(day=1) - timedelta(days=1)).replace(day=1)
    monthly_volumes = get_monthly_volumes_data(conn, two_months_ago, end_date)
    
    # Get today's progress
    todays_progress = get_todays_progress_data(conn)
    
    # Get current backlog from summary_stats
    current_backlog = get_current_backlog(conn)
    
    return DashboardData(
        daily_volume=daily_volume,
        weekly_averages=weekly_averages,
        weekly_volumes=weekly_volumes,
        monthly_volumes=monthly_volumes,
        todays_progress=todays_progress,
        current_backlog=current_backlog
    )


@router.get("/daily-volume")
async def get_daily_volume(
    start_date: Optional[date] = Query(None, description="Start date (defaults to 30 days ago)"),
    end_date: Optional[date] = Query(None, description="End date (defaults to today)"),
    conn=Depends(get_postgres_connection)
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
    
    daily_data = get_daily_volume_data(conn, start_date, end_date)
    
    return {"data": daily_data}


@router.get("/weekly-averages")
async def get_weekly_averages(
    start_date: Optional[date] = Query(None, description="Start date (defaults to 30 days ago)"),
    end_date: Optional[date] = Query(None, description="End date (defaults to today)"),
    conn=Depends(get_postgres_connection)
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
    
    weekly_data = get_weekly_averages_data(conn, start_date, end_date)
    
    return {"data": weekly_data}


@router.get("/weekly-volumes")
async def get_weekly_volumes(
    start_date: Optional[date] = Query(None, description="Start date (defaults to 30 days ago)"),
    end_date: Optional[date] = Query(None, description="End date (defaults to today)"),
    conn=Depends(get_postgres_connection)
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
    
    weekly_data = get_weekly_volumes_data(conn, start_date, end_date)
    
    return {"data": weekly_data}


@router.get("/monthly-volumes")
async def get_monthly_volumes(
    months: int = Query(2, ge=1, le=24, description="Number of months to include"),
    conn=Depends(get_postgres_connection)
):
    """Get monthly volume data."""
    today = date.today()
    
    # Calculate start date based on number of months
    end_date = today
    start_date = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
    
    # Go back additional months
    for _ in range(months - 1):
        start_date = (start_date.replace(day=1) - timedelta(days=1)).replace(day=1)
    
    monthly_data = get_monthly_volumes_data(conn, start_date, end_date)
    
    return {"data": monthly_data}


@router.get("/todays-progress")
async def get_todays_progress(
    conn=Depends(get_postgres_connection)
):
    """Get today's progress metrics."""
    progress_data = get_todays_progress_data(conn)
    
    return progress_data


# Helper functions to query PostgreSQL database

def get_daily_volume_data(conn, start_date: date, end_date: date) -> List[DailyVolumeData]:
    """Query daily_progress table for daily volume data."""
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
        cursor.execute("""
            SELECT date, total_applications as count
            FROM daily_progress
            WHERE date BETWEEN %s AND %s
            ORDER BY date
        """, (start_date, end_date))
        
        result = []
        for row in cursor.fetchall():
            result.append(DailyVolumeData(
                date=row['date'],
                count=row['count']
            ))
        
        return result


def get_weekly_averages_data(conn, start_date: date, end_date: date) -> List[WeeklyAverageData]:
    """Query daily_progress table for weekly averages by day of week."""
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
        cursor.execute("""
            SELECT day_of_week, AVG(total_applications) as average_volume
            FROM daily_progress
            WHERE date BETWEEN %s AND %s
            GROUP BY day_of_week
            ORDER BY CASE day_of_week
                WHEN 'Monday' THEN 1
                WHEN 'Tuesday' THEN 2
                WHEN 'Wednesday' THEN 3
                WHEN 'Thursday' THEN 4
                WHEN 'Friday' THEN 5
                WHEN 'Saturday' THEN 6
                WHEN 'Sunday' THEN 7
            END
        """, (start_date, end_date))
        
        result = []
        for row in cursor.fetchall():
            result.append(WeeklyAverageData(
                day_of_week=row['day_of_week'],
                average_volume=float(row['average_volume'])
            ))
        
        return result


def get_weekly_volumes_data(conn, start_date: date, end_date: date) -> List[WeeklyVolumeData]:
    """Query weekly_summary view for weekly volume data."""
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
        cursor.execute("""
            SELECT week_start, total_applications
            FROM weekly_summary
            WHERE week_start BETWEEN %s AND %s
            ORDER BY week_start
        """, (start_date, end_date))
        
        result = []
        for row in cursor.fetchall():
            result.append(WeeklyVolumeData(
                week_starting=row['week_start'],
                total_volume=row['total_applications']
            ))
        
        return result


def get_monthly_volumes_data(conn, start_date: date, end_date: date) -> List[MonthlyVolumeData]:
    """Query monthly_summary view for monthly volume data."""
    # Calculate the year and month for start and end dates
    start_year, start_month = start_date.year, start_date.month
    end_year, end_month = end_date.year, end_date.month
    
    # Convert numeric months to month names
    month_names = [
        'January', 'February', 'March', 'April', 'May', 'June',
        'July', 'August', 'September', 'October', 'November', 'December'
    ]
    
    start_month_name = month_names[start_month - 1]
    end_month_name = month_names[end_month - 1]
    
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
        # Query using month names instead of numeric comparisons
        cursor.execute("""
            WITH months_order AS (
                SELECT 'January' as month, 1 as month_num UNION
                SELECT 'February', 2 UNION
                SELECT 'March', 3 UNION
                SELECT 'April', 4 UNION
                SELECT 'May', 5 UNION
                SELECT 'June', 6 UNION
                SELECT 'July', 7 UNION
                SELECT 'August', 8 UNION
                SELECT 'September', 9 UNION
                SELECT 'October', 10 UNION
                SELECT 'November', 11 UNION
                SELECT 'December', 12
            )
            SELECT ms.year, ms.month, ms.total_count as total_volume
            FROM monthly_summary ms
            JOIN months_order mo ON ms.month = mo.month
            WHERE (ms.year > %s OR (ms.year = %s AND mo.month_num >= %s))
               AND (ms.year < %s OR (ms.year = %s AND mo.month_num <= %s))
            ORDER BY ms.year, mo.month_num
        """, (start_year, start_year, start_month, end_year, end_year, end_month))
        
        result = []
        for row in cursor.fetchall():
            result.append(MonthlyVolumeData(
                month=row['month'],
                year=row['year'],
                total_volume=row['total_volume']
            ))
        
        return result


def get_todays_progress_data(conn) -> Dict[str, Any]:
    """Query summary_stats table for today's progress metrics."""
    today = date.today()
    yesterday = today - timedelta(days=1)
    
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
        # Get today's stats
        cursor.execute("""
            SELECT total_applications as new_cases, 
                   completed_today as processed_cases,
                   changes_today
            FROM summary_stats
            WHERE record_date = %s
        """, (today,))
        
        today_row = cursor.fetchone()
        
        # Get yesterday's stats for comparison
        cursor.execute("""
            SELECT total_applications as new_cases, 
                   completed_today as processed_cases
            FROM summary_stats
            WHERE record_date = %s
        """, (yesterday,))
        
        yesterday_row = cursor.fetchone()
        
        # Calculate metrics
        new_cases = 0
        processed_cases = 0
        new_cases_change = 0
        processed_cases_change = 0
        
        if today_row:
            new_cases = today_row['new_cases'] or 0
            processed_cases = today_row['processed_cases'] or 0
        
        if yesterday_row and today_row:
            yesterday_new = yesterday_row['new_cases'] or 0
            yesterday_processed = yesterday_row['processed_cases'] or 0
            
            if yesterday_new > 0:
                new_cases_change = ((new_cases - yesterday_new) / yesterday_new) * 100
            
            if yesterday_processed > 0:
                processed_cases_change = ((processed_cases - yesterday_processed) / yesterday_processed) * 100
        
        return {
            "new_cases": new_cases,
            "processed_cases": processed_cases,
            "new_cases_change": new_cases_change,
            "processed_cases_change": processed_cases_change,
            "date": today.isoformat()
        }


def get_current_backlog(conn) -> int:
    """Query summary_stats table for current backlog."""
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT pending_applications
            FROM summary_stats
            ORDER BY record_date DESC
            LIMIT 1
        """)
        
        row = cursor.fetchone()
        return row[0] if row else 0


@router.post("/refresh")
async def refresh_data():
    """Manually trigger data refresh from DOL API."""
    # Since data scraping is now handled by a separate service, 
    # this endpoint should be updated or removed
    return {
        "status": "info", 
        "message": "Data scraping is now handled by a separate service. This endpoint is deprecated."
    }