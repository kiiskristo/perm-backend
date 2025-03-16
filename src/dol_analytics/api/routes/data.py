from datetime import date, timedelta
from typing import Dict, Any, Optional, List
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
import psycopg2
import psycopg2.extras

# Use relative imports if running as a module
try:
    from ...models.database import get_postgres_connection
    from ...models.schemas import DashboardData, DailyVolumeData, WeeklyAverageData, WeeklyVolumeData, MonthlyVolumeData, TodaysProgressData
except ImportError:
    # Use absolute imports if running as a script
    from src.dol_analytics.models.database import get_postgres_connection
    from src.dol_analytics.models.schemas import DashboardData, DailyVolumeData, WeeklyAverageData, WeeklyVolumeData, MonthlyVolumeData, TodaysProgressData

router = APIRouter(prefix="/data", tags=["data"])


@router.get("/dashboard")
async def get_dashboard_data(
    days: int = Query(30, ge=1, le=365, description="Number of days to include in data"),
    conn=Depends(get_postgres_connection)
):
    """
    Get dashboard visualization data in the format expected by the frontend.
    """
    # Get start date based on number of days
    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    
    # Get data using existing helper functions
    daily_volume_data = get_daily_volume_data(conn, start_date, end_date)
    weekly_averages_data = get_weekly_averages_data(conn, start_date, end_date)
    weekly_volumes_data = get_weekly_volumes_data(conn, start_date, end_date)
    
    # Get monthly volumes (for past 2 months)
    two_months_ago = (end_date.replace(day=1) - timedelta(days=1)).replace(day=1)
    monthly_volumes_data = get_monthly_volumes_data(conn, two_months_ago, end_date)
    
    # Get today's progress
    todays_progress = get_todays_progress_data(conn)
    
    # Get current backlog from summary_stats
    current_backlog = get_current_backlog(conn)
    
    # Transform data to match frontend expectations
    formatted_daily_volume = [
        {"date": item.date.isoformat(), "volume": item.count}
        for item in daily_volume_data
    ]
    
    formatted_weekly_averages = [
        {"day": item.day_of_week, "average": item.average_volume}
        for item in weekly_averages_data
    ]
    
    formatted_weekly_volumes = [
        {"week": item.week_starting.isoformat(), "volume": item.total_volume}
        for item in weekly_volumes_data
    ]
    
    formatted_monthly_volumes = [
        {"month": f"{item.month} {item.year}", "volume": item.total_volume}
        for item in monthly_volumes_data
    ]
    
    # Combine today's progress with current backlog to create metrics object
    metrics = {
        "new_cases": todays_progress.new_cases,
        "new_cases_change": todays_progress.new_cases_change,
        "processed_cases": todays_progress.processed_cases,
        "processed_cases_change": todays_progress.processed_cases_change,
        "current_backlog": current_backlog
    }
    
    # Return in the format expected by frontend
    return {
        "daily_volume": formatted_daily_volume,
        "weekly_averages": formatted_weekly_averages,
        "weekly_volumes": formatted_weekly_volumes,
        "monthly_volumes": formatted_monthly_volumes,
        "metrics": metrics
    }


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
    """Query daily_progress view for volume data."""
    try:
        result = []
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Use the known column name directly
            cursor.execute("""
                SELECT date, total_applications as volume
                FROM daily_progress
                WHERE date BETWEEN %s AND %s
                ORDER BY date
            """, (start_date, end_date))
            
            for row in cursor.fetchall():
                result.append(DailyVolumeData(
                    date=row['date'],
                    count=row['volume']
                ))
        
        return result
    except Exception as e:
        print(f"Error in get_daily_volume_data: {str(e)}")
        # Return empty list on error
        return []


def get_weekly_averages_data(conn, start_date: date, end_date: date) -> List[WeeklyAverageData]:
    """Query daily_progress table for weekly averages by day of week."""
    try:
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
    except Exception as e:
        print(f"Error in get_weekly_averages_data: {str(e)}")
        # Return empty list on error
        return []


def get_weekly_volumes_data(conn, start_date: date, end_date: date) -> List[WeeklyVolumeData]:
    """Query weekly_summary view for weekly volume data."""
    try:
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
    except Exception as e:
        print(f"Error in get_weekly_volumes_data: {str(e)}")
        # Return empty list on error
        return []


def get_monthly_volumes_data(conn, start_date: date, end_date: date) -> List[MonthlyVolumeData]:
    """Query monthly_summary view for monthly volume data."""
    try:
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
            # First approach - use a simpler query that just filters on the year
            # This works well if we're just getting recent months
            cursor.execute("""
                SELECT year, month, total_count as total_volume
                FROM monthly_summary
                WHERE year BETWEEN %s AND %s
                ORDER BY year, 
                CASE month
                    WHEN 'January' THEN 1
                    WHEN 'February' THEN 2
                    WHEN 'March' THEN 3
                    WHEN 'April' THEN 4
                    WHEN 'May' THEN 5
                    WHEN 'June' THEN 6
                    WHEN 'July' THEN 7
                    WHEN 'August' THEN 8
                    WHEN 'September' THEN 9
                    WHEN 'October' THEN 10
                    WHEN 'November' THEN 11
                    WHEN 'December' THEN 12
                END
            """, (start_year, end_year))
            
            result = []
            for row in cursor.fetchall():
                # Filter out months that are outside our range
                month_num = month_names.index(row['month']) + 1
                
                if (row['year'] == start_year and month_num < start_month) or \
                   (row['year'] == end_year and month_num > end_month):
                    continue
                    
                result.append(MonthlyVolumeData(
                    month=row['month'],
                    year=row['year'],
                    total_volume=row['total_volume']
                ))
            
            return result
    except Exception as e:
        print(f"Error in get_monthly_volumes_data: {str(e)}")
        # Return empty list on error
        return []


def get_todays_progress_data(conn) -> TodaysProgressData:
    """Get today's progress metrics from summary_stats."""
    try:
        today = date.today()
        yesterday = today - timedelta(days=1)
        
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Get today's stats
            cursor.execute("""
                SELECT changes_today as new_cases, completed_today as processed_cases
                FROM summary_stats
                WHERE record_date = %s
            """, (today,))
            
            today_row = cursor.fetchone()
            
            # Get yesterday's stats
            cursor.execute("""
                SELECT changes_today as new_cases, completed_today as processed_cases
                FROM summary_stats
                WHERE record_date = %s
            """, (yesterday,))
            
            yesterday_row = cursor.fetchone()
            
            # Get current backlog
            cursor.execute("""
                SELECT pending_applications as backlog
                FROM summary_stats
                ORDER BY record_date DESC
                LIMIT 1
            """)
            
            backlog_row = cursor.fetchone()
            
            # Default values
            new_cases = 0
            processed_cases = 0
            new_cases_change = 0
            processed_cases_change = 0
            current_backlog = 0
            
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
            
            if backlog_row:
                current_backlog = backlog_row['backlog'] or 0
            
            return TodaysProgressData(
                new_cases=new_cases,
                processed_cases=processed_cases,
                new_cases_change=new_cases_change,
                processed_cases_change=processed_cases_change,
                date=today,
                current_backlog=current_backlog
            )
    except Exception as e:
        print(f"Error in get_todays_progress_data: {str(e)}")
        # Return default data on error
        return TodaysProgressData(
            new_cases=0,
            processed_cases=0,
            new_cases_change=0,
            processed_cases_change=0,
            date=date.today(),
            current_backlog=0
        )


def get_current_backlog(conn) -> int:
    """Query summary_stats table for current backlog."""
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                SELECT pending_applications
                FROM summary_stats
                ORDER BY record_date DESC
                LIMIT 1
            """)
            
            row = cursor.fetchone()
            return row[0] if row else 0
    except Exception as e:
        print(f"Error in get_current_backlog: {str(e)}")
        return 0


@router.post("/refresh")
async def refresh_data():
    """Manually trigger data refresh from DOL API."""
    # Since data scraping is now handled by a separate service, 
    # this endpoint should be updated or removed
    return {
        "status": "info", 
        "message": "Data scraping is now handled by a separate service. This endpoint is deprecated."
    }


@router.get("/test-connection")
async def test_connection(conn=Depends(get_postgres_connection)):
    """Test PostgreSQL connection."""
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            
            # Also test one of your actual tables
            cursor.execute("SELECT COUNT(*) FROM daily_progress;")
            count = cursor.fetchone()[0]
            
            return {
                "status": "success",
                "postgres_version": version,
                "daily_progress_count": count
            }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


@router.get("/inspect-schema", include_in_schema=False)  # Hide from API docs
async def inspect_schema(conn=Depends(get_postgres_connection)):
    """
    Database schema inspection endpoint (for development only).
    
    Note: This endpoint is deprecated since we now have comprehensive 
    database documentation in database_docs.py
    """
    from src.dol_analytics.models.database_docs import get_schema_overview, get_table_docs
    
    # Return our documentation instead of inspecting live schema
    tables = ['daily_progress', 'monthly_status', 'processing_times', 
              'summary_stats', 'weekly_summary', 'monthly_summary']
    
    docs = {table: get_table_docs(table) for table in tables}
    
    return {
        "schema_overview": get_schema_overview(),
        "table_documentation": docs
    }