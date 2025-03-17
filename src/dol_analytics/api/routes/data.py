from datetime import date, timedelta
from typing import Dict, Any, Optional, List
from fastapi import APIRouter, Depends, Query, HTTPException
import psycopg2
import psycopg2.extras

# Use relative imports if running as a module
try:
    from ...models.database import get_postgres_connection
    from ...models.schemas import DashboardData, DailyVolumeData, WeeklyAverageData, WeeklyVolumeData, MonthlyVolumeData, TodaysProgressData, MonthlyBacklogData
except ImportError:
    # Use absolute imports if running as a script
    from src.dol_analytics.models.database import get_postgres_connection
    from src.dol_analytics.models.schemas import DashboardData, DailyVolumeData, WeeklyAverageData, WeeklyVolumeData, MonthlyVolumeData, TodaysProgressData, MonthlyBacklogData

router = APIRouter(prefix="/data", tags=["data"])


@router.get("/dashboard")
async def get_dashboard_data(
    days: int = Query(30, ge=1, le=365, description="Number of days to include in data"),
    metrics_period: Optional[str] = Query(None, description="Time period for metrics comparison ('day', 'week', or 'month')"),
    conn=Depends(get_postgres_connection)
):
    """
    Get dashboard visualization data in the format expected by the frontend.
    """
    # Get start date based on number of days
    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    
    # Determine metrics period based on days if not explicitly provided
    if metrics_period is None:
        if days <= 7:
            metrics_period = "day"
        elif days <= 30:
            metrics_period = "week"
        else:
            metrics_period = "month"
    
    # Get data using existing helper functions
    daily_volume_data = get_daily_volume_data(conn, start_date, end_date)
    weekly_averages_data = get_weekly_averages_data(conn, start_date, end_date)
    weekly_volumes_data = get_weekly_volumes_data(conn, start_date, end_date)
    
    # Get monthly volumes (for past 2 months)
    two_months_ago = (end_date.replace(day=1) - timedelta(days=1)).replace(day=1)
    monthly_volumes_data = get_monthly_volumes_data(conn, two_months_ago, end_date)
    
    # Get today's progress with appropriate comparison period
    todays_progress = get_todays_progress_data(conn, metrics_period)
    
    # Get current backlog from summary_stats
    current_backlog = get_current_backlog(conn)
    
    # Get processing time metrics
    processing_times = get_latest_processing_times(conn)
    
    # Get ALL monthly backlog data (not just 12 months)
    # Go back to at least 2023
    backlog_start_date = date(2023, 1, 1)
    monthly_backlog_data = get_monthly_backlog_data(conn, backlog_start_date, end_date)
    
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
    
    formatted_monthly_backlog = [
        {
            "month": f"{item.month} {item.year}", 
            "backlog": item.backlog,
            "is_active": item.is_active,
            "withdrawn": item.withdrawn
        }
        for item in monthly_backlog_data
    ]
    
    # Combine today's progress with current backlog and processing times to create metrics object
    metrics = {
        "new_cases": todays_progress.new_cases,
        "new_cases_change": todays_progress.new_cases_change,
        "processed_cases": todays_progress.processed_cases,
        "processed_cases_change": todays_progress.processed_cases_change,
        "current_backlog": current_backlog,
        "processing_times": processing_times
    }
    
    # Return in the format expected by frontend
    return {
        "daily_volume": formatted_daily_volume,
        "weekly_averages": formatted_weekly_averages,
        "weekly_volumes": formatted_weekly_volumes,
        "monthly_volumes": formatted_monthly_volumes,
        "monthly_backlog": formatted_monthly_backlog,
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


@router.get("/monthly-backlog")
async def get_monthly_backlog(
    months: int = Query(12, ge=1, le=36, description="Number of months to include"),
    conn=Depends(get_postgres_connection)
):
    """Get monthly backlog data showing ANALYST REVIEW cases."""
    today = date.today()
    
    # Calculate start date based on number of months
    end_date = today
    start_date = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
    
    # Go back additional months
    for _ in range(months - 1):
        start_date = (start_date.replace(day=1) - timedelta(days=1)).replace(day=1)
    
    backlog_data = get_monthly_backlog_data(conn, start_date, end_date)
    
    return {"data": backlog_data}


@router.get("/processing-times")
async def get_processing_times(
    conn=Depends(get_postgres_connection)
):
    """Get the latest processing time metrics."""
    processing_times = get_latest_processing_times(conn)
    
    return processing_times


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


def get_todays_progress_data(conn, period: str = "day") -> TodaysProgressData:
    """
    Get progress metrics with comparison to previous period.
    
    Args:
        conn: Database connection
        period: Comparison period - 'day', 'week', or 'month'
    """
    try:
        today = date.today()
        
        # Determine comparison date based on period
        if period.lower() == "week":
            comparison_date = today - timedelta(days=7)
            period_name = "week"
        elif period.lower() == "month":
            comparison_date = today - timedelta(days=30)
            period_name = "month"
        else:  # Default to day
            comparison_date = today - timedelta(days=1)
            period_name = "day"
        
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # For current period, we sum data from comparison_date+1 to today
            cursor.execute("""
                SELECT 
                    SUM(changes_today) as new_cases, 
                    SUM(completed_today) as processed_cases
                FROM summary_stats
                WHERE record_date > %s AND record_date <= %s
            """, (comparison_date, today))
            
            current_period_row = cursor.fetchone()
            
            # For previous period, we sum data from previous_start to comparison_date
            previous_start = comparison_date - timedelta(days=(today - comparison_date).days)
            
            cursor.execute("""
                SELECT 
                    SUM(changes_today) as new_cases, 
                    SUM(completed_today) as processed_cases
                FROM summary_stats
                WHERE record_date > %s AND record_date <= %s
            """, (previous_start, comparison_date))
            
            previous_period_row = cursor.fetchone()
            
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
            
            if current_period_row:
                new_cases = current_period_row['new_cases'] or 0
                processed_cases = current_period_row['processed_cases'] or 0
            
            if previous_period_row and current_period_row:
                previous_new = previous_period_row['new_cases'] or 0
                previous_processed = previous_period_row['processed_cases'] or 0
                
                if previous_new > 0:
                    new_cases_change = ((new_cases - previous_new) / previous_new) * 100
                
                if previous_processed > 0:
                    processed_cases_change = ((processed_cases - previous_processed) / previous_processed) * 100
            
            if backlog_row:
                current_backlog = backlog_row['backlog'] or 0
            
            return TodaysProgressData(
                new_cases=int(new_cases),
                processed_cases=int(processed_cases),
                new_cases_change=new_cases_change,
                processed_cases_change=processed_cases_change,
                date=today,
                current_backlog=current_backlog,
                comparison_period=period_name  # Add comparison period to the response
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
            current_backlog=0,
            comparison_period="day"  # Default period
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


def get_monthly_backlog_data(conn, start_date: date, end_date: date) -> List[MonthlyBacklogData]:
    """Query monthly_status table for ANALYST REVIEW cases by month."""
    try:
        # Month name to number mapping
        month_to_num = {
            'January': 1, 'February': 2, 'March': 3, 'April': 4,
            'May': 5, 'June': 6, 'July': 7, 'August': 8,
            'September': 9, 'October': 10, 'November': 11, 'December': 12
        }
        
        result_dict = {}
        
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Get ANALYST REVIEW cases and the is_active flag in one query
            cursor.execute("""
                SELECT 
                    ms.year, 
                    ms.month, 
                    ms.count AS count, 
                    'ANALYST REVIEW' AS status,
                    COALESCE(ms2.is_active, FALSE) AS is_active
                FROM monthly_status ms
                LEFT JOIN monthly_summary ms2 
                    ON ms.year = ms2.year AND ms.month = ms2.month
                WHERE ms.status = 'ANALYST REVIEW'
                
                UNION ALL
                
                SELECT 
                    year, 
                    month, 
                    count, 
                    'WITHDRAWN' AS status,
                    FALSE AS is_active
                FROM monthly_status
                WHERE status = 'WITHDRAWN'
                
                ORDER BY year, month
            """)
            
            # Process results into a dictionary keyed by (year, month)
            for row in cursor.fetchall():
                year = row['year']
                month = row['month']
                key = (year, month)
                month_num = month_to_num.get(month, 0)
                
                # Skip months outside our date range
                row_date = date(year, month_num, 1)
                if row_date < start_date or row_date > end_date:
                    continue
                
                # Initialize the record if we haven't seen this month yet
                if key not in result_dict:
                    result_dict[key] = {
                        'year': year,
                        'month': month,
                        'backlog': 0,
                        'is_active': False,
                        'withdrawn': 0
                    }
                
                # Update the appropriate field based on the status
                if row['status'] == 'ANALYST REVIEW':
                    result_dict[key]['backlog'] = row['count']
                    result_dict[key]['is_active'] = row['is_active']
                elif row['status'] == 'WITHDRAWN':
                    result_dict[key]['withdrawn'] = row['count']
        
        # Convert dictionary to sorted list of MonthlyBacklogData objects
        sorted_keys = sorted(result_dict.keys(), key=lambda k: (k[0], month_to_num[k[1]]))
        result = []
        for key in sorted_keys:
            data = result_dict[key]
            result.append(MonthlyBacklogData(
                month=data['month'],
                year=data['year'],
                backlog=data['backlog'],
                is_active=data['is_active'],
                withdrawn=data['withdrawn']
            ))
        
        return result
    except Exception as e:
        print(f"Error in get_monthly_backlog_data: {str(e)}")
        return []


def get_latest_processing_times(conn) -> Dict[str, Any]:
    """Query processing_times table for latest processing metrics."""
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("""
                SELECT 
                    percentile_30 as lower_estimate_days,
                    percentile_50 as median_days,
                    percentile_80 as upper_estimate_days,
                    record_date
                FROM processing_times
                ORDER BY record_date DESC
                LIMIT 1
            """)
            
            row = cursor.fetchone()
            if row:
                return {
                    "lower_estimate_days": int(row['lower_estimate_days']) if row['lower_estimate_days'] is not None else None,
                    "median_days": int(row['median_days']) if row['median_days'] is not None else None,
                    "upper_estimate_days": int(row['upper_estimate_days']) if row['upper_estimate_days'] is not None else None,
                    "as_of_date": row['record_date'].isoformat() if row['record_date'] else None
                }
            return {
                "lower_estimate_days": None,
                "median_days": None,
                "upper_estimate_days": None,
                "as_of_date": None
            }
    except Exception as e:
        print(f"Error in get_latest_processing_times: {str(e)}")
        return {
            "lower_estimate_days": None,
            "median_days": None, 
            "upper_estimate_days": None,
            "as_of_date": None
        }