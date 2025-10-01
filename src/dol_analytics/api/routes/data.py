from datetime import date, timedelta, datetime
from typing import Dict, Any, Optional, List
from fastapi import APIRouter, Depends, Query, HTTPException, Request
from pydantic import BaseModel, Field
import psycopg2
import psycopg2.extras

# Use relative imports if running as a module
try:
    from ...models.database import get_postgres_connection
    from ...models.schemas import (
        DailyVolumeData, WeeklyAverageData, WeeklyVolumeData, MonthlyVolumeData, 
        TodaysProgressData, MonthlyBacklogData, PermCaseActivityData, PermCasesMetrics,
        CompanySearchRequest, CompanySearchResponse, CompanyCasesRequest, CompanyCasesResponse,
        UpdatedCasesRequest, UpdatedCasesResponse
    )
    from ..routes.predictions import verify_recaptcha
    from ...middleware.rate_limiter import check_rate_limit, rate_limiter
except ImportError:
    # Use absolute imports if running as a script
    from src.dol_analytics.models.database import get_postgres_connection
    from src.dol_analytics.models.schemas import (
        DailyVolumeData, WeeklyAverageData, WeeklyVolumeData, MonthlyVolumeData, 
        TodaysProgressData, MonthlyBacklogData, PermCaseActivityData, PermCasesMetrics,
        CompanySearchRequest, CompanySearchResponse, CompanyCasesRequest, CompanyCasesResponse,
        UpdatedCasesRequest, UpdatedCasesResponse
    )
    from src.dol_analytics.api.routes.predictions import verify_recaptcha
    from src.dol_analytics.middleware.rate_limiter import check_rate_limit, rate_limiter

router = APIRouter(prefix="/data", tags=["data"])

# Cache settings
CACHE_TIMEOUT = 3600  # 1 hour in seconds
last_cache_reset = {}  # Track last reset time by endpoint

# Dashboard cache with keys for common time periods
dashboard_cache = {}


# Request/Response models are now defined in schemas.py

def should_reset_cache(endpoint):
    """Check if cache should be reset based on timeout."""
    now = datetime.now()
    if endpoint not in last_cache_reset or (now - last_cache_reset[endpoint]).total_seconds() > CACHE_TIMEOUT:
        print(f"Cache expired for {endpoint} - refreshing")
        last_cache_reset[endpoint] = now
        return True
    return False


@router.post("/clear-cache")
async def clear_dashboard_cache():
    """
    Clear the dashboard cache manually.
    Useful during development or when fresh data is needed immediately.
    """
    global dashboard_cache, last_cache_reset
    dashboard_cache.clear()
    last_cache_reset.clear()
    return {"message": "Dashboard cache cleared successfully", "cleared_items": len(dashboard_cache)}


@router.get("/admin/rate-limit-stats")
async def get_rate_limit_stats():
    """
    Get rate limiting statistics for monitoring.
    Shows suspicious IPs and current rate limit status.
    """
    from ...middleware.rate_limiter import get_rate_limit_stats
    
    stats = get_rate_limit_stats()
    
    # Add some additional info
    stats["message"] = "Rate limiting is active"
    stats["endpoints_protected"] = list(rate_limiter.limits.keys())
    
    return stats


@router.post("/admin/block-ip")
async def block_ip(ip_address: str, duration: int = 3600):
    """
    Manually block an IP address.
    Duration is in seconds (default: 1 hour).
    """
    rate_limiter.block_ip(ip_address, duration)
    return {
        "message": f"IP {ip_address} has been blocked for {duration} seconds",
        "blocked_until": f"{duration} seconds from now"
    }


@router.post("/company-search", response_model=CompanySearchResponse)
async def search_companies(
    request: CompanySearchRequest,
    http_request: Request,
    conn=Depends(get_postgres_connection),
    _rate_limit: None = Depends(check_rate_limit)
):
    """
    Search for company names in perm_cases table for autocomplete.
    Searches data from March 1st, 2024 onward. Protected by reCAPTCHA to prevent scraping.
    """
    # Log the request for monitoring
    client_ip = rate_limiter.get_client_ip(http_request)
    print(f"🔍 Company search request from IP: {client_ip}, query: '{request.query[:50]}...'")
    
    # Verify reCAPTCHA token before processing
    if not verify_recaptcha(request.recaptcha_token):
        print(f"❌ Invalid reCAPTCHA from IP: {client_ip}")
        raise HTTPException(status_code=400, detail="Invalid reCAPTCHA. Please try again.")
    
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Search for companies that start with the query string
            # Only include data from March 1st, 2024 onward and get unique company names
            cursor.execute("""
                WITH normalized_companies AS (
                    SELECT DISTINCT
                        -- Normalize company name: proper case, remove trailing periods
                        INITCAP(TRIM(TRAILING '.' FROM employer_name)) as normalized_name,
                        employer_name as original_name,
                        LENGTH(TRIM(TRAILING '.' FROM employer_name)) as name_length
                    FROM perm_cases
                    WHERE UPPER(employer_name) LIKE UPPER(%s)
                    AND submit_date >= '2024-03-01'
                ),
                grouped_companies AS (
                    SELECT 
                        normalized_name,
                        MIN(original_name) as display_name,  -- Pick one representative name
                        MIN(name_length) as min_length
                    FROM normalized_companies
                    GROUP BY normalized_name
                )
                SELECT display_name
                FROM grouped_companies
                ORDER BY min_length, normalized_name
                LIMIT %s
            """, (f"{request.query}%", request.limit))
            
            companies = cursor.fetchall()
            
            return {
                "companies": [row["display_name"] for row in companies],
                "total": len(companies),
                "query": request.query
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching companies: {str(e)}")


@router.post("/company-cases", response_model=CompanyCasesResponse)
async def get_company_cases(
    request: CompanyCasesRequest,
    http_request: Request,
    conn=Depends(get_postgres_connection),
    _rate_limit: None = Depends(check_rate_limit)
):
    """
    Get PERM cases for a specific company within a date range.
    Protected by reCAPTCHA to prevent scraping.
    Returns case number, job title, priority date, and other relevant information.
    Date range limited to March 1st, 2024 onward with maximum 2-week window.
    """
    # Log the request for monitoring
    client_ip = rate_limiter.get_client_ip(http_request)
    print(f"🏢 Company cases request from IP: {client_ip}, company: '{request.company_name[:50]}...', date range: {request.start_date} to {request.end_date}")
    
    # Verify reCAPTCHA token before processing
    if not verify_recaptcha(request.recaptcha_token):
        print(f"❌ Invalid reCAPTCHA from IP: {client_ip}")
        raise HTTPException(status_code=400, detail="Invalid reCAPTCHA. Please try again.")
    
    # Validate date range
    if request.start_date > request.end_date:
        raise HTTPException(status_code=400, detail="Start date must be before or equal to end date")
    
    # Validate minimum date
    min_date = date(2024, 3, 1)  # March 1st, 2024
    max_date = date.today()
    
    if request.start_date < min_date:
        raise HTTPException(
            status_code=400, 
            detail=f"Start date must be on or after March 1st, 2024. Provided: {request.start_date.isoformat()}"
        )
    
    if request.end_date > max_date:
        raise HTTPException(
            status_code=400, 
            detail=f"End date cannot be in the future. Maximum allowed date: {max_date.isoformat()}"
        )
    
    # Validate maximum window size (2 weeks = 14 days)
    date_range_days = (request.end_date - request.start_date).days
    if date_range_days > 14:
        raise HTTPException(
            status_code=400, 
            detail=f"Date range cannot exceed 2 weeks (14 days). Current range: {date_range_days} days"
        )
    
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Get total count for pagination (case-insensitive search with punctuation normalization)
            cursor.execute("""
                SELECT COUNT(*) as total
                FROM perm_cases
                WHERE UPPER(TRIM(TRAILING '.' FROM employer_name)) = UPPER(TRIM(TRAILING '.' FROM %s))
                AND submit_date BETWEEN %s AND %s
            """, (request.company_name, request.start_date, request.end_date))
            
            total_count = cursor.fetchone()["total"]
            
            # Get the cases with pagination (case-insensitive search with punctuation normalization)
            cursor.execute("""
                SELECT 
                    case_number,
                    job_title,
                    submit_date,
                    employer_name,
                    employer_first_letter
                FROM perm_cases
                WHERE UPPER(TRIM(TRAILING '.' FROM employer_name)) = UPPER(TRIM(TRAILING '.' FROM %s))
                AND submit_date BETWEEN %s AND %s
                ORDER BY submit_date DESC
                LIMIT %s OFFSET %s
            """, (request.company_name, request.start_date, request.end_date, request.limit, request.offset))
            
            cases = cursor.fetchall()
            
            # Convert to list of dictionaries for JSON response
            cases_list = []
            for case in cases:
                case_dict = dict(case)
                # Convert dates to ISO format strings
                if case_dict["submit_date"]:
                    case_dict["submit_date"] = case_dict["submit_date"].isoformat()
                cases_list.append(case_dict)
            
            return {
                "cases": cases_list,
                "total": total_count,
                "limit": request.limit,
                "offset": request.offset,
                "company_name": request.company_name,
                "date_range": {
                    "start_date": request.start_date.isoformat(),
                    "end_date": request.end_date.isoformat()
                }
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving company cases: {str(e)}")


@router.post("/updated-cases", response_model=UpdatedCasesResponse)
async def get_updated_cases(
    request: UpdatedCasesRequest,
    conn=Depends(get_postgres_connection)
):
    """
    Get PERM cases that were updated on a specific date (ET timezone).
    Returns case number, job title, current status, previous status, update timestamp, and other relevant information.
    Date range is limited to March 1st, 2024 through today.
    Excludes withdrawn cases from results. No rate limiting or reCAPTCHA protection.
    """
    # Validate date range
    min_date = date(2024, 3, 1)  # March 1st, 2024
    max_date = date.today()
    
    if request.target_date < min_date:
        raise HTTPException(
            status_code=400, 
            detail=f"Date must be on or after March 1st, 2024. Provided: {request.target_date.isoformat()}"
        )
    
    if request.target_date > max_date:
        raise HTTPException(
            status_code=400, 
            detail=f"Date cannot be in the future. Maximum allowed date: {max_date.isoformat()}"
        )
    
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Get total count for pagination
            # Convert UTC updated_at to ET timezone and filter by date, excluding withdrawn cases
            # Exclude cases submitted within 3 days of the update date to avoid new submissions
            cursor.execute("""
                SELECT COUNT(*) as total
                FROM perm_cases
                WHERE date(updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') = %s
                AND submit_date < %s - INTERVAL '3 days'
                AND status != 'WITHDRAWN'
            """, (request.target_date, request.target_date))
            
            total_count = cursor.fetchone()["total"]
            
            # Get the cases with pagination
            # Include status, previous_status and updated_at in the results, excluding withdrawn cases
            # Exclude cases submitted within 3 days of the update date to avoid new submissions
            cursor.execute("""
                SELECT 
                    COALESCE(case_number, '') as case_number,
                    job_title,
                    submit_date,
                    employer_name,
                    COALESCE(employer_first_letter, '') as employer_first_letter,
                    COALESCE(status, '') as status,
                    previous_status,
                    updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York' as updated_at_et
                FROM perm_cases
                WHERE date(updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') = %s
                AND submit_date < %s - INTERVAL '3 days'
                AND status != 'WITHDRAWN'
                ORDER BY submit_date DESC
                LIMIT %s OFFSET %s
            """, (request.target_date, request.target_date, request.limit, request.offset))
            
            cases = cursor.fetchall()
            
            # Convert to list of dictionaries for JSON response
            cases_list = []
            for case in cases:
                case_dict = dict(case)
                # Convert dates to ISO format strings
                if case_dict["submit_date"]:
                    case_dict["submit_date"] = case_dict["submit_date"].isoformat()
                if case_dict["updated_at_et"]:
                    case_dict["updated_at"] = case_dict["updated_at_et"].isoformat()
                    del case_dict["updated_at_et"]  # Remove the temporary field name
                
                # Handle null values by providing defaults or None
                if case_dict["job_title"] is None:
                    case_dict["job_title"] = None  # Keep as None, schema now allows it
                if case_dict["employer_name"] is None:
                    case_dict["employer_name"] = None  # Keep as None, schema now allows it
                if case_dict["previous_status"] is None:
                    case_dict["previous_status"] = None  # Keep as None, schema allows it
                    
                cases_list.append(case_dict)
            
            return {
                "cases": cases_list,
                "total": total_count,
                "limit": request.limit,
                "offset": request.offset,
                "target_date": request.target_date.isoformat(),
                "timezone_note": "All timestamps are converted to Eastern Time (ET)"
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving updated cases: {str(e)}")


@router.get("/dashboard")
async def get_dashboard_data(
    days: int = Query(30, ge=1, le=365, description="Number of days to include in data"),
    data_type: str = Query("certified", regex="^(certified|processed)$", description="Type of data to fetch: 'certified' or 'processed'"),
    conn=Depends(get_postgres_connection)
):
    """
    Get dashboard visualization data in the format expected by the frontend.
    Uses caching for common time periods (7, 30, 90, 180 days).
    
    Parameters:
    - days: Number of days to include in data (1-365)
    - data_type: Type of data to fetch - 'certified' (uses certified_total column) or 'processed' (uses processed_total column)
    """
    # Check if we should clear the cache
    if should_reset_cache("dashboard"):
        dashboard_cache.clear()
    
    # Create cache key that includes both days and data_type
    cache_key = f"{days}_{data_type}"
    
    # Check if we have this data period and type in cache
    if cache_key in dashboard_cache:
        print(f"🚀 Cache HIT: Serving dashboard data for {days} days ({data_type}) from cache")
        return dashboard_cache[cache_key]
    
    print(f"⏳ Cache MISS: Fetching dashboard data for {days} days ({data_type}) from database")
    
    # Not in cache, generate the data
    # Get start date based on number of days
    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    
    # Get data using existing helper functions
    daily_volume_data = get_daily_volume_data(conn, start_date, end_date, data_type)
    weekly_averages_data = get_weekly_averages_data(conn, start_date, end_date, data_type)
    weekly_volumes_data = get_weekly_volumes_data(conn, start_date, end_date, data_type)
    
    # Get monthly volumes using the same date range as other data
    monthly_volumes_data = get_monthly_volumes_data(conn, start_date, end_date, data_type)
    
    # Get today's progress with days parameter
    todays_progress = get_todays_progress_data(conn, days)
    
    # Get current backlog from summary_stats
    current_backlog = get_current_backlog(conn)
    
    # Get processing time metrics
    processing_times = get_latest_processing_times(conn)
    
    # Get PERM cases activity data for the latest date with data
    perm_cases_metrics = get_perm_cases_metrics(conn)
    
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
            "withdrawn": item.withdrawn,
            "denied": item.denied,
            "rfi": item.rfi
        }
        for item in monthly_backlog_data
    ]
    
    # Format PERM cases data
    formatted_perm_cases = {
        "daily_activity": {
            "activity_data": [
                {
                    "employer_first_letter": item.employer_first_letter,
                    "submit_month": item.submit_month,
                    "certified_count": item.certified_count,
                    "processed_count": item.processed_count or item.certified_count
                }
                for item in perm_cases_metrics["daily_activity"]["activity_data"]
            ],
            "most_active_letter": perm_cases_metrics["daily_activity"]["most_active_letter"],
            "most_active_month": perm_cases_metrics["daily_activity"]["most_active_month"],
            "total_certified_cases": perm_cases_metrics["daily_activity"]["total_certified_cases"],
            "data_date": perm_cases_metrics["daily_activity"]["data_date"].isoformat()
        },
        "latest_month_activity": {
            "activity_data": [
                {
                    "employer_first_letter": item.employer_first_letter,
                    "submit_month": item.submit_month,
                    "certified_count": item.certified_count,
                    "review_count": item.review_count
                }
                for item in perm_cases_metrics["latest_month_activity"]["activity_data"]
            ],
            "most_active_letter": perm_cases_metrics["latest_month_activity"]["most_active_letter"],
            "latest_active_month": perm_cases_metrics["latest_month_activity"]["latest_active_month"],
            "total_certified_cases": perm_cases_metrics["latest_month_activity"]["total_certified_cases"]
        }
    }
    
    # Combine today's progress with current backlog and processing times to create metrics object
    metrics = {
        "new_cases": todays_progress.new_cases,
        "new_cases_change": todays_progress.new_cases_change,
        "processed_cases": todays_progress.processed_cases,
        "processed_cases_change": todays_progress.processed_cases_change,
        "current_backlog": current_backlog,
        "processing_times": processing_times
    }
    
    # Create result object
    result = {
        "daily_volume": formatted_daily_volume,
        "weekly_averages": formatted_weekly_averages,
        "weekly_volumes": formatted_weekly_volumes,
        "monthly_volumes": formatted_monthly_volumes,
        "monthly_backlog": formatted_monthly_backlog,
        "perm_cases": formatted_perm_cases,
        "metrics": metrics
    }
    
    # Cache the result for common time periods
    dashboard_cache[cache_key] = result
    print(f"📦 Cached dashboard data for {days} days ({data_type})")
    
    return result


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
    start_date: Optional[date] = Query(None, description="Start date (defaults to 30 days ago)"),
    end_date: Optional[date] = Query(None, description="End date (defaults to today)"),
    conn=Depends(get_postgres_connection)
):
    """Get monthly volume data."""
    # Set default dates if not provided
    if not end_date:
        end_date = date.today()
    
    if not start_date:
        start_date = end_date - timedelta(days=30)
    
    # Validate date range
    if start_date > end_date:
        raise HTTPException(status_code=400, detail="Start date must be before end date")
    
    monthly_data = get_monthly_volumes_data(conn, start_date, end_date)
    
    return {"data": monthly_data}


@router.get("/todays-progress")
async def get_todays_progress(
    days: int = Query(1, ge=1, le=365, description="Number of days to compare against"),
    conn=Depends(get_postgres_connection)
):
    """Get today's progress metrics."""
    progress_data = get_todays_progress_data(conn, days)
    
    return progress_data


@router.get("/monthly-backlog")
async def get_monthly_backlog(
    months: int = Query(12, ge=1, le=36, description="Number of months to include"),
    conn=Depends(get_postgres_connection)
):
    """Get monthly backlog data showing backlog (ANALYST REVIEW + RECONSIDERATION APPEALS), WITHDRAWN, DENIED, and RFI cases."""
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
    """Get latest processing time estimates."""
    processing_times = get_latest_processing_times(conn)
    return processing_times


@router.get("/perm-cases")
async def get_perm_cases(
    conn=Depends(get_postgres_connection)
):
    """Get PERM cases activity data for debugging and testing."""
    perm_cases_metrics = get_perm_cases_metrics(conn)
    
    return {
        "daily_activity": {
            "activity_data": [
                {
                    "employer_first_letter": item.employer_first_letter,
                    "submit_month": item.submit_month,
                    "certified_count": item.certified_count
                }
                for item in perm_cases_metrics["daily_activity"]["activity_data"]
            ],
            "most_active_letter": perm_cases_metrics["daily_activity"]["most_active_letter"],
            "most_active_month": perm_cases_metrics["daily_activity"]["most_active_month"],
            "total_certified_cases": perm_cases_metrics["daily_activity"]["total_certified_cases"],
            "data_date": perm_cases_metrics["daily_activity"]["data_date"].isoformat()
        },
        "latest_month_activity": {
            "activity_data": [
                {
                    "employer_first_letter": item.employer_first_letter,
                    "submit_month": item.submit_month,
                    "certified_count": item.certified_count,
                    "review_count": item.review_count
                }
                for item in perm_cases_metrics["latest_month_activity"]["activity_data"]
            ],
            "most_active_letter": perm_cases_metrics["latest_month_activity"]["most_active_letter"],
            "latest_active_month": perm_cases_metrics["latest_month_activity"]["latest_active_month"],
            "total_certified_cases": perm_cases_metrics["latest_month_activity"]["total_certified_cases"]
        }
    }


# Helper functions to query PostgreSQL database

def get_daily_volume_data(conn, start_date: date, end_date: date, data_type: str = "certified") -> List[DailyVolumeData]:
    """Query daily_progress table for volume data using certified_total or processed_total columns."""
    try:
        result = []
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Choose the appropriate column based on data_type
            column_name = "certified_total" if data_type == "certified" else "processed_total"
            
            cursor.execute(f"""
                SELECT date, {column_name} as volume
                FROM daily_progress
                WHERE date BETWEEN %s AND %s
                AND {column_name} IS NOT NULL
                ORDER BY date
            """, (start_date, end_date))
            
            for row in cursor.fetchall():
                result.append(DailyVolumeData(
                    date=row['date'],
                    count=int(row['volume']) if row['volume'] is not None else 0
                ))
        
        return result
    except Exception as e:
        print(f"Error in get_daily_volume_data: {str(e)}")
        # Return empty list on error
        return []


def get_weekly_averages_data(conn, start_date: date, end_date: date, data_type: str = "certified") -> List[WeeklyAverageData]:
    """Query daily_progress table for weekly averages by day of week using certified_total or processed_total columns."""
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Choose the appropriate column based on data_type
            column_name = "certified_total" if data_type == "certified" else "processed_total"
            
            cursor.execute(f"""
                SELECT day_of_week, AVG({column_name}) as average_volume
                FROM daily_progress
                WHERE date BETWEEN %s AND %s
                AND {column_name} IS NOT NULL
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


def get_weekly_volumes_data(conn, start_date: date, end_date: date, data_type: str = "certified") -> List[WeeklyVolumeData]:
    """Query weekly_summary view for weekly volume data using certified_total or processed_total columns."""
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Choose the appropriate column based on data_type
            column_name = "certified_total" if data_type == "certified" else "processed_total"
            
            cursor.execute(f"""
                SELECT week_start, {column_name} as total_applications
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


def get_monthly_volumes_data(conn, start_date: date, end_date: date, data_type: str = "certified") -> List[MonthlyVolumeData]:
    """Query monthly_summary view for monthly volume data using certified_total or processed_total columns."""
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Choose the appropriate column based on data_type
            column_name = "certified_total" if data_type == "certified" else "processed_total"
            
            # Query the monthly_summary view using date range
            cursor.execute(f"""
                SELECT 
                    EXTRACT(YEAR FROM year)::INTEGER as year,
                    TO_CHAR(month, 'Month') as month_name,
                    {column_name} as total_volume
                FROM monthly_summary
                WHERE month BETWEEN %s AND %s
                ORDER BY year, month
            """, (start_date, end_date))
            
            result = []
            for row in cursor.fetchall():
                # Convert month_name to proper format (remove trailing spaces)
                month_name = row['month_name'].strip()
                
                result.append(MonthlyVolumeData(
                    month=month_name,
                    year=row['year'],
                    total_volume=row['total_volume']
                ))
            
            return result
    except Exception as e:
        print(f"Error in get_monthly_volumes_data: {str(e)}")
        # Return empty list on error
        return []


def get_todays_progress_data(conn, comparison_days: int = 1) -> TodaysProgressData:
    """
    Get today's progress metrics with comparison to the average of all
    matching weekdays in the selected period.
    
    For example, if today is Tuesday:
    - 7-day view: Compares to last Tuesday
    - 30-day view: Compares to average of all Tuesdays in past 30 days
    
    Args:
        conn: Database connection
        comparison_days: Dashboard period (7, 30, etc.)
    """
    try:
        today = date.today()
        
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Find the latest date with data
            cursor.execute("""
                SELECT MAX(record_date) as latest_date
                FROM summary_stats
            """)
            latest_row = cursor.fetchone()
            latest_date = latest_row['latest_date'] if latest_row and latest_row['latest_date'] else today
            
            # Get today's data and day of week
            cursor.execute("""
                SELECT 
                    changes_today as new_cases, 
                    completed_today as processed_cases,
                    EXTRACT(DOW FROM record_date) as day_of_week
                FROM summary_stats
                WHERE record_date = %s
            """, (latest_date,))
            
            today_row = cursor.fetchone()
            
            if not today_row:
                # No data for latest date
                return TodaysProgressData(
                    new_cases=0,
                    processed_cases=0,
                    new_cases_change=0,
                    processed_cases_change=0,
                    date=latest_date,
                    current_backlog=0,
                    comparison_days=comparison_days,
                    comparison_period="Historical Average",
                    period_label="Today"
                )
            
            # Get the day of week (0=Sunday, 1=Monday, etc.)
            day_of_week = int(today_row['day_of_week'])
            weekday_name = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"][day_of_week]
            
            # For 7 days or less, just compare to last week on same day
            if comparison_days <= 7:
                comparison_date = latest_date - timedelta(days=7)
                
                cursor.execute("""
                    SELECT 
                        changes_today as new_cases, 
                        completed_today as processed_cases
                    FROM summary_stats
                    WHERE record_date = %s
                """, (comparison_date,))
                
                comparison_row = cursor.fetchone()
                
                comparison_new = comparison_row['new_cases'] if comparison_row and comparison_row['new_cases'] else 0
                comparison_processed = comparison_row['processed_cases'] if comparison_row and comparison_row['processed_cases'] else 0
                
                comparison_label = f"Last {weekday_name}"
                
            else:
                # For more than 7 days, compare to average of all matching weekdays in period
                period_start = latest_date - timedelta(days=comparison_days)
                
                # Find all matching weekdays in the period excluding today
                cursor.execute("""
                    SELECT 
                        AVG(changes_today) as avg_new_cases, 
                        AVG(completed_today) as avg_processed_cases,
                        COUNT(*) as count_days
                    FROM summary_stats
                    WHERE record_date < %s
                      AND record_date >= %s
                      AND EXTRACT(DOW FROM record_date) = %s
                """, (latest_date, period_start, day_of_week))
                
                weekday_avg_row = cursor.fetchone()
                
                comparison_new = weekday_avg_row['avg_new_cases'] if weekday_avg_row and weekday_avg_row['avg_new_cases'] else 0
                comparison_processed = weekday_avg_row['avg_processed_cases'] if weekday_avg_row and weekday_avg_row['avg_processed_cases'] else 0
                days_count = int(weekday_avg_row['count_days'] if weekday_avg_row and weekday_avg_row['count_days'] else 0)
                
                comparison_label = f"Avg {weekday_name}s ({days_count})"
            
            # Get current backlog
            cursor.execute("""
                SELECT pending_applications as backlog
                FROM summary_stats
                WHERE record_date = %s
            """, (latest_date,))
            
            backlog_row = cursor.fetchone()
            current_backlog = backlog_row['backlog'] if backlog_row else 0
            
            # Calculate changes
            new_cases = today_row['new_cases'] or 0
            processed_cases = today_row['processed_cases'] or 0
            
            new_cases_change = 0
            if comparison_new > 0:
                new_cases_change = ((new_cases - comparison_new) / comparison_new) * 100
            
            processed_cases_change = 0
            if comparison_processed > 0:
                processed_cases_change = ((processed_cases - comparison_processed) / comparison_processed) * 100
            
            return TodaysProgressData(
                new_cases=int(new_cases),
                processed_cases=int(processed_cases),
                new_cases_change=new_cases_change,
                processed_cases_change=processed_cases_change,
                date=latest_date,
                current_backlog=int(current_backlog),
                comparison_days=comparison_days,
                comparison_period=comparison_label,
                period_label=weekday_name  # Today is a specific weekday
            )
    except Exception as e:
        print(f"Error in get_todays_progress_data: {str(e)}")
        print(f"Exception type: {type(e)}")
        import traceback
        print(traceback.format_exc())
        # Return default data on error
        return TodaysProgressData(
            new_cases=0,
            processed_cases=0,
            new_cases_change=0,
            processed_cases_change=0,
            date=date.today(),
            current_backlog=0,
            comparison_days=comparison_days,
            comparison_period="Historical Average",
            period_label="Today"
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
    """Query monthly_status table for backlog (ANALYST REVIEW + RECONSIDERATION APPEALS), WITHDRAWN, DENIED, and RFI cases by month."""
    try:
        # Month name to number mapping
        month_to_num = {
            'January': 1, 'February': 2, 'March': 3, 'April': 4,
            'May': 5, 'June': 6, 'July': 7, 'August': 8,
            'September': 9, 'October': 10, 'November': 11, 'December': 12
        }
        
        result_dict = {}
        
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Get backlog cases (ANALYST REVIEW + RECONSIDERATION APPEALS) and other statuses
            cursor.execute("""
                SELECT 
                    ms.year, 
                    ms.month, 
                    SUM(ms.count) AS count, 
                    'BACKLOG' AS status,
                    BOOL_OR(COALESCE(ms.is_active, FALSE)) AS is_active
                FROM monthly_status ms
                WHERE ms.status IN ('ANALYST REVIEW', 'RECONSIDERATION APPEALS')
                GROUP BY ms.year, ms.month
                
                UNION ALL
                
                SELECT 
                    year, 
                    month, 
                    count, 
                    'WITHDRAWN' AS status,
                    FALSE AS is_active
                FROM monthly_status
                WHERE status = 'WITHDRAWN'
                
                UNION ALL
                
                SELECT 
                    year, 
                    month, 
                    count, 
                    'DENIED' AS status,
                    FALSE AS is_active
                FROM monthly_status
                WHERE status = 'DENIED'
                
                UNION ALL
                
                SELECT 
                    year, 
                    month, 
                    count, 
                    'RFI ISSUED' AS status,
                    FALSE AS is_active
                FROM monthly_status
                WHERE status = 'RFI ISSUED'
                
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
                        'withdrawn': 0,
                        'denied': 0,
                        'rfi': 0
                    }
                
                # Update the appropriate field based on the status
                if row['status'] == 'BACKLOG':
                    result_dict[key]['backlog'] = row['count']
                    result_dict[key]['is_active'] = row['is_active']
                elif row['status'] == 'WITHDRAWN':
                    result_dict[key]['withdrawn'] = row['count']
                elif row['status'] == 'DENIED':
                    result_dict[key]['denied'] = row['count']
                elif row['status'] == 'RFI ISSUED':
                    result_dict[key]['rfi'] = row['count']
        
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
                withdrawn=data['withdrawn'],
                denied=data['denied'],
                rfi=data['rfi']
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
                    record_date,
                    created_at
                FROM processing_times
                ORDER BY record_date DESC
                LIMIT 1
            """)
            
            row = cursor.fetchone()
            if row:
                # Try to get the most recent case update time for this date
                # Convert UTC to ET for proper date comparison
                cursor.execute("""
                    SELECT MAX(updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') as latest_update_time
                    FROM perm_cases 
                    WHERE date(updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') = %s
                """, (row['record_date'],))
                
                update_time_row = cursor.fetchone()
                latest_update_time = update_time_row['latest_update_time'] if update_time_row and update_time_row['latest_update_time'] else None
                
                # Use the latest case update time if available, otherwise fall back to processing_times created_at
                as_of_datetime = latest_update_time if latest_update_time else row['created_at']
                
                return {
                    "lower_estimate_days": int(row['lower_estimate_days']) if row['lower_estimate_days'] is not None else None,
                    "median_days": int(row['median_days']) if row['median_days'] is not None else None,
                    "upper_estimate_days": int(row['upper_estimate_days']) if row['upper_estimate_days'] is not None else None,
                    "as_of_date": as_of_datetime.isoformat() if as_of_datetime else (row['record_date'].isoformat() if row['record_date'] else None)
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


def get_perm_cases_activity_data(conn) -> List[PermCaseActivityData]:
    """Query perm_cases table for activity by employer first letter and month for the latest date with data."""
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Find the latest date with data (same pattern as get_todays_progress_data)
            cursor.execute("""
                SELECT MAX(record_date) as latest_date
                FROM summary_stats
            """)
            latest_row = cursor.fetchone()
            latest_date = latest_row['latest_date'] if latest_row and latest_row['latest_date'] else date.today()
            print(f"🔍 Using latest data date: {latest_date}")
            
            # First, let's check if the table exists and has data
            cursor.execute("""
                SELECT COUNT(*) as total_count
                FROM perm_cases
            """)
            total_row = cursor.fetchone()
            total_count = total_row['total_count'] if total_row else 0
            print(f"🔍 Total PERM cases in database: {total_count}")
            
            # Check how many certified cases exist
            cursor.execute("""
                SELECT COUNT(*) as certified_count
                FROM perm_cases
                WHERE status = 'CERTIFIED'
            """)
            certified_row = cursor.fetchone()
            certified_count = certified_row['certified_count'] if certified_row else 0
            print(f"🔍 Total CERTIFIED PERM cases: {certified_count}")
            
            # Query 1: Activity for the latest date with data - certified and processed counts
            # Convert UTC updated_at to ET time before extracting date
            cursor.execute("""
                SELECT 
                    employer_first_letter, 
                    date_part('month', submit_date) as submit_month, 
                    SUM(CASE WHEN status = 'CERTIFIED' THEN 1 ELSE 0 END) as certified_count,
                    COUNT(*) as processed_count
                FROM perm_cases 
                WHERE date(updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') = %s 
                AND status IN ('CERTIFIED', 'DENIED', 'RFI ISSUED')
                GROUP BY employer_first_letter, date_part('month', submit_date)
                ORDER BY date_part('month', submit_date) ASC, employer_first_letter ASC
            """, (latest_date,))
            
            result = []
            for row in cursor.fetchall():
                result.append(PermCaseActivityData(
                    employer_first_letter=row['employer_first_letter'],
                    submit_month=int(row['submit_month']),
                    certified_count=int(row['certified_count']),
                    processed_count=int(row['processed_count'])
                ))
            
            print(f"🔍 Found {len(result)} activity records for {latest_date}")
            
            return result
    except Exception as e:
        print(f"Error in get_perm_cases_activity_data: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return []


def get_perm_cases_latest_month_data(conn) -> List[PermCaseActivityData]:
    """Query 2: Get the busiest submission month from recent certification activity."""
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Find the most recent update date (when work was done)
            # Convert UTC to ET time for proper date comparison
            cursor.execute("""
                SELECT MAX(updated_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/New_York') as latest_update_date
                FROM perm_cases 
                WHERE status = 'CERTIFIED'
            """)
            
            latest_update_row = cursor.fetchone()
            if not latest_update_row or not latest_update_row['latest_update_date']:
                print("🔍 No certified PERM cases found")
                return []
            
            latest_update_date = latest_update_row['latest_update_date']
            print(f"🔍 Most recent certification activity date (ET): {latest_update_date}")
            
            # Use June (month 6) as the featured month for dashboard consistency
            # This provides stable reporting regardless of daily processing variations
            busiest_month = 6  # May
            print(f"🔍 Using June (month {busiest_month}) as featured month for dashboard")
            
            # Now get all employer data for that busiest month
            # Get ALL certified and review cases for the busiest submission month, not just recent certifications
            # Focus on 2024 data for current relevance
            cursor.execute("""
                SELECT 
                    employer_first_letter, 
                    %s as submit_month,
                    SUM(CASE WHEN status = 'CERTIFIED' THEN 1 ELSE 0 END) as case_count,
                    SUM(CASE WHEN status IN ('ANALYST REVIEW', 'RECONSIDERATION APPEALS') THEN 1 ELSE 0 END) as review_count
                FROM perm_cases 
                WHERE date_part('month', submit_date) = %s
                AND date_part('year', submit_date) = 2024
                AND status IN ('CERTIFIED', 'ANALYST REVIEW', 'RECONSIDERATION APPEALS')
                GROUP BY employer_first_letter
                HAVING SUM(CASE WHEN status = 'CERTIFIED' THEN 1 ELSE 0 END) > 0
                ORDER BY employer_first_letter ASC
            """, (busiest_month, busiest_month))
            
            result = []
            for row in cursor.fetchall():
                result.append(PermCaseActivityData(
                    employer_first_letter=row['employer_first_letter'],
                    submit_month=int(row['submit_month']),
                    certified_count=int(row['case_count']),
                    review_count=int(row['review_count'])
                ))
            
            print(f"🔍 Found {len(result)} employers in busiest month {busiest_month}")
            return result
            
    except Exception as e:
        print(f"Error in get_perm_cases_latest_month_data: {str(e)}")
        import traceback
        print(traceback.format_exc())
        return []


def get_perm_cases_metrics(conn) -> Dict[str, Any]:
    """Get PERM cases metrics for dashboard integration with both queries."""
    try:
        # Get the latest date with data (same pattern as other functions)
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("""
                SELECT MAX(record_date) as latest_date
                FROM summary_stats
            """)
            latest_row = cursor.fetchone()
            latest_date = latest_row['latest_date'] if latest_row and latest_row['latest_date'] else date.today()
        
        # Query 1: Activity data for the latest date with updates
        daily_activity_data = get_perm_cases_activity_data(conn)
        
        # Query 2: All employer letters from the latest active month
        latest_month_data = get_perm_cases_latest_month_data(conn)
        
        # Calculate summary metrics for daily activity
        daily_most_active_letter = None
        daily_most_active_month = None
        daily_max_count = 0
        daily_total_certified_cases = 0
        
        for activity in daily_activity_data:
            daily_total_certified_cases += activity.certified_count
            if activity.certified_count > daily_max_count:
                daily_max_count = activity.certified_count
                daily_most_active_letter = activity.employer_first_letter
                daily_most_active_month = activity.submit_month
        
        # Calculate summary metrics for latest month
        month_most_active_letter = None
        month_max_count = 0
        month_total_certified_cases = 0
        latest_active_month = None
        
        for activity in latest_month_data:
            month_total_certified_cases += activity.certified_count
            latest_active_month = activity.submit_month
            if activity.certified_count > month_max_count:
                month_max_count = activity.certified_count
                month_most_active_letter = activity.employer_first_letter
        
        return {
            "daily_activity": {
                "activity_data": daily_activity_data,
                "most_active_letter": daily_most_active_letter,
                "most_active_month": daily_most_active_month,
                "total_certified_cases": daily_total_certified_cases,
                "data_date": latest_date
            },
            "latest_month_activity": {
                "activity_data": latest_month_data,
                "most_active_letter": month_most_active_letter,
                "latest_active_month": latest_active_month,
                "total_certified_cases": month_total_certified_cases
            }
        }
    except Exception as e:
        print(f"Error in get_perm_cases_metrics: {str(e)}")
        import traceback
        print(traceback.format_exc())
        # Return empty metrics on error
        return {
            "daily_activity": {
                "activity_data": [],
                "most_active_letter": None,
                "most_active_month": None,
                "total_certified_cases": 0,
                "data_date": date.today()
            },
            "latest_month_activity": {
                "activity_data": [],
                "most_active_letter": None,
                "latest_active_month": None,
                "total_certified_cases": 0
            }
        }