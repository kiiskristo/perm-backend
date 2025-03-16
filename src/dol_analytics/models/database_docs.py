"""
Documentation for DOL Analytics Database Schema

This file contains comprehensive documentation for all tables and views
in the DOL Analytics database, including their structure, purpose, and example queries.
"""

# Table Documentation

DAILY_PROGRESS_DOCS = """
Table: daily_progress
--------------------
Tracks the daily processing statistics for DOL applications.

Columns:
    id (INTEGER): Primary key, auto-incrementing identifier
    date (DATE): The specific date of the record
    day_of_week (TEXT): String representation of the weekday (e.g., "Monday")
    total_applications (INTEGER): Number of applications processed on this date
    created_at (TIMESTAMP): When this record was created/updated in the database

Purpose:
    This table tracks how many applications are processed by the DOL each day,
    providing insights into processing patterns, weekly trends, and enabling
    accurate predictions for application timelines.

Notes:
    - Processing volumes typically drop significantly on weekends
    - Data is usually imported/updated daily from DOL processing statistics
    - Forms the basis for weekly and monthly aggregate views/tables

Example Queries:
    
    # Get average processing volume by day of week
    SELECT 
        day_of_week,
        AVG(total_applications) as avg_applications
    FROM daily_progress
    GROUP BY day_of_week
    ORDER BY 
        CASE 
            WHEN day_of_week = 'Monday' THEN 1
            WHEN day_of_week = 'Tuesday' THEN 2
            WHEN day_of_week = 'Wednesday' THEN 3
            WHEN day_of_week = 'Thursday' THEN 4
            WHEN day_of_week = 'Friday' THEN 5
            WHEN day_of_week = 'Saturday' THEN 6
            WHEN day_of_week = 'Sunday' THEN 7
        END;
    
    # Get total weekly processing for recent weeks
    SELECT 
        date_trunc('week', date) as week_start,
        SUM(total_applications) as weekly_total
    FROM daily_progress
    WHERE date >= CURRENT_DATE - INTERVAL '8 weeks'
    GROUP BY week_start
    ORDER BY week_start DESC;
"""

MONTHLY_STATUS_DOCS = """
Table: monthly_status
--------------------
Tracks monthly case statistics by status type.

Columns:
    id (INTEGER): Primary key identifier
    month (TEXT): Month name (e.g., "January", "February")
    year (INTEGER): Year of the data point (e.g., 2023, 2024, 2025)
    status (TEXT): Case status type (e.g., "ANALYST REVIEW", "CERTIFIED", "WITHDRAWN")
    count (INTEGER): Number of cases in this status for the month/year
    daily_change (INTEGER): Daily change in the count (often 0 or small numbers)
    is_active (BOOLEAN): Whether this status is currently active
    created_at (TIMESTAMP): Record creation timestamp

Purpose:
    Provides monthly aggregated data on case statuses, critical for analyzing
    queue positions and backlog patterns over time. This table is particularly 
    important for estimating how many cases are ahead of a given submission.

Notes:
    - Status types include: ANALYST REVIEW, CERTIFIED, WITHDRAWN, DENIED, 
      SUPERVISED RECRUITMENT, RFI ISSUED, BALCA APPEALS, etc.
    - ANALYST REVIEW status represents cases waiting in the queue
    - Month is stored as a text name rather than numeric value, requiring
      special handling for chronological ordering

Example Queries:
    # Get all ANALYST REVIEW cases by month/year, ordered chronologically
    SELECT 
        year, 
        month, 
        count 
    FROM monthly_status 
    WHERE status = 'ANALYST REVIEW'
    ORDER BY year DESC, 
        CASE
            WHEN month = 'January' THEN 1
            WHEN month = 'February' THEN 2
            WHEN month = 'March' THEN 3
            WHEN month = 'April' THEN 4
            WHEN month = 'May' THEN 5
            WHEN month = 'June' THEN 6
            WHEN month = 'July' THEN 7
            WHEN month = 'August' THEN 8
            WHEN month = 'September' THEN 9
            WHEN month = 'October' THEN 10
            WHEN month = 'November' THEN 11
            WHEN month = 'December' THEN 12
        END DESC;
"""

PROCESSING_TIMES_DOCS = """
Table: processing_times
--------------------
Statistical data on application processing durations.

Columns:
    id (INTEGER): Primary key identifier
    record_date (DATE): Date when this data point was recorded
    percentile_30 (INTEGER): 30th percentile processing time in days
    percentile_50 (INTEGER): Median processing time in days (50th percentile)
    percentile_80 (INTEGER): 80th percentile processing time in days
    created_at (TIMESTAMP): Record creation timestamp

Purpose:
    Provides statistical measures of how long applications take to process,
    which forms the foundation of time predictions. Different percentiles
    offer various confidence levels for estimates.

Notes:
    - Data represents statistical analysis of completed applications
    - The 50th percentile (median) indicates typical processing times
    - The 80th percentile provides a more conservative estimate
    - Values tend to be stable over time (e.g., 486, 494, 507 days)
"""

SUMMARY_STATS_DOCS = """
Table: summary_stats
--------------------
Daily summary statistics for DOL application processing.

Columns:
    id (INTEGER): Primary key identifier
    record_date (DATE): Date of this summary record
    total_applications (INTEGER): Total number of applications in the system
    pending_applications (INTEGER): Number of applications that are still pending
    pending_percentage (TEXT): Percentage of applications that are pending (formatted as string)
    changes_today (INTEGER): Total application status changes today
    completed_today (INTEGER): Number of applications completed today
    created_at (TIMESTAMP): Record creation timestamp

Purpose:
    Provides a daily snapshot of the overall DOL processing status,
    including backlog size and completion metrics. This table is critical
    for tracking trends in total pending applications and daily throughput.
"""

# View Documentation 

WEEKLY_SUMMARY_DOCS = """
View: weekly_summary
--------------------
Weekly aggregation of application processing data.

Columns:
    week_start (DATE): Start date of the week (typically Monday)
    total_applications (INTEGER): Total number of applications processed in the week
    avg_daily_applications (NUMERIC): Average daily processing rate for the week

Purpose:
    Provides a weekly view of processing volumes to smooth out daily fluctuations.
    This view is particularly important for calculating processing rates used in
    queue time predictions.

Notes:
    - Generated from daily_progress data, aggregated by week
    - Used to calculate the average weekly processing rate (critical for predictions)
    - Typically shows around 2,700-3,200 applications processed per week
"""

MONTHLY_SUMMARY_DOCS = """
View: monthly_summary
--------------------
Monthly aggregation of application processing by year and month.

Columns:
    year (INTEGER): Year of the data point
    month (TEXT): Month name (e.g., "January", "February")
    total_count (INTEGER): Total number of applications processed in the month
    is_active (BOOLEAN): Whether this month is still active/current

Purpose:
    Provides a monthly overview of application volumes, useful for identifying
    seasonal patterns and long-term trends in DOL processing capacity.

Notes:
    - Aggregates all application statuses for each month
    - The is_active flag indicates whether the month is still receiving updates
    - Monthly volumes vary significantly, from ~6,600 to ~19,000 applications
"""

# Function to get documentation for a specific table
def get_table_docs(table_name):
    """
    Get documentation for a specific table.
    
    Args:
        table_name (str): Name of the table to get documentation for
        
    Returns:
        str: Documentation string for the specified table, or None if not found
    """
    docs_map = {
        'daily_progress': DAILY_PROGRESS_DOCS,
        'monthly_status': MONTHLY_STATUS_DOCS,
        'processing_times': PROCESSING_TIMES_DOCS,
        'summary_stats': SUMMARY_STATS_DOCS,
        'weekly_summary': WEEKLY_SUMMARY_DOCS,
        'monthly_summary': MONTHLY_SUMMARY_DOCS
    }
    
    return docs_map.get(table_name.lower())


# Schema Overview
def get_schema_overview():
    """
    Return a high-level overview of the database schema.
    
    Returns:
        str: Text description of the database schema and relationships
    """
    return """
    DOL Analytics Database Schema
    ============================
    
    Core Tables:
    - daily_progress: Daily processing statistics
    - monthly_status: Monthly case counts by status
    - processing_times: Statistical measures of processing durations
    - summary_stats: Daily summary of application processing
    
    Views:
    - weekly_summary: Weekly aggregation of processing data
    - monthly_summary: Monthly aggregation of processing data
    
    The database is organized to track Department of Labor (DOL) application
    processing statistics at different time intervals (daily, weekly, monthly),
    providing the foundation for accurate queue position and completion time
    predictions.
    """ 