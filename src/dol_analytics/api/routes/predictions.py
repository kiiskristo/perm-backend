from datetime import date, timedelta
from typing import Dict, Any, Optional
from fastapi import APIRouter, Depends, Query, HTTPException, Path
import psycopg2
import psycopg2.extras
import json
from pydantic import BaseModel

# Use relative imports if running as a module
try:
    from ...models.database import get_postgres_connection
    from ...models.schemas import CasePrediction
except ImportError:
    # Use absolute imports if running as a script
    from src.dol_analytics.models.database import get_postgres_connection
    from src.dol_analytics.models.schemas import CasePrediction

router = APIRouter(prefix="/predictions", tags=["predictions"])

# Add this class for the request body
class DateSubmissionRequest(BaseModel):
    submit_date: date

@router.post("/from-date")
async def predict_from_submit_date(
    request: DateSubmissionRequest,
    conn=Depends(get_postgres_connection)
):
    """
    Predict completion date for a case submitted on a specific date,
    factoring in current processing rates and queue position.
    """
    submit_date = request.submit_date
    today = date.today()
    
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # 1. Get base processing time metrics
            cursor.execute("""
                SELECT 
                    percentile_50 as median_days,
                    percentile_80 as upper_estimate_days
                FROM processing_times
                ORDER BY record_date DESC
                LIMIT 1
            """)
            processing_row = cursor.fetchone()
            
            if not processing_row:
                raise HTTPException(status_code=404, detail="No processing time data available")
                
            # 2. Get current backlog (cases ahead in queue)
            cursor.execute("""
                SELECT pending_applications
                FROM summary_stats
                ORDER BY record_date DESC
                LIMIT 1
            """)
            backlog_row = cursor.fetchone()
            current_backlog = float(backlog_row['pending_applications']) if backlog_row and backlog_row['pending_applications'] else 0
            
            # 3. Use weekly_summary view to get weekly processing rate
            # Get the current week's start date
            current_week_start = today - timedelta(days=today.weekday())
            
            # Query weekly_summary for average weekly processing using a subquery
            cursor.execute("""
                SELECT AVG(total_applications) as avg_weekly_apps
                FROM (
                    SELECT week_start, total_applications
                    FROM weekly_summary
                    WHERE week_start < %s  -- Exclude current incomplete week
                    ORDER BY week_start DESC
                    LIMIT 4
                ) as recent_weeks
            """, (current_week_start,))
            
            # Get result and apply fallback if needed
            weekly_row = cursor.fetchone()
            weekly_rate = float(weekly_row['avg_weekly_apps']) if weekly_row and weekly_row['avg_weekly_apps'] else 2900
            
            # 4. Basic processing times
            base_days = float(processing_row['median_days']) if processing_row and processing_row['median_days'] else 150
            upper_base_days = float(processing_row['upper_estimate_days']) if processing_row and processing_row['upper_estimate_days'] else 300
            
            # 5. Calculate queue position from monthly_status table
            days_in_queue = max(0, (today - submit_date).days)

            # Get the month and year from the submit date
            submit_month_name = submit_date.strftime("%B")
            submit_year = submit_date.year

            # Create a mapping of month names to numbers
            month_to_num = {
                "January": 1, "February": 2, "March": 3, "April": 4,
                "May": 5, "June": 6, "July": 7, "August": 8,
                "September": 9, "October": 10, "November": 11, "December": 12
            }

            # Get numeric value of the submission month
            submit_month_num = month_to_num.get(submit_month_name, 0)

            # Sum all ANALYST REVIEW cases from months BEFORE the submit month
            cursor.execute("""
                SELECT SUM(count) as cases_ahead
                FROM monthly_status
                WHERE status = 'ANALYST REVIEW'
                AND (
                    (year < %s) OR 
                    (year = %s AND 
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
                        END < %s
                    )
                )
            """, (submit_year, submit_year, submit_month_num))

            row = cursor.fetchone()
            cases_before_month = float(row['cases_ahead']) if row and row['cases_ahead'] else 0

            # For the submit month itself, take cases proportional to the day of month
            cursor.execute("""
                SELECT count
                FROM monthly_status
                WHERE status = 'ANALYST REVIEW'
                AND year = %s AND month = %s
            """, (submit_year, submit_month_name))

            same_month_row = cursor.fetchone()
            same_month_cases = 0
            if same_month_row and same_month_row['count']:
                month_percentage = (submit_date.day - 1) / 30.0
                same_month_cases = float(same_month_row['count']) * month_percentage

            estimated_queue_position = cases_before_month + same_month_cases

            # 6. Calculate final estimates
            queue_weeks = estimated_queue_position / weekly_rate if weekly_rate > 0 else 0
            queue_days = int(queue_weeks * 7)  # This is the full 336 days

            # Don't subtract days_in_queue - use the full queue time as remaining days
            remaining_days = queue_days  # Full 336 days

            # Total journey is days already waited plus remaining days
            total_journey_days = days_in_queue + remaining_days  # 158 + 336 = 494 days

            # Calculate completion date based on today + full queue time
            estimated_completion_date = today + timedelta(days=remaining_days)
            upper_bound_date = today + timedelta(days=int(remaining_days * 1.15))

            return {
                "submit_date": submit_date.isoformat(),
                "estimated_completion_date": estimated_completion_date.isoformat(),
                "upper_bound_date": upper_bound_date.isoformat(),
                "estimated_days": total_journey_days,  # 494 days (total journey)
                "remaining_days": remaining_days,      # 336 days (full queue time)
                "upper_bound_days": int(total_journey_days * 1.15),
                "queue_analysis": {
                    "current_backlog": int(current_backlog),
                    "estimated_queue_position": int(estimated_queue_position),
                    "weekly_processing_rate": int(weekly_rate),
                    "daily_processing_rate": int(weekly_rate / 7),
                    "estimated_queue_wait_weeks": round(queue_weeks, 1),
                    "days_already_in_queue": days_in_queue
                },
                "factors_considered": {
                    "queue_time": queue_days,
                    "days_remaining": remaining_days,
                    "total_journey_days": total_journey_days
                },
                "confidence_level": 0.8
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error predicting completion date: {str(e)}")
