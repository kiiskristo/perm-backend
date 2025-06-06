from datetime import date, timedelta
from typing import Dict, Any
from fastapi import APIRouter, Depends, HTTPException
import psycopg2
import psycopg2.extras
from pydantic import BaseModel, Field
import requests

# Use relative imports if running as a module
try:
    from ...models.database import get_postgres_connection
except ImportError:
    # Use absolute imports if running as a script
    from src.dol_analytics.models.database import get_postgres_connection

from ...config import get_settings

settings = get_settings()

router = APIRouter(prefix="/predictions", tags=["predictions"])

# Update request model to include employer first letter and case number
class DateSubmissionRequest(BaseModel):
    submit_date: date
    employer_first_letter: str = Field(..., min_length=1, max_length=1, description="First letter of employer name (A-Z)")
    case_number: str = Field(..., description="Case number for the application")
    recaptcha_token: str = Field(..., description="Google reCAPTCHA token")

@router.post("/from-date")
async def predict_from_submit_date(
    request: DateSubmissionRequest,
    conn=Depends(get_postgres_connection)
):
    """
    Predict completion date for a case submitted on a specific date,
    factoring in current processing rates, queue position, and employer name.
    Protected by reCAPTCHA to prevent abuse.
    """
    # Verify reCAPTCHA token before processing
    if not verify_recaptcha(request.recaptcha_token):
        raise HTTPException(status_code=400, detail="Invalid reCAPTCHA. Please try again.")
    
    submit_date = request.submit_date
    employer_letter = request.employer_first_letter.upper()
    case_number = request.case_number
    today = date.today()
    
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # First, ensure the prediction_requests table exists
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS prediction_requests (
                    id SERIAL PRIMARY KEY,
                    submit_date DATE NOT NULL,
                    employer_first_letter CHAR(1) NOT NULL,
                    case_number VARCHAR(255) NOT NULL,
                    request_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    estimated_completion_date DATE,
                    estimated_days INTEGER,
                    confidence_level DECIMAL(3,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Store the prediction request
            cursor.execute("""
                INSERT INTO prediction_requests (
                    submit_date, employer_first_letter, case_number, request_timestamp
                ) VALUES (%s, %s, %s, %s)
                RETURNING id
            """, (submit_date, employer_letter, case_number, today))
            
            request_id = cursor.fetchone()['id']
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

            # For the submit month itself, day of month is less relevant than employer letter
            cursor.execute("""
                SELECT count
                FROM monthly_status
                WHERE status = 'ANALYST REVIEW'
                AND year = %s AND month = %s
            """, (submit_year, submit_month_name))

            same_month_row = cursor.fetchone()
            same_month_total = float(same_month_row['count']) if same_month_row and same_month_row['count'] else 0

            # Determine letter position (A=0, Z=25)
            letter_position = ord(employer_letter) - ord('A')

            # Calculate position within the same month based primarily on letter
            # Employers with names earlier in alphabet get processed earlier
            letter_percentage = letter_position / 25.0  # 0.0 for 'A', 1.0 for 'Z'

            # The day of month has a minor effect compared to employer letter (20% day, 80% letter)
            day_percentage = (submit_date.day - 1) / 30.0  # 0.0 for day 1, 1.0 for day 31
            month_position_factor = (letter_percentage * 0.8) + (day_percentage * 0.2)

            # Calculate position within month
            same_month_cases = same_month_total * month_position_factor

            # Calculate total queue position
            raw_queue_position = cases_before_month + same_month_cases

            # We don't need the additional letter adjustment since we incorporated it directly
            adjusted_queue_position = raw_queue_position

            # Recalculate queue time with the position
            queue_weeks = adjusted_queue_position / weekly_rate if weekly_rate > 0 else 0
            queue_days = int(queue_weeks * 7)
            
            # Rest of calculations using the adjusted queue_days
            remaining_days = queue_days
            total_journey_days = days_in_queue + remaining_days
            
            # Calculate completion date based on today + adjusted queue time
            estimated_completion_date = today + timedelta(days=remaining_days)
            upper_bound_date = today + timedelta(days=int(remaining_days * 1.15))
            
            # Define letter priority categories
            letter_priority = "HIGH" if letter_position < 9 else "MEDIUM" if letter_position < 18 else "LOW"
            letter_impact = "FASTER" if letter_position < 9 else "AVERAGE" if letter_position < 18 else "SLOWER"

            # Update the stored prediction with calculated results
            cursor.execute("""
                UPDATE prediction_requests 
                SET estimated_completion_date = %s, 
                    estimated_days = %s, 
                    confidence_level = %s
                WHERE id = %s
            """, (estimated_completion_date, total_journey_days, 0.8, request_id))
            
            # Include letter information and case number in response
            return {
                "request_id": request_id,
                "submit_date": submit_date.isoformat(),
                "employer_first_letter": employer_letter,
                "case_number": case_number,
                "estimated_completion_date": estimated_completion_date.isoformat(),
                "upper_bound_date": upper_bound_date.isoformat(),
                "estimated_days": total_journey_days,
                "remaining_days": remaining_days,
                "upper_bound_days": int(total_journey_days * 1.15),
                "queue_analysis": {
                    "current_backlog": int(current_backlog),
                    "raw_queue_position": int(raw_queue_position),
                    "adjusted_queue_position": int(adjusted_queue_position),
                    "weekly_processing_rate": int(weekly_rate),
                    "daily_processing_rate": int(weekly_rate / 7),
                    "estimated_queue_wait_weeks": round(queue_weeks, 1),
                    "days_already_in_queue": days_in_queue,
                    "employer_letter_impact": round((letter_percentage - 0.5) * 160, 0)  # days faster/slower vs middle of alphabet
                },
                "factors_considered": {
                    "queue_time": queue_days,
                    "days_remaining": remaining_days,
                    "total_journey_days": total_journey_days,
                    "employer_name_letter": employer_letter,
                    "letter_priority": letter_priority,  # HIGH, MEDIUM, or LOW
                    "processing_impact": letter_impact   # FASTER, AVERAGE, or SLOWER
                },
                "confidence_level": 0.8
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error predicting completion date: {str(e)}")

@router.get("/requests")
async def get_prediction_requests(
    limit: int = 100,
    offset: int = 0,
    conn=Depends(get_postgres_connection)
):
    """
    Get stored prediction requests with pagination.
    """
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Get total count
            cursor.execute("SELECT COUNT(*) as total FROM prediction_requests")
            total_count = cursor.fetchone()['total']
            
            # Get paginated results
            cursor.execute("""
                SELECT 
                    id,
                    submit_date,
                    employer_first_letter,
                    case_number,
                    request_timestamp,
                    estimated_completion_date,
                    estimated_days,
                    confidence_level,
                    created_at
                FROM prediction_requests
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
            """, (limit, offset))
            
            requests = cursor.fetchall()
            
            return {
                "total": total_count,
                "limit": limit,
                "offset": offset,
                "requests": [dict(row) for row in requests]
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving prediction requests: {str(e)}")


@router.get("/requests/{request_id}")
async def get_prediction_request(
    request_id: int,
    conn=Depends(get_postgres_connection)
):
    """
    Get a specific prediction request by ID.
    """
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            cursor.execute("""
                SELECT 
                    id,
                    submit_date,
                    employer_first_letter,
                    case_number,
                    request_timestamp,
                    estimated_completion_date,
                    estimated_days,
                    confidence_level,
                    created_at
                FROM prediction_requests
                WHERE id = %s
            """, (request_id,))
            
            request = cursor.fetchone()
            
            if not request:
                raise HTTPException(status_code=404, detail="Prediction request not found")
            
            return dict(request)
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error retrieving prediction request: {str(e)}")


def verify_recaptcha(token: str) -> bool:
    """Verify reCAPTCHA token with Google's API."""
    try:
        # Skip verification in development mode if configured
        if settings.DEBUG and settings.SKIP_RECAPTCHA_IN_DEBUG:
            print("DEBUG mode: Skipping reCAPTCHA verification")
            return True
            
        recaptcha_secret = settings.RECAPTCHA_SECRET_KEY
        if not recaptcha_secret:
            print("WARNING: reCAPTCHA secret key not configured, skipping verification")
            return True
            
        # Make request to Google's verification API
        response = requests.post(
            "https://www.google.com/recaptcha/api/siteverify",
            data={
                "secret": recaptcha_secret,
                "response": token
            }
        )
        result = response.json()
        
        # Log result for debugging
        print(f"reCAPTCHA verification result: {result}")
        
        # Return True if successful, False otherwise
        return result.get("success", False)
    except Exception as e:
        print(f"Error verifying reCAPTCHA: {str(e)}")
        # In case of error, default to rejecting the request for security
        return False
