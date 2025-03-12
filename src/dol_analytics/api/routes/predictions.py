from datetime import date, timedelta
from typing import Dict, Any, Optional
from fastapi import APIRouter, Depends, Query, HTTPException, Path
import psycopg2
import psycopg2.extras
import json

# Use relative imports if running as a module
try:
    from ...models.database import get_postgres_connection
    from ...models.schemas import CasePrediction
except ImportError:
    # Use absolute imports if running as a script
    from src.dol_analytics.models.database import get_postgres_connection
    from src.dol_analytics.models.schemas import CasePrediction

router = APIRouter(prefix="/predictions", tags=["predictions"])


@router.get("/case/{case_id}")
async def predict_case_completion(
    case_id: str = Path(..., description="Case identifier"),
    conn=Depends(get_postgres_connection)
):
    """
    Predict completion date for a specific case identifier.
    
    Note: Since we don't have individual case data in the external PostgreSQL database,
    this endpoint provides an estimate based on average processing times.
    """
    try:
        # Since we don't have case-level data, we'll use the statistics to make a prediction
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # Get processing time metrics
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
            
            # Get current backlog
            cursor.execute("""
                SELECT pending_applications
                FROM summary_stats
                ORDER BY record_date DESC
                LIMIT 1
            """)
            
            backlog_row = cursor.fetchone()
            current_backlog = backlog_row['pending_applications'] if backlog_row else 0
            
            # Assume case was submitted today
            submit_date = date.today()
            median_days = processing_row['median_days'] or 150
            upper_estimate = processing_row['upper_estimate_days'] or 300
            
            estimated_completion_date = submit_date + timedelta(days=median_days)
            upper_bound_date = submit_date + timedelta(days=upper_estimate)
            
            return {
                "case_id": case_id,
                "estimated_completion_date": estimated_completion_date.isoformat(),
                "upper_bound_date": upper_bound_date.isoformat(),
                "estimated_days": median_days,
                "upper_bound_days": upper_estimate,
                "note": "This is an estimate based on current processing times, not specific to this case.",
                "factors_considered": {
                    "current_backlog": current_backlog,
                    "base_processing_time": median_days
                },
                "confidence_level": 0.8  # 80% confidence
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error predicting completion date: {str(e)}")


@router.get("/from-date")
async def predict_from_submit_date(
    submit_date: date = None,
    conn=Depends(get_postgres_connection)
):
    """
    Predict completion date for a case submitted on a specific date.
    
    If no date is provided, uses today's date.
    """
    if not submit_date:
        submit_date = date.today()
        
    try:
        # Get the latest processing time metrics
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
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
            
            # Get current backlog
            cursor.execute("""
                SELECT pending_applications
                FROM summary_stats
                ORDER BY record_date DESC
                LIMIT 1
            """)
            
            backlog_row = cursor.fetchone()
            current_backlog = backlog_row['pending_applications'] if backlog_row else 0
            
            # Calculate estimated completion date
            median_days = processing_row['median_days'] or 150  # Default to 150 days if null
            upper_estimate = processing_row['upper_estimate_days'] or 300  # Default to 300 days if null
            
            estimated_completion_date = submit_date + timedelta(days=median_days)
            upper_bound_date = submit_date + timedelta(days=upper_estimate)
            
            return {
                "submit_date": submit_date.isoformat(),
                "estimated_completion_date": estimated_completion_date.isoformat(),
                "upper_bound_date": upper_bound_date.isoformat(),
                "estimated_days": median_days,
                "upper_bound_days": upper_estimate,
                "factors_considered": {
                    "current_backlog": current_backlog,
                    "base_processing_time": median_days
                },
                "confidence_level": 0.8  # 80% confidence
            }
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error predicting completion date: {str(e)}")


@router.get("/expected-time")
async def get_expected_processing_time(
    conn=Depends(get_postgres_connection)
):
    """
    Get current expected processing time for new cases.
    
    Returns the current expected processing time in days for a new case
    submitted today, based on the current backlog and historical patterns.
    """
    try:
        today = date.today()
        
        # Get the latest processing times from the processing_times table
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
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
            
            # Get current backlog from summary_stats
            cursor.execute("""
                SELECT pending_applications
                FROM summary_stats
                ORDER BY record_date DESC
                LIMIT 1
            """)
            
            backlog_row = cursor.fetchone()
            backlog = backlog_row['pending_applications'] if backlog_row else 0
            
            # Use median (50th percentile) as the expected processing time
            expected_days = processing_row['median_days']
            
            # Use 80th percentile for confidence level factor
            confidence_level = 0.8  # 80% confidence
            
            return {
                "expected_days": expected_days,
                "current_backlog": backlog,
                "confidence_level": confidence_level
            }
            
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calculating expected time: {str(e)}")