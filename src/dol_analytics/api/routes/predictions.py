from datetime import date
from typing import Optional
from fastapi import APIRouter, Depends, Query, HTTPException, Path
from sqlalchemy.orm import Session

from src.dol_analytics.models.database import get_db
from src.dol_analytics.services.prediction import PredictionService
from src.dol_analytics.models.schemas import CasePrediction

router = APIRouter(prefix="/predictions", tags=["predictions"])


@router.get("/case/{case_id}", response_model=CasePrediction)
async def predict_case_completion(
    case_id: str = Path(..., description="Case identifier"),
    db: Session = Depends(get_db)
):
    """
    Predict completion date for a specific case.
    
    This endpoint takes a case identifier and predicts when it will be completed
    based on historical patterns, current backlog, and other factors.
    """
    prediction_service = PredictionService(db)
    
    try:
        prediction = await prediction_service.predict_case_completion(case_id)
        return prediction
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating prediction: {str(e)}")


@router.get("/from-date", response_model=CasePrediction)
async def predict_from_date(
    submit_date: date = Query(..., description="Case submission date"),
    db: Session = Depends(get_db)
):
    """
    Predict completion date for a hypothetical case submitted on a specific date.
    
    This endpoint allows predicting when a case would be completed if it were
    submitted on a specified date, based on current backlog and historical patterns.
    """
    # Validate submit date isn't in the future
    today = date.today()
    if submit_date > today:
        raise HTTPException(
            status_code=400, 
            detail="Submission date cannot be in the future"
        )
    
    prediction_service = PredictionService(db)
    
    try:
        prediction = prediction_service.predict_from_date(submit_date)
        return prediction
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating prediction: {str(e)}")


@router.get("/expected-time")
async def get_expected_processing_time(
    db: Session = Depends(get_db)
):
    """
    Get current expected processing time for new cases.
    
    Returns the current expected processing time in days for a new case
    submitted today, based on the current backlog and historical patterns.
    """
    prediction_service = PredictionService(db)
    
    try:
        today = date.today()
        prediction = prediction_service.predict_from_date(today)
        
        processing_days = (prediction.estimated_completion_date - today).days
        
        return {
            "expected_days": processing_days,
            "current_backlog": prediction.factors_considered["current_backlog"],
            "confidence_level": prediction.confidence_level
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error calculating expected time: {str(e)}")