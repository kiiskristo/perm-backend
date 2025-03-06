import json
from datetime import datetime, date, timedelta
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import desc

# Use relative imports if running as a module
try:
    from ..models.database import CaseData, DailyMetrics, PredictionModel
    from ..services.dol_api import DOLAPIClient
    from ..models.schemas import CasePrediction
except ImportError:
    # Use absolute imports if running as a script
    from src.dol_analytics.models.database import CaseData, DailyMetrics, PredictionModel
    from src.dol_analytics.services.dol_api import DOLAPIClient
    from src.dol_analytics.models.schemas import CasePrediction


class PredictionService:
    """Service for predicting case completion dates."""
    
    def __init__(self, db: Session, dol_client: Optional[DOLAPIClient] = None):
        self.db = db
        self.dol_client = dol_client or DOLAPIClient()
    
    async def predict_case_completion(self, case_identifier: str) -> CasePrediction:
        """Predict completion date for a specific case."""
        # First, check if case exists in our database
        case = self.db.query(CaseData).filter(
            CaseData.case_identifier == case_identifier
        ).first()
        
        # If not, fetch from API
        if not case:
            try:
                case_data = await self.dol_client.get_case_by_id(case_identifier)
                
                # Create simplified case object for prediction
                case = CaseData(
                    case_identifier=case_identifier,
                    submit_date=datetime.strptime(case_data.get("submit_date", ""), "%Y-%m-%d").date(),
                    status=case_data.get("status", ""),
                    agency=case_data.get("agency", "")
                )
            except Exception:
                # Create a default case with today's date if API fetch fails
                case = CaseData(
                    case_identifier=case_identifier,
                    submit_date=date.today(),
                    status="Unknown",
                    agency="Unknown"
                )
        
        # Get current backlog
        backlog = self.db.query(CaseData).filter(
            CaseData.processed_date.is_(None)
        ).count()
        
        # Get latest prediction model
        model = self.db.query(PredictionModel).order_by(
            desc(PredictionModel.model_date)
        ).first()
        
        if not model:
            # Use default values if no model exists
            base_time = 30  # Default: 30 days
            backlog_factor = 0.01
            seasonal_factors = {
                "monthly": {str(i): 1.0 for i in range(1, 13)},
                "daily": {str(i): 1.0 for i in range(7)}
            }
        else:
            base_time = model.base_processing_time
            backlog_factor = model.backlog_factor
            seasonal_factors = json.loads(model.seasonal_factors)
        
        # Calculate estimated completion date
        processing_days = self._calculate_processing_time(
            case.submit_date, base_time, backlog, backlog_factor, seasonal_factors
        )
        
        estimated_date = case.submit_date + timedelta(days=processing_days)
        
        # Create prediction result
        factors = {
            "base_processing_time": base_time,
            "current_backlog": backlog,
            "backlog_factor": backlog_factor,
            "submit_month": case.submit_date.month,
            "seasonal_adjustment": self._get_seasonal_factor(case.submit_date, seasonal_factors)
        }
        
        return CasePrediction(
            case_identifier=case_identifier,
            submit_date=case.submit_date,
            estimated_completion_date=estimated_date,
            confidence_level=0.85,  # Placeholder - would be calculated from model accuracy
            factors_considered=factors
        )
    
    def predict_from_date(self, submit_date: date) -> CasePrediction:
        """Predict completion date for a case submitted on a specific date."""
        # Generate a pseudo case identifier
        case_identifier = f"HYPO-{submit_date.isoformat()}"
        
        # Create a hypothetical case
        case = CaseData(
            case_identifier=case_identifier,
            submit_date=submit_date,
            status="New",
            agency="Hypothetical"
        )
        
        # Get current backlog
        backlog = self.db.query(CaseData).filter(
            CaseData.processed_date.is_(None)
        ).count()
        
        # Get latest prediction model
        model = self.db.query(PredictionModel).order_by(
            desc(PredictionModel.model_date)
        ).first()
        
        if not model:
            # Use default values if no model exists
            base_time = 30  # Default: 30 days
            backlog_factor = 0.01
            seasonal_factors = {
                "monthly": {str(i): 1.0 for i in range(1, 13)},
                "daily": {str(i): 1.0 for i in range(7)}
            }
        else:
            base_time = model.base_processing_time
            backlog_factor = model.backlog_factor
            seasonal_factors = json.loads(model.seasonal_factors)
        
        # Calculate estimated completion date
        processing_days = self._calculate_processing_time(
            case.submit_date, base_time, backlog, backlog_factor, seasonal_factors
        )
        
        estimated_date = case.submit_date + timedelta(days=processing_days)
        
        # Create prediction result
        factors = {
            "base_processing_time": base_time,
            "current_backlog": backlog,
            "backlog_factor": backlog_factor,
            "submit_month": case.submit_date.month,
            "seasonal_adjustment": self._get_seasonal_factor(case.submit_date, seasonal_factors)
        }
        
        return CasePrediction(
            case_identifier=case_identifier,
            submit_date=case.submit_date,
            estimated_completion_date=estimated_date,
            confidence_level=0.8,  # Lower confidence for hypothetical case
            factors_considered=factors
        )
    
    def _calculate_processing_time(
        self,
        submit_date: date,
        base_time: float,
        backlog: int,
        backlog_factor: float,
        seasonal_factors: Dict[str, Dict[str, float]]
    ) -> int:
        """Calculate the expected processing time in days."""
        # Base processing time
        processing_time = base_time
        
        # Add backlog impact
        processing_time += backlog * backlog_factor
        
        # Apply seasonal factors
        seasonal_factor = self._get_seasonal_factor(submit_date, seasonal_factors)
        processing_time *= seasonal_factor
        
        return round(processing_time)
    
    def _get_seasonal_factor(
        self, 
        submit_date: date, 
        seasonal_factors: Dict[str, Dict[str, float]]
    ) -> float:
        """Get the combined seasonal factor for a date."""
        # Get month factor
        month_factor = seasonal_factors.get("monthly", {}).get(
            str(submit_date.month), 1.0
        )
        
        # Get day of week factor
        day_factor = seasonal_factors.get("daily", {}).get(
            str(submit_date.weekday()), 1.0
        )
        
        # Combine factors
        return month_factor * day_factor