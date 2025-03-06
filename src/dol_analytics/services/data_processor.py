from datetime import datetime, date, timedelta
import json
from typing import Dict, List, Optional, Tuple, Any
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, desc
import pandas as pd
import numpy as np

# Use relative imports if running as a module
try:
    from ..models.database import CaseData, DailyMetrics, PredictionModel
    from ..services.dol_api import DOLAPIClient
    from ..models.schemas import (
        CaseCreate, CaseUpdate, DailyMetricsCreate, PredictionModelCreate,
        DailyVolumeData, WeeklyAverageData, WeeklyVolumeData, MonthlyVolumeData
    )
except ImportError:
    # Use absolute imports if running as a script
    from src.dol_analytics.models.database import CaseData, DailyMetrics, PredictionModel
    from src.dol_analytics.services.dol_api import DOLAPIClient
    from src.dol_analytics.models.schemas import (
        CaseCreate, CaseUpdate, DailyMetricsCreate, PredictionModelCreate,
        DailyVolumeData, WeeklyAverageData, WeeklyVolumeData, MonthlyVolumeData
    )


class DataProcessor:
    """Process and transform DOL API data."""
    
    def __init__(self, db: Session, dol_client: Optional[DOLAPIClient] = None):
        self.db = db
        self.dol_client = dol_client or DOLAPIClient()
    
    async def fetch_and_process_daily_data(self) -> None:
        """Fetch latest data from DOL API and update database."""
        # Get yesterday's date
        yesterday = date.today() - timedelta(days=1)
        
        # Fetch new cases submitted yesterday
        new_cases = await self.dol_client.get_cases_by_date_range(
            start_date=yesterday,
            end_date=yesterday,
            fields=["case_identifier", "submit_date", "status", "agency"]
        )
        
        # Process and store new cases
        for case_data in new_cases:
            case = CaseCreate(
                case_identifier=case_data["case_identifier"],
                submit_date=datetime.strptime(case_data["submit_date"], "%Y-%m-%d").date(),
                status=case_data["status"],
                agency=case_data["agency"]
            )
            self._create_or_update_case(case)
        
        # Fetch processed cases (status changed to a completed status)
        # This would depend on your specific DOL dataset structure
        processed_cases = await self.dol_client.get_cases_by_date_range(
            start_date=yesterday,
            end_date=yesterday,
            fields=["case_identifier", "status", "processed_date"],
            status="Completed"  # Adjust based on actual status values
        )
        
        # Update processed cases
        for case_data in processed_cases:
            case_update = CaseUpdate(
                status=case_data["status"],
                processed_date=yesterday
            )
            self._update_case(case_data["case_identifier"], case_update)
        
        # Calculate daily metrics
        self._calculate_daily_metrics(yesterday)
        
        # Update prediction model if it's the first of the month
        if yesterday.day == 1:
            self._update_prediction_model()
    
    def _create_or_update_case(self, case_data: CaseCreate) -> None:
        """Create a new case or update existing one."""
        existing_case = self.db.query(CaseData).filter(
            CaseData.case_identifier == case_data.case_identifier
        ).first()
        
        if existing_case:
            # Update case
            for key, value in case_data.model_dump(exclude_unset=True).items():
                setattr(existing_case, key, value)
        else:
            # Create new case
            db_case = CaseData(**case_data.model_dump())
            self.db.add(db_case)
        
        self.db.commit()
    
    def _update_case(self, case_identifier: str, case_update: CaseUpdate) -> None:
        """Update an existing case."""
        existing_case = self.db.query(CaseData).filter(
            CaseData.case_identifier == case_identifier
        ).first()
        
        if existing_case:
            for key, value in case_update.model_dump(exclude_unset=True).items():
                setattr(existing_case, key, value)
            
            self.db.commit()
    
    def _calculate_daily_metrics(self, metric_date: date) -> None:
        """Calculate and store daily metrics."""
        # Count new cases for the day
        new_cases_count = self.db.query(func.count(CaseData.id)).filter(
            CaseData.submit_date == metric_date
        ).scalar() or 0
        
        # Count processed cases for the day
        processed_cases_count = self.db.query(func.count(CaseData.id)).filter(
            CaseData.processed_date == metric_date
        ).scalar() or 0
        
        # Calculate current backlog (all unprocessed cases)
        backlog_count = self.db.query(func.count(CaseData.id)).filter(
            CaseData.processed_date.is_(None)
        ).scalar() or 0
        
        # Calculate average processing time for cases processed on this day
        processed_cases = self.db.query(CaseData).filter(
            CaseData.processed_date == metric_date
        ).all()
        
        avg_processing_time = None
        if processed_cases:
            processing_times = [
                (case.processed_date - case.submit_date).days
                for case in processed_cases
                if case.processed_date and case.submit_date
            ]
            
            if processing_times:
                avg_processing_time = sum(processing_times) / len(processing_times)
        
        # Create or update daily metrics
        existing_metrics = self.db.query(DailyMetrics).filter(
            DailyMetrics.date == metric_date
        ).first()
        
        metrics_data = DailyMetricsCreate(
            date=metric_date,
            new_cases=new_cases_count,
            processed_cases=processed_cases_count,
            backlog=backlog_count,
            avg_processing_time=avg_processing_time
        )
        
        if existing_metrics:
            for key, value in metrics_data.model_dump().items():
                setattr(existing_metrics, key, value)
        else:
            db_metrics = DailyMetrics(**metrics_data.model_dump())
            self.db.add(db_metrics)
        
        self.db.commit()
    
    def _update_prediction_model(self) -> None:
        """Update prediction model based on historical data."""
        # This is a simplified implementation
        # A real implementation would involve more sophisticated statistical analysis
        
        # Get historical daily metrics for the past 3 months
        three_months_ago = date.today() - timedelta(days=90)
        metrics = self.db.query(DailyMetrics).filter(
            DailyMetrics.date >= three_months_ago
        ).order_by(DailyMetrics.date).all()
        
        if not metrics:
            return
        
        # Calculate base processing time (average of avg_processing_time)
        processing_times = [
            m.avg_processing_time for m in metrics
            if m.avg_processing_time is not None
        ]
        
        if not processing_times:
            return
        
        base_processing_time = sum(processing_times) / len(processing_times)
        
        # Calculate backlog factor
        # Simple linear model: additional days = backlog_factor * backlog_size
        # This is a simplification; a real model would be more complex
        backlog_factor = 0.01  # Default value
        
        # Create seasonal factors
        # This is a placeholder; a real implementation would calculate
        # actual seasonal factors based on historical data
        seasonal_factors = {
            "monthly": {str(i): 1.0 for i in range(1, 13)},  # Month numbers
            "daily": {str(i): 1.0 for i in range(7)}  # Day of week (0 = Monday)
        }
        
        # Create prediction model
        model_data = PredictionModelCreate(
            model_date=date.today(),
            base_processing_time=base_processing_time,
            backlog_factor=backlog_factor,
            seasonal_factors=json.dumps(seasonal_factors)
        )
        
        db_model = PredictionModel(**model_data.model_dump())
        self.db.add(db_model)
        self.db.commit()
    
    def get_dashboard_data(self, days: int = 30) -> Dict[str, Any]:
        """Get dashboard visualization data."""
        today = date.today()
        start_date = today - timedelta(days=days)
        
        # Daily volume for the specified period
        daily_data = self._get_daily_volume(start_date, today)
        
        # Weekly averages by day of week
        weekly_averages = self._get_weekly_averages(start_date, today)
        
        # Weekly volume as bar chart data
        weekly_volumes = self._get_weekly_volumes(start_date, today)
        
        # Monthly volume for the past 2 months
        two_months_ago = today.replace(day=1) - timedelta(days=1)
        two_months_ago = two_months_ago.replace(day=1)
        monthly_volumes = self._get_monthly_volumes(two_months_ago, today)
        
        # Today's progress
        todays_progress = self._get_todays_progress()
        
        # Current backlog
        current_backlog = self.db.query(func.count(CaseData.id)).filter(
            CaseData.processed_date.is_(None)
        ).scalar() or 0
        
        return {
            "daily_volume": daily_data,
            "weekly_averages": weekly_averages,
            "weekly_volumes": weekly_volumes,
            "monthly_volumes": monthly_volumes,
            "todays_progress": todays_progress,
            "current_backlog": current_backlog
        }
    
    def _get_daily_volume(
        self, start_date: date, end_date: date
    ) -> List[DailyVolumeData]:
        """Get daily volume data."""
        daily_metrics = self.db.query(DailyMetrics).filter(
            and_(
                DailyMetrics.date >= start_date,
                DailyMetrics.date <= end_date
            )
        ).order_by(DailyMetrics.date).all()
        
        result = []
        for metric in daily_metrics:
            result.append(DailyVolumeData(
                date=metric.date,
                count=metric.new_cases
            ))
        
        return result
    
    def _get_weekly_averages(
        self, start_date: date, end_date: date
    ) -> List[WeeklyAverageData]:
        """Get average volume by day of week."""
        daily_metrics = self.db.query(DailyMetrics).filter(
            and_(
                DailyMetrics.date >= start_date,
                DailyMetrics.date <= end_date
            )
        ).all()
        
        # Group by day of week and calculate averages
        day_data = {}
        for metric in daily_metrics:
            day_of_week = metric.date.weekday()
            if day_of_week not in day_data:
                day_data[day_of_week] = []
            day_data[day_of_week].append(metric.new_cases)
        
        # Calculate averages
        result = []
        days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        for day_num, values in day_data.items():
            avg_value = sum(values) / len(values) if values else 0
            result.append(WeeklyAverageData(
                day_of_week=days[day_num],
                average_volume=avg_value
            ))
        
        # Sort by day of week
        result.sort(key=lambda x: days.index(x.day_of_week))
        return result
    
    def _get_weekly_volumes(
        self, start_date: date, end_date: date
    ) -> List[WeeklyVolumeData]:
        """Get weekly volume for bar chart."""
        # Get daily metrics within the date range
        daily_metrics = self.db.query(DailyMetrics).filter(
            and_(
                DailyMetrics.date >= start_date,
                DailyMetrics.date <= end_date
            )
        ).order_by(DailyMetrics.date).all()
        
        # Group by week
        weekly_data = {}
        for metric in daily_metrics:
            # Get the Monday of the week
            monday = metric.date - timedelta(days=metric.date.weekday())
            if monday not in weekly_data:
                weekly_data[monday] = 0
            weekly_data[monday] += metric.new_cases
        
        # Convert to result format
        result = []
        for week_start, total in sorted(weekly_data.items()):
            result.append(WeeklyVolumeData(
                week_starting=week_start,
                total_volume=total
            ))
        
        return result
    
    def _get_monthly_volumes(
        self, start_date: date, end_date: date
    ) -> List[MonthlyVolumeData]:
        """Get monthly volume data."""
        # Get all metrics within the date range
        metrics = self.db.query(DailyMetrics).filter(
            and_(
                DailyMetrics.date >= start_date,
                DailyMetrics.date <= end_date
            )
        ).order_by(DailyMetrics.date).all()
        
        # Group by month and year
        monthly_data = {}
        for metric in metrics:
            key = (metric.date.year, metric.date.month)
            if key not in monthly_data:
                monthly_data[key] = 0
            monthly_data[key] += metric.new_cases
        
        # Convert to result format
        result = []
        month_names = [
            "January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"
        ]
        
        for (year, month), total in sorted(monthly_data.items()):
            result.append(MonthlyVolumeData(
                month=month_names[month - 1],
                year=year,
                total_volume=total
            ))
        
        return result
    
    def _get_todays_progress(self) -> Dict[str, Any]:
        """Get today's progress metrics."""
        today = date.today()
        
        # Get today's metrics if available
        today_metrics = self.db.query(DailyMetrics).filter(
            DailyMetrics.date == today
        ).first()
        
        # Get yesterday's metrics for comparison
        yesterday = today - timedelta(days=1)
        yesterday_metrics = self.db.query(DailyMetrics).filter(
            DailyMetrics.date == yesterday
        ).first()
        
        # Calculate metrics
        new_cases = 0
        processed_cases = 0
        
        if today_metrics:
            new_cases = today_metrics.new_cases
            processed_cases = today_metrics.processed_cases
        
        # Calculate percent changes
        new_cases_change = 0
        processed_cases_change = 0
        
        if yesterday_metrics:
            if yesterday_metrics.new_cases > 0:
                new_cases_change = ((new_cases - yesterday_metrics.new_cases) / 
                                    yesterday_metrics.new_cases) * 100
            
            if yesterday_metrics.processed_cases > 0:
                processed_cases_change = ((processed_cases - yesterday_metrics.processed_cases) / 
                                         yesterday_metrics.processed_cases) * 100
        
        return {
            "new_cases": new_cases,
            "processed_cases": processed_cases,
            "new_cases_change": new_cases_change,
            "processed_cases_change": processed_cases_change,
            "date": today.isoformat()
        }