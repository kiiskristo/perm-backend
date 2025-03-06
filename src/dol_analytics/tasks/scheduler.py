import asyncio
import logging
from datetime import datetime
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.orm import Session

# Use relative imports if running as a module
try:
    from ..models.database import get_db, init_db
    from ..services.data_processor import DataProcessor
    from ..config import get_settings
except ImportError:
    # Use absolute imports if running as a script
    from src.dol_analytics.models.database import get_db, init_db
    from src.dol_analytics.services.data_processor import DataProcessor
    from src.dol_analytics.config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)


class Scheduler:
    """Scheduler for running periodic tasks."""
    
    def __init__(self):
        self.scheduler = AsyncIOScheduler()
        self.db_generator = get_db()
        self.db = next(self.db_generator)
        
        # Ensure database tables exist
        init_db()
        
        # Set up logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        )
    
    def start(self):
        """Start the scheduler."""
        # Configure jobs
        self._configure_jobs()
        
        # Start the scheduler
        self.scheduler.start()
        logger.info("Scheduler started")
    
    def shutdown(self):
        """Shutdown the scheduler."""
        if self.scheduler.running:
            self.scheduler.shutdown()
            logger.info("Scheduler shutdown")
    
    def _configure_jobs(self):
        """Configure scheduled jobs."""
        # Schedule data fetch job daily at 1:00 AM
        self.scheduler.add_job(
            self._fetch_data_job,
            trigger=CronTrigger(hour=1, minute=0),
            id="fetch_daily_data",
            name="Fetch DOL API Data Daily",
            replace_existing=True,
        )
        
        logger.info("Scheduled daily data fetch job for 1:00 AM")
    
    async def _fetch_data_job(self):
        """Job to fetch data from DOL API."""
        logger.info("Starting daily data fetch job")
        
        try:
            processor = DataProcessor(self.db)
            await processor.fetch_and_process_daily_data()
            logger.info("Daily data fetch completed successfully")
        except Exception as e:
            logger.error("Error in daily data fetch job: %s", str(e), exc_info=True)
    
    def run_manual_fetch(self):
        """Manually run the data fetch job."""
        asyncio.create_task(self._fetch_data_job())
        logger.info("Manual data fetch job initiated")


# Singleton instance
scheduler_instance = Scheduler()


def get_scheduler() -> Scheduler:
    """Get the scheduler instance."""
    return scheduler_instance