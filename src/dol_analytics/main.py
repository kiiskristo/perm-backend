import logging
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from src.dol_analytics.config import get_settings
from src.dol_analytics.models.database import init_db
from src.dol_analytics.api.routes import data, predictions
from src.dol_analytics.tasks.scheduler import get_scheduler, Scheduler

settings = get_settings()

# Configure logging
logging.basicConfig(
    level=logging.INFO if settings.DEBUG else logging.WARNING,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("dol_analytics")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifecycle events for the FastAPI application.
    - Initialize database
    - Start scheduler for cron jobs
    """
    # Initialize database tables
    logger.info("Initializing database")
    init_db()
    
    # Start scheduler
    logger.info("Starting scheduler")
    scheduler = get_scheduler()
    scheduler.start()
    
    # Yield control to the application
    yield
    
    # Shutdown scheduler
    logger.info("Shutting down scheduler")
    scheduler.shutdown()


# Create FastAPI application
app = FastAPI(
    title=settings.APP_NAME,
    description="API for DOL data analytics and predictions",
    version="1.0.0",
    lifespan=lifespan,
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, replace with specific origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(data.router, prefix=settings.API_PREFIX)
app.include_router(predictions.router, prefix=settings.API_PREFIX)


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "app_name": settings.APP_NAME,
        "version": "1.0.0",
        "status": "running",
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    
    # Run the application using Uvicorn
    uvicorn.run(
        "src.dol_analytics.main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.DEBUG,
    )