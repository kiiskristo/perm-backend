import logging
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from src.dol_analytics.config import get_settings
from src.dol_analytics.api.routes import data, predictions, chatbot

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
    """
    # Initialize database tables for local SQLAlchemy models
    # (though we'll be primarily using the external PostgreSQL database)
    logger.info("Initializing database")
    
    # Yield control to the application
    yield
    
    # Cleanup (if needed)
    logger.info("Shutting down application")


# Create FastAPI app
app = FastAPI(
    title=settings.APP_NAME,
    description="DOL Analytics API for PERM data visualization and predictions",
    version="0.1.0",
    lifespan=lifespan,
)

# Configure CORS middleware
allowed_origins = [
    "https://permupdate.com",
    "https://www.permupdate.com",
]

# Only include localhost in development mode
if settings.DEBUG:
    allowed_origins.extend([
        "http://localhost:3000", 
        "http://localhost:8000",
    ])
    logger.info(f"Running in DEBUG mode with CORS origins: {allowed_origins}")
else:
    logger.info(f"Running in PRODUCTION mode with CORS origins: {allowed_origins}")

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API routes
app.include_router(data.router, prefix=settings.API_PREFIX)
app.include_router(predictions.router, prefix=settings.API_PREFIX)
app.include_router(chatbot.router, prefix=settings.API_PREFIX)


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "app_name": settings.APP_NAME,
        "version": "1.0.0",
        "status": "running",
        "data_source": "PostgreSQL"
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