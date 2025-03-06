import os
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings."""
    
    # App configuration
    APP_NAME: str = "DOL Analytics API"
    API_PREFIX: str = "/api"
    DEBUG: bool = os.getenv("DEBUG", "False").lower() == "true"
    
    # DOL API configuration
    DOL_API_KEY: str = os.getenv("DOL_API_KEY", "")
    DOL_API_BASE_URL: str = "https://apiprod.dol.gov/v4"
    DOL_AGENCY: str = os.getenv("DOL_AGENCY", "")  # Agency abbreviation
    DOL_ENDPOINT: str = os.getenv("DOL_ENDPOINT", "")  # API endpoint
    
    # Database configuration
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///./dol_analytics.db")
    
    # Schedule configuration (in minutes)
    DATA_FETCH_INTERVAL: int = int(os.getenv("DATA_FETCH_INTERVAL", "60"))  # Default: every hour
    
    class Config:
        env_file = ".env"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    """Return cached settings."""
    return Settings()