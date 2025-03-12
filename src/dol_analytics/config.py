import os
from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings."""
    
    # App configuration
    APP_NAME: str = "DOL Analytics API"
    API_PREFIX: str = "/api"
    DEBUG: bool = os.getenv("DEBUG", "False").lower() == "true"
    
    # Database configuration
    DATABASE_URL: str = os.getenv("DATABASE_URL", "sqlite:///./dol_analytics.db")
    
    # PostgreSQL configuration for the external data service
    POSTGRES_DATABASE_URL: str = os.getenv("POSTGRES_DATABASE_URL", os.getenv("DATABASE_URL"))
    
    class Config:
        env_file = ".env"
        case_sensitive = True
        extra = "ignore"


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings."""
    return Settings()