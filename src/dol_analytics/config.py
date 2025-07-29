import os
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import ConfigDict
from functools import lru_cache
from dotenv import load_dotenv

# Explicitly load .env file from project root
load_dotenv()


class Settings(BaseSettings):
    """Application settings."""
    
    # App configuration
    APP_NAME: str = "DOL Analytics API"
    API_PREFIX: str = "/api"
    DEBUG: bool = False
    
    # PostgreSQL configuration for the external data service
    POSTGRES_DATABASE_URL: str
    
    # reCAPTCHA configuration
    RECAPTCHA_SECRET_KEY: str = ""
    SKIP_RECAPTCHA_IN_DEBUG: bool = True
    
    # OpenAI configuration
    OPENAI_API_KEY: str = ""
    
    # Specify exactly where to look for the .env file
    model_config = ConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore"
    )


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings."""
    # Clear cache if settings change
    return Settings()