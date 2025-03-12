"""Tests for configuration module."""
import pytest
from src.dol_analytics.config import get_settings


def test_settings_has_required_fields():
    """Test that settings has the required fields."""
    settings = get_settings()
    assert hasattr(settings, "APP_NAME")
    assert hasattr(settings, "API_PREFIX")
    assert hasattr(settings, "DEBUG")
    assert hasattr(settings, "DATABASE_URL")
    assert hasattr(settings, "POSTGRES_DATABASE_URL") 