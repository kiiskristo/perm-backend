"""Tests for database module."""
import pytest
from src.dol_analytics.models.database import Base, get_db


def test_database_models_defined():
    """Test that database models are defined."""
    # Just checking that some models exist in Base.metadata.tables
    assert len(Base.metadata.tables) > 0
    assert "daily_metrics" in Base.metadata.tables
    assert "case_data" in Base.metadata.tables
    assert "prediction_models" in Base.metadata.tables


def test_get_db_is_generator():
    """Test that get_db is a generator function."""
    # This test just verifies that get_db returns a generator
    # We don't actually connect to the database
    db_gen = get_db()
    assert hasattr(db_gen, "__iter__")
    assert hasattr(db_gen, "__next__") 