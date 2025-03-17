"""Test fixtures for pytest."""
import os
import pytest
import psycopg2
from psycopg2.extras import DictCursor

from src.dol_analytics.models.database import get_postgres_connection, MockPostgresConnection
from src.dol_analytics.config import get_settings


@pytest.fixture
def mock_postgres_connection():
    """Get a mock PostgreSQL connection for tests."""
    return MockPostgresConnection()


@pytest.fixture
def mock_postgres_cursor(mock_postgres_connection):
    """Get a mock PostgreSQL cursor for tests."""
    return mock_postgres_connection.cursor()


@pytest.fixture
def override_get_postgres_connection():
    """Override get_postgres_connection to return a mock connection."""
    def _get_mock_connection():
        yield MockPostgresConnection()
    
    return _get_mock_connection


@pytest.fixture
def client(override_get_postgres_connection):
    """Get a test client for the FastAPI app."""
    from fastapi.testclient import TestClient
    from fastapi import Depends
    from src.dol_analytics.main import app
    from src.dol_analytics.models.database import get_postgres_connection
    
    # Override the dependency
    app.dependency_overrides[get_postgres_connection] = override_get_postgres_connection
    
    client = TestClient(app)
    yield client
    
    # Clean up
    app.dependency_overrides = {} 