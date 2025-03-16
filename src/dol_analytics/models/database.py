"""
Database connection management for DOL Analytics.

This module provides connection handling for the PostgreSQL database
that stores DOL application processing data and statistics.
"""
import os
from typing import Dict, Any, Optional
import psycopg2
import psycopg2.extras

from src.dol_analytics.config import get_settings

settings = get_settings()

# PostgreSQL connection for the database
try:
    from src.dol_analytics.secrets import POSTGRES_URL
    # Use this URL when available
    POSTGRES_CONNECTION_STRING = POSTGRES_URL
except ImportError:
    # Fall back to environment variable
    POSTGRES_CONNECTION_STRING = settings.POSTGRES_DATABASE_URL


def get_postgres_connection():
    """Dependency for PostgreSQL connection to the database."""
    # Log the connection string (sanitized for passwords)
    conn_string = POSTGRES_CONNECTION_STRING
    if ":" in conn_string and "@" in conn_string:
        # Sanitize password for logging
        parts = conn_string.split(":")
        userpass = parts[1].split("@")[0]
        conn_string = conn_string.replace(userpass, "******")
    
    print(f"Connecting to PostgreSQL with: {conn_string}")
    
    # Check if connection string is SQLite or empty
    if not POSTGRES_CONNECTION_STRING or POSTGRES_CONNECTION_STRING.startswith("sqlite:"):
        if settings.DEBUG:
            print("WARNING: Using mock data instead of PostgreSQL connection.")
            yield MockPostgresConnection()
            return
        else:
            raise ValueError("Invalid PostgreSQL connection string. Please set POSTGRES_DATABASE_URL in .env file.")
    
    # Use PostgreSQL connection
    try:
        conn = psycopg2.connect(POSTGRES_CONNECTION_STRING)
        print("Successfully connected to PostgreSQL database!")
        
        # Set autocommit to True to avoid transaction issues
        conn.autocommit = True
        
        try:
            yield conn
        finally:
            conn.close()
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {str(e)}")
        if settings.DEBUG:
            print("Falling back to mock data in debug mode")
            yield MockPostgresConnection()
        else:
            raise


class MockPostgresConnection:
    """Mock PostgreSQL connection for development and testing."""
    
    def cursor(self, **kwargs):
        return MockCursor()
    
    def close(self):
        pass


class MockCursor:
    """Mock cursor that returns sample data."""
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
    
    def execute(self, query, params=None):
        # We could parse the query to return different mock data based on what was requested
        pass
    
    def fetchall(self):
        # Return mock data based on the last query
        return []
    
    def fetchone(self):
        return None


# Import database documentation
try:
    from .database_docs import get_table_docs, get_schema_overview
except ImportError:
    def get_table_docs(table_name):
        return f"Documentation for {table_name} not available."
    
    def get_schema_overview():
        return "Database schema documentation not available."


def init_db():
    """
    Stub function for backward compatibility.
    
    Note: This application now uses PostgreSQL directly and no longer 
    requires SQLAlchemy model initialization.
    """
    print("NOTE: init_db() is deprecated as we're using PostgreSQL directly.")
    pass


def get_db():
    """
    Stub function for backward compatibility.
    
    This used to provide a SQLAlchemy session, but the application 
    now uses PostgreSQL connections directly.
    
    Note: Any code using this function should be updated to use 
    get_postgres_connection() instead.
    """
    print("WARNING: get_db() is deprecated. Use get_postgres_connection() instead.")
    # Return a generator that yields a mock object
    class MockSession:
        def close(self):
            pass
    
    mock_session = MockSession()
    try:
        yield mock_session
    finally:
        mock_session.close()