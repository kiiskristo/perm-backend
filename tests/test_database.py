"""Tests for database module."""
import pytest
from src.dol_analytics.models.database import get_postgres_connection, MockPostgresConnection, MockCursor


def test_mock_postgres_connection():
    """Test that MockPostgresConnection works as expected."""
    conn = MockPostgresConnection()
    assert hasattr(conn, "cursor")
    assert hasattr(conn, "close")
    
    cursor = conn.cursor()
    assert isinstance(cursor, MockCursor)


def test_get_postgres_connection_is_generator():
    """Test that get_postgres_connection is a generator function."""
    # This test just verifies that get_postgres_connection returns a generator
    # We don't actually connect to the database
    conn_gen = get_postgres_connection()
    assert hasattr(conn_gen, "__iter__")
    assert hasattr(conn_gen, "__next__")


def test_mock_cursor_methods():
    """Test that MockCursor provides required methods."""
    cursor = MockCursor()
    
    # Test context manager
    with cursor as c:
        assert c is cursor
    
    # Test methods
    cursor.execute("SELECT 1")
    assert cursor.fetchall() == []
    assert cursor.fetchone() is None 