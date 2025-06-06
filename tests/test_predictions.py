import pytest
from datetime import date
from unittest.mock import Mock, patch, MagicMock
from fastapi.testclient import TestClient
from fastapi import Depends
from src.dol_analytics.main import app
from src.dol_analytics.api.routes.predictions import router

# Create a mock database connection for testing
def get_mock_postgres_connection():
    """Mock database connection for testing."""
    mock_connection = Mock()
    mock_cursor = Mock()
    mock_cursor.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.__exit__ = Mock(return_value=None)
    mock_connection.cursor.return_value = mock_cursor
    
    # Set up default mock responses
    mock_cursor.fetchone.return_value = None
    mock_cursor.fetchall.return_value = []
    
    yield mock_connection


class TestPredictionEndpoints:
    """Test prediction endpoints with case number storage."""
    
    def setup_method(self):
        """Set up test client with mocked database."""
        # Override the database dependency
        app.dependency_overrides = {}
        from src.dol_analytics.models.database import get_postgres_connection
        app.dependency_overrides[get_postgres_connection] = get_mock_postgres_connection
        self.client = TestClient(app)
    
    def teardown_method(self):
        """Clean up after tests."""
        app.dependency_overrides = {}
    
    @patch('src.dol_analytics.api.routes.predictions.verify_recaptcha')
    def test_predict_from_date_with_case_number(self, mock_recaptcha):
        """Test the /api/predictions/from-date endpoint with case number."""
        # Mock reCAPTCHA verification
        mock_recaptcha.return_value = True
        
        # Override the database dependency with specific mock responses
        def get_test_postgres_connection():
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_cursor.__enter__ = Mock(return_value=mock_cursor)
            mock_cursor.__exit__ = Mock(return_value=None)
            
            # Mock database responses in order
            mock_cursor.fetchone.side_effect = [
                {'id': 1},  # INSERT RETURNING id
                {'median_days': 150, 'upper_estimate_days': 300},  # processing_times
                {'pending_applications': 50000},  # summary_stats
                {'avg_weekly_apps': 2900},  # weekly_summary
                {'cases_ahead': 10000},  # monthly_status before month
                {'count': 5000},  # monthly_status same month
            ]
            
            mock_connection.cursor.return_value = mock_cursor
            yield mock_connection
        
        from src.dol_analytics.models.database import get_postgres_connection
        app.dependency_overrides[get_postgres_connection] = get_test_postgres_connection
        
        # Test data
        test_data = {
            "submit_date": "2024-01-15",
            "employer_first_letter": "A",
            "case_number": "CASE123456",
            "recaptcha_token": "test_token"
        }
        
        # Make request
        response = self.client.post("/api/predictions/from-date", json=test_data)
        
        # Assertions
        assert response.status_code == 200
        data = response.json()
        
        # Check that response includes case number and request ID
        assert "case_number" in data
        assert "request_id" in data
        assert data["case_number"] == "CASE123456"
        assert data["employer_first_letter"] == "A"
        assert "estimated_completion_date" in data
        assert "estimated_days" in data
    
    def test_get_prediction_requests(self):
        """Test the GET /api/predictions/requests endpoint."""
        # Override the database dependency with specific mock responses
        def get_test_postgres_connection():
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_cursor.__enter__ = Mock(return_value=mock_cursor)
            mock_cursor.__exit__ = Mock(return_value=None)
            
            # Mock database responses
            mock_cursor.fetchone.return_value = {'total': 2}
            mock_cursor.fetchall.return_value = [
                {
                    'id': 1,
                    'submit_date': date(2024, 1, 15),
                    'employer_first_letter': 'A',
                    'case_number': 'CASE123456',
                    'request_timestamp': '2024-01-15T10:00:00',
                    'estimated_completion_date': date(2024, 6, 15),
                    'estimated_days': 150,
                    'confidence_level': 0.8,
                    'created_at': '2024-01-15T10:00:00'
                },
                {
                    'id': 2,
                    'submit_date': date(2024, 1, 16),
                    'employer_first_letter': 'B',
                    'case_number': 'CASE789012',
                    'request_timestamp': '2024-01-16T11:00:00',
                    'estimated_completion_date': date(2024, 6, 16),
                    'estimated_days': 152,
                    'confidence_level': 0.8,
                    'created_at': '2024-01-16T11:00:00'
                }
            ]
            
            mock_connection.cursor.return_value = mock_cursor
            yield mock_connection
        
        from src.dol_analytics.models.database import get_postgres_connection
        app.dependency_overrides[get_postgres_connection] = get_test_postgres_connection
        
        # Make request
        response = self.client.get("/api/predictions/requests")
        
        # Assertions
        assert response.status_code == 200
        data = response.json()
        
        assert "total" in data
        assert "requests" in data
        assert data["total"] == 2
        assert len(data["requests"]) == 2
        assert data["requests"][0]["case_number"] == "CASE123456"
        assert data["requests"][1]["case_number"] == "CASE789012"
    
    def test_get_prediction_request_by_id(self):
        """Test the GET /api/predictions/requests/{id} endpoint."""
        # Override the database dependency with specific mock responses
        def get_test_postgres_connection():
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_cursor.__enter__ = Mock(return_value=mock_cursor)
            mock_cursor.__exit__ = Mock(return_value=None)
            
            # Mock database response
            mock_cursor.fetchone.return_value = {
                'id': 1,
                'submit_date': date(2024, 1, 15),
                'employer_first_letter': 'A',
                'case_number': 'CASE123456',
                'request_timestamp': '2024-01-15T10:00:00',
                'estimated_completion_date': date(2024, 6, 15),
                'estimated_days': 150,
                'confidence_level': 0.8,
                'created_at': '2024-01-15T10:00:00'
            }
            
            mock_connection.cursor.return_value = mock_cursor
            yield mock_connection
        
        from src.dol_analytics.models.database import get_postgres_connection
        app.dependency_overrides[get_postgres_connection] = get_test_postgres_connection
        
        # Make request
        response = self.client.get("/api/predictions/requests/1")
        
        # Assertions
        assert response.status_code == 200
        data = response.json()
        
        assert data["id"] == 1
        assert data["case_number"] == "CASE123456"
        assert data["employer_first_letter"] == "A"
        assert data["estimated_days"] == 150
    
    def test_get_prediction_request_not_found(self):
        """Test the GET /api/predictions/requests/{id} endpoint with non-existent ID."""
        # Override the database dependency with specific mock responses
        def get_test_postgres_connection():
            mock_connection = Mock()
            mock_cursor = Mock()
            mock_cursor.__enter__ = Mock(return_value=mock_cursor)
            mock_cursor.__exit__ = Mock(return_value=None)
            
            # Mock database response - no result found
            mock_cursor.fetchone.return_value = None
            
            mock_connection.cursor.return_value = mock_cursor
            yield mock_connection
        
        from src.dol_analytics.models.database import get_postgres_connection
        app.dependency_overrides[get_postgres_connection] = get_test_postgres_connection
        
        # Make request
        response = self.client.get("/api/predictions/requests/999")
        
        # Assertions
        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower() 