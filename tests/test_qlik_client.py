"""
Unit tests for Qlik API client.
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from src.qlik_monitor.integrations.qlik_client import QlikAPIClient
from src.qlik_monitor.core.models import TaskStatus


class TestQlikAPIClient:
    """Test suite for QlikAPIClient."""

    def setup_method(self):
        """Setup test fixtures."""
        self.client = QlikAPIClient(
            api_endpoint="https://test-qlik.com/api",
            api_token="test-token"
        )

    @patch('src.qlik_monitor.integrations.qlik_client.requests.Session')
    def test_get_all_tasks_success(self, mock_session):
        """Test successful retrieval of all tasks."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "tasks": [
                {
                    "task_id": "task-1",
                    "task_name": "Test Task",
                    "status": "RUNNING",
                    "source_endpoint": "source",
                    "target_endpoint": "target",
                    "metrics": {"lag_seconds": 100}
                }
            ]
        }
        mock_session.return_value.get.return_value = mock_response

        self.client.session = mock_session()
        tasks = self.client.get_all_tasks()

        assert len(tasks) == 1
        assert tasks[0].task_id == "task-1"
        assert tasks[0].status == TaskStatus.RUNNING

    @patch('src.qlik_monitor.integrations.qlik_client.requests.Session')
    def test_get_all_tasks_empty(self, mock_session):
        """Test retrieval when no tasks exist."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"tasks": []}
        mock_session.return_value.get.return_value = mock_response

        self.client.session = mock_session()
        tasks = self.client.get_all_tasks()

        assert len(tasks) == 0

    @patch('src.qlik_monitor.integrations.qlik_client.requests.Session')
    def test_restart_task_success(self, mock_session):
        """Test successful task restart."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_session.return_value.post.return_value = mock_response

        self.client.session = mock_session()
        result = self.client.restart_task("task-1")

        assert result is True

    @patch('requests.exceptions.RequestException', Exception)
    def test_restart_task_failure(self):
        """Test failed task restart."""
        with patch.object(self.client.session, 'post', side_effect=Exception("Connection error")):
            result = self.client.restart_task("task-1")
            assert result is False

    def test_parse_datetime_valid(self):
        """Test datetime parsing with valid ISO format."""
        dt_str = "2024-01-15T10:30:00Z"
        result = QlikAPIClient._parse_datetime(dt_str)

        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_parse_datetime_invalid(self):
        """Test datetime parsing with invalid format."""
        result = QlikAPIClient._parse_datetime("invalid-date")

        assert result is None

    def test_parse_datetime_none(self):
        """Test datetime parsing with None input."""
        result = QlikAPIClient._parse_datetime(None)

        assert result is None
