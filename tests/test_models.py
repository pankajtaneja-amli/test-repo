"""
Unit tests for data models.
"""
import pytest
from datetime import datetime
from src.qlik_monitor.core.models import (
    ReplicationTask,
    TaskStatus,
    TaskMetrics,
    TaskAnomaly,
    RestartProposal,
    MonitoringState
)


class TestModels:
    """Test suite for data models."""

    def test_task_metrics_creation(self):
        """Test TaskMetrics model creation."""
        metrics = TaskMetrics(
            lag_seconds=300,
            records_processed=1000000,
            records_per_second=100.5,
            error_count=5
        )

        assert metrics.lag_seconds == 300
        assert metrics.records_processed == 1000000
        assert metrics.records_per_second == 100.5
        assert metrics.error_count == 5

    def test_replication_task_creation(self):
        """Test ReplicationTask model creation."""
        task = ReplicationTask(
            task_id="test-task-1",
            task_name="Test Replication Task",
            status=TaskStatus.RUNNING,
            source_endpoint="mysql://source:3306",
            target_endpoint="postgres://target:5432"
        )

        assert task.task_id == "test-task-1"
        assert task.task_name == "Test Replication Task"
        assert task.status == TaskStatus.RUNNING
        assert task.restart_attempts == 0

    def test_task_anomaly_creation(self):
        """Test TaskAnomaly model creation."""
        anomaly = TaskAnomaly(
            task_id="task-1",
            task_name="Test Task",
            anomaly_type="high_lag",
            severity="critical",
            description="Task has high replication lag",
            metrics_snapshot={"lag_seconds": 1200}
        )

        assert anomaly.anomaly_type == "high_lag"
        assert anomaly.severity == "critical"
        assert "high replication lag" in anomaly.description
        assert anomaly.detected_at is not None

    def test_restart_proposal_creation(self):
        """Test RestartProposal model creation."""
        proposal = RestartProposal(
            task_id="task-1",
            task_name="Test Task",
            reason="Task is stuck",
            confidence=0.85,
            suggested_action="Restart the task immediately",
            analysis="AI determined the task is stuck and should be restarted"
        )

        assert proposal.confidence == 0.85
        assert proposal.requires_approval is True
        assert proposal.created_at is not None

    def test_restart_proposal_confidence_validation(self):
        """Test that confidence is validated to be between 0 and 1."""
        with pytest.raises(ValueError):
            RestartProposal(
                task_id="task-1",
                task_name="Test Task",
                reason="Test",
                confidence=1.5,  # Invalid
                suggested_action="Test",
                analysis="Test"
            )

    def test_monitoring_state_creation(self):
        """Test MonitoringState model creation."""
        state = MonitoringState(
            total_tasks=100,
            healthy_tasks=80,
            lagging_tasks=10,
            stuck_tasks=5,
            aborted_tasks=3,
            error_tasks=2,
            last_check_time=datetime.utcnow()
        )

        assert state.total_tasks == 100
        assert state.healthy_tasks == 80
        assert state.last_check_time is not None

    def test_task_status_enum(self):
        """Test TaskStatus enum values."""
        assert TaskStatus.RUNNING.value == "RUNNING"
        assert TaskStatus.STOPPED.value == "STOPPED"
        assert TaskStatus.ERROR.value == "ERROR"
        assert TaskStatus.STUCK.value == "STUCK"
        assert TaskStatus.ABORTED.value == "ABORTED"
