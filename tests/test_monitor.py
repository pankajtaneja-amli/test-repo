"""
Unit tests for task monitor anomaly detection.
"""
import pytest
from datetime import datetime, timedelta
from src.qlik_monitor.core.monitor import TaskMonitor
from src.qlik_monitor.core.models import (
    ReplicationTask,
    TaskStatus,
    TaskMetrics
)


class TestTaskMonitor:
    """Test suite for TaskMonitor."""

    def setup_method(self):
        """Setup test fixtures."""
        self.monitor = TaskMonitor()

    def test_detect_aborted_task(self):
        """Test detection of aborted tasks."""
        task = ReplicationTask(
            task_id="task-1",
            task_name="Test Task",
            status=TaskStatus.ABORTED,
            source_endpoint="source-db",
            target_endpoint="target-db",
            metrics=TaskMetrics()
        )

        anomalies = self.monitor.detect_anomalies(task)

        assert len(anomalies) == 1
        assert anomalies[0].anomaly_type == "aborted"
        assert anomalies[0].severity == "critical"

    def test_detect_high_lag(self):
        """Test detection of high replication lag."""
        task = ReplicationTask(
            task_id="task-2",
            task_name="Lagging Task",
            status=TaskStatus.RUNNING,
            source_endpoint="source-db",
            target_endpoint="target-db",
            metrics=TaskMetrics(lag_seconds=1200)  # Above threshold
        )

        anomalies = self.monitor.detect_anomalies(task)

        high_lag_anomalies = [a for a in anomalies if a.anomaly_type == "high_lag"]
        assert len(high_lag_anomalies) > 0
        assert high_lag_anomalies[0].severity in ["high", "critical"]

    def test_detect_stuck_task(self):
        """Test detection of stuck tasks."""
        old_time = datetime.utcnow() - timedelta(seconds=2400)
        task = ReplicationTask(
            task_id="task-3",
            task_name="Stuck Task",
            status=TaskStatus.RUNNING,
            source_endpoint="source-db",
            target_endpoint="target-db",
            metrics=TaskMetrics(last_update_time=old_time)
        )

        anomalies = self.monitor.detect_anomalies(task)

        stuck_anomalies = [a for a in anomalies if a.anomaly_type == "stuck"]
        assert len(stuck_anomalies) > 0
        assert stuck_anomalies[0].severity == "high"

    def test_no_anomalies_healthy_task(self):
        """Test that healthy tasks have no anomalies."""
        task = ReplicationTask(
            task_id="task-4",
            task_name="Healthy Task",
            status=TaskStatus.RUNNING,
            source_endpoint="source-db",
            target_endpoint="target-db",
            metrics=TaskMetrics(
                lag_seconds=100,  # Below threshold
                error_count=0,
                records_per_second=100.0,
                last_update_time=datetime.utcnow()
            )
        )

        anomalies = self.monitor.detect_anomalies(task)

        assert len(anomalies) == 0

    def test_should_restart_aborted_task(self):
        """Test restart decision for aborted task."""
        task = ReplicationTask(
            task_id="task-5",
            task_name="Aborted Task",
            status=TaskStatus.ABORTED,
            source_endpoint="source-db",
            target_endpoint="target-db",
            metrics=TaskMetrics(),
            restart_attempts=0
        )

        anomalies = self.monitor.detect_anomalies(task)
        should_restart = self.monitor.should_restart_task(task, anomalies)

        assert should_restart is True

    def test_should_not_restart_max_attempts(self):
        """Test that tasks with max restart attempts are not restarted."""
        task = ReplicationTask(
            task_id="task-6",
            task_name="Max Restart Task",
            status=TaskStatus.ERROR,
            source_endpoint="source-db",
            target_endpoint="target-db",
            metrics=TaskMetrics(),
            restart_attempts=5  # Above max
        )

        anomalies = self.monitor.detect_anomalies(task)
        should_restart = self.monitor.should_restart_task(task, anomalies)

        assert should_restart is False

    def test_assess_overall_health(self):
        """Test overall health assessment of multiple tasks."""
        tasks = [
            ReplicationTask(
                task_id=f"task-{i}",
                task_name=f"Task {i}",
                status=TaskStatus.RUNNING if i % 2 == 0 else TaskStatus.ERROR,
                source_endpoint="source-db",
                target_endpoint="target-db",
                metrics=TaskMetrics(lag_seconds=100 if i % 2 == 0 else 1200)
            )
            for i in range(10)
        ]

        health_state = self.monitor.assess_overall_health(tasks)

        assert health_state.total_tasks == 10
        assert health_state.healthy_tasks >= 0
        assert health_state.error_tasks >= 0
        assert health_state.last_check_time is not None
