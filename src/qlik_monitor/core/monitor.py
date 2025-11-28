"""
Core monitoring agent for detecting task anomalies.
"""
import logging
from typing import List, Optional
from datetime import datetime, timedelta

from ..config import config
from ..core.models import (
    ReplicationTask,
    TaskStatus,
    TaskAnomaly,
    MonitoringState
)

logger = logging.getLogger(__name__)


class TaskMonitor:
    """Monitor for detecting anomalies in replication tasks."""

    def __init__(self):
        """Initialize task monitor with configuration."""
        self.lag_threshold = config.monitoring.lag_threshold_seconds
        self.stuck_threshold = config.monitoring.stuck_task_threshold_seconds

    def detect_anomalies(self, task: ReplicationTask) -> List[TaskAnomaly]:
        """
        Detect anomalies in a replication task.

        Args:
            task: The replication task to analyze

        Returns:
            List of detected anomalies
        """
        anomalies = []

        # Check for aborted tasks
        if task.status == TaskStatus.ABORTED:
            anomalies.append(TaskAnomaly(
                task_id=task.task_id,
                task_name=task.task_name,
                anomaly_type="aborted",
                severity="critical",
                description=f"Task {task.task_name} is in ABORTED state",
                metrics_snapshot={
                    "status": task.status.value,
                    "error_count": task.metrics.error_count
                }
            ))

        # Check for error state
        if task.status == TaskStatus.ERROR:
            anomalies.append(TaskAnomaly(
                task_id=task.task_id,
                task_name=task.task_name,
                anomaly_type="error",
                severity="high",
                description=f"Task {task.task_name} is in ERROR state",
                metrics_snapshot={
                    "status": task.status.value,
                    "error_count": task.metrics.error_count
                }
            ))

        # Check for high lag
        if task.metrics.lag_seconds and task.metrics.lag_seconds > self.lag_threshold:
            severity = "critical" if task.metrics.lag_seconds > self.lag_threshold * 2 else "high"
            anomalies.append(TaskAnomaly(
                task_id=task.task_id,
                task_name=task.task_name,
                anomaly_type="high_lag",
                severity=severity,
                description=(
                    f"Task {task.task_name} has high replication lag: "
                    f"{task.metrics.lag_seconds} seconds (threshold: {self.lag_threshold})"
                ),
                metrics_snapshot={
                    "lag_seconds": task.metrics.lag_seconds,
                    "threshold": self.lag_threshold
                }
            ))

        # Check for stuck task (no updates for extended period)
        if task.metrics.last_update_time:
            time_since_update = (datetime.utcnow() - task.metrics.last_update_time).total_seconds()
            if time_since_update > self.stuck_threshold and task.status == TaskStatus.RUNNING:
                anomalies.append(TaskAnomaly(
                    task_id=task.task_id,
                    task_name=task.task_name,
                    anomaly_type="stuck",
                    severity="high",
                    description=(
                        f"Task {task.task_name} appears stuck: no updates for "
                        f"{int(time_since_update)} seconds"
                    ),
                    metrics_snapshot={
                        "time_since_update": time_since_update,
                        "threshold": self.stuck_threshold,
                        "last_update": task.metrics.last_update_time.isoformat()
                    }
                ))

        # Check for high error rate
        if task.metrics.error_count and task.metrics.error_count > 10:
            severity = "critical" if task.metrics.error_count > 50 else "medium"
            anomalies.append(TaskAnomaly(
                task_id=task.task_id,
                task_name=task.task_name,
                anomaly_type="high_errors",
                severity=severity,
                description=(
                    f"Task {task.task_name} has high error count: "
                    f"{task.metrics.error_count} errors"
                ),
                metrics_snapshot={
                    "error_count": task.metrics.error_count
                }
            ))

        # Check for low processing rate (potential performance issue)
        if (task.status == TaskStatus.RUNNING and
            task.metrics.records_per_second is not None and
            task.metrics.records_per_second < 1.0):
            anomalies.append(TaskAnomaly(
                task_id=task.task_id,
                task_name=task.task_name,
                anomaly_type="low_performance",
                severity="medium",
                description=(
                    f"Task {task.task_name} has low processing rate: "
                    f"{task.metrics.records_per_second:.2f} records/sec"
                ),
                metrics_snapshot={
                    "records_per_second": task.metrics.records_per_second
                }
            ))

        if anomalies:
            logger.info(
                f"Detected {len(anomalies)} anomalies for task {task.task_id}: "
                f"{[a.anomaly_type for a in anomalies]}"
            )

        return anomalies

    def assess_overall_health(self, tasks: List[ReplicationTask]) -> MonitoringState:
        """
        Assess overall health of all replication tasks.

        Args:
            tasks: List of all replication tasks

        Returns:
            MonitoringState with aggregated statistics
        """
        state = MonitoringState(
            total_tasks=len(tasks),
            last_check_time=datetime.utcnow()
        )

        for task in tasks:
            anomalies = self.detect_anomalies(task)

            if not anomalies:
                state.healthy_tasks += 1
            else:
                # Categorize by most severe anomaly
                has_critical = any(a.severity == "critical" for a in anomalies)
                anomaly_types = {a.anomaly_type for a in anomalies}

                if task.status == TaskStatus.ABORTED or "aborted" in anomaly_types:
                    state.aborted_tasks += 1
                elif task.status == TaskStatus.ERROR or "error" in anomaly_types:
                    state.error_tasks += 1
                elif "stuck" in anomaly_types:
                    state.stuck_tasks += 1
                elif "high_lag" in anomaly_types:
                    state.lagging_tasks += 1
                else:
                    # Other issues but not critical
                    if has_critical:
                        state.error_tasks += 1
                    else:
                        state.healthy_tasks += 1

        logger.info(
            f"Health assessment: {state.healthy_tasks}/{state.total_tasks} healthy, "
            f"{state.lagging_tasks} lagging, {state.stuck_tasks} stuck, "
            f"{state.aborted_tasks} aborted, {state.error_tasks} errors"
        )

        return state

    def should_restart_task(
        self,
        task: ReplicationTask,
        anomalies: List[TaskAnomaly]
    ) -> bool:
        """
        Determine if a task should be restarted based on heuristics.

        Args:
            task: The replication task
            anomalies: List of detected anomalies

        Returns:
            True if task should be restarted, False otherwise
        """
        # Don't restart if already attempted too many times
        if task.restart_attempts >= config.monitoring.max_restart_attempts:
            logger.info(
                f"Task {task.task_id} has reached max restart attempts "
                f"({task.restart_attempts})"
            )
            return False

        # Restart if aborted or in error state
        if task.status in [TaskStatus.ABORTED, TaskStatus.ERROR]:
            return True

        # Restart if stuck for too long
        stuck_anomalies = [a for a in anomalies if a.anomaly_type == "stuck"]
        if stuck_anomalies and any(a.severity in ["high", "critical"] for a in stuck_anomalies):
            return True

        # Restart if lag is critical and growing
        critical_lag = [
            a for a in anomalies
            if a.anomaly_type == "high_lag" and a.severity == "critical"
        ]
        if critical_lag:
            return True

        return False
